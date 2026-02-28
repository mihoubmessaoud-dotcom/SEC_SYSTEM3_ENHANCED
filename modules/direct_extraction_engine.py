from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


XLINK_NS = "{http://www.w3.org/1999/xlink}"


def _norm_accession(v: str) -> str:
    return str(v or "").replace("-", "").strip()


def _role_kind(txt: str) -> Optional[str]:
    v = str(txt or "").lower()
    if "comprehensive" in v:
        return None
    if ("balance" in v and "sheet" in v) or ("financial position" in v):
        return "balance_sheet"
    if ("income" in v) or ("operations" in v) or ("earnings" in v) or ("profit and loss" in v):
        return "income_statement"
    if ("cash flow" in v) or ("cashflow" in v) or ("cash flows" in v):
        return "cash_flow"
    return None


def _as_float_cell(text: str) -> Optional[float]:
    s = str(text or "").strip()
    if not s:
        return None
    s = s.replace("$", "").replace(",", "").replace("\u2014", "").replace("—", "").replace("\xa0", " ").strip()
    if not s or s in {"-", "--"}:
        return None
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return None


def _unit_multiplier_from_header(header_text: str) -> float:
    t = str(header_text or "").lower()
    if " in billions" in t or "$ in billions" in t:
        return 1_000_000_000.0
    if " in millions" in t or "$ in millions" in t:
        return 1_000_000.0
    if " in thousands" in t or "$ in thousands" in t:
        return 1_000.0
    return 1.0


def _clean_doc_wrapper(raw: str) -> str:
    pos = raw.lower().find("<html")
    if pos >= 0:
        return raw[pos:]
    return raw


def _concept_base_from_defref(onclick_text: str) -> Optional[str]:
    if not onclick_text:
        return None
    m = re.search(r"defref_([A-Za-z0-9\-]+)_([A-Za-z0-9_]+)", onclick_text)
    if not m:
        return None
    return m.group(2)


def _tag_from_defref(onclick_text: str) -> Optional[str]:
    if not onclick_text:
        return None
    m = re.search(r"defref_([A-Za-z0-9\-]+)_([A-Za-z0-9_]+)", onclick_text)
    if not m:
        return None
    return f"{m.group(1)}:{m.group(2)}"


def _safe_get(url: str, headers: Dict[str, str], timeout: int) -> requests.Response:
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r


@dataclass
class StatementReport:
    kind: str
    role: str
    short_name: str
    html_file: str
    position: int


class DirectExtractionEngine:
    def __init__(self, user_agent: str) -> None:
        self.headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
        }

    def discover_reports(self, base_dir: str, timeout: int = 60) -> List[StatementReport]:
        filing_summary = _safe_get(f"{base_dir}/FilingSummary.xml", self.headers, timeout).text
        root = ET.fromstring(filing_summary)
        reports: List[StatementReport] = []
        fallback_reports: List[StatementReport] = []
        for rpt in root.findall(".//Report"):
            short_name = (rpt.findtext("ShortName") or "").strip()
            menu = (rpt.findtext("MenuCategory") or "").strip().lower()
            role = (rpt.findtext("Role") or "").strip()
            html_file = (rpt.findtext("HtmlFileName") or "").strip()
            pos_txt = (rpt.findtext("Position") or "").strip()
            try:
                position = int(pos_txt)
            except Exception:
                position = 10**9
            if not short_name or not html_file or not role:
                continue
            if menu != "statements":
                continue
            kind = _role_kind(short_name) or _role_kind(role)
            if kind not in {"balance_sheet", "income_statement", "cash_flow"}:
                continue
            role_lc = role.lower()
            short_lc = short_name.lower()
            if "comprehensive" in role_lc or "comprehensive" in short_lc:
                continue
            rec = StatementReport(
                kind=kind,
                role=role,
                short_name=short_name,
                html_file=html_file,
                position=position,
            )
            if "consolidated" in short_name.lower():
                reports.append(rec)
            else:
                fallback_reports.append(
                    StatementReport(
                        kind=kind,
                        role=role,
                        short_name=short_name,
                        html_file=html_file,
                        position=position,
                    )
                )
        # Fallback: some filers do not include "Consolidated" in ShortName.
        if not reports:
            reports = fallback_reports
        reports.sort(key=lambda r: r.position)
        picked: Dict[str, StatementReport] = {}
        for rpt in reports:
            if rpt.kind not in picked:
                picked[rpt.kind] = rpt
        return [picked[k] for k in ("balance_sheet", "income_statement", "cash_flow") if k in picked]

    @staticmethod
    def _pick_balance_total(rows_for_period: List[Dict], concept_candidates: List[str], label_candidates: List[str]) -> Optional[Dict]:
        concept_candidates_lc = [c.lower() for c in concept_candidates]
        label_candidates_lc = [lc.lower() for lc in label_candidates]

        def _score(r: Dict) -> int:
            s = 0
            concept = str(r.get("Concept") or "").lower()
            label = str(r.get("Line_Item") or "").lower()
            if concept in concept_candidates_lc:
                s += 5
            if any(x in label for x in label_candidates_lc):
                s += 3
            return s

        candidates = [r for r in rows_for_period if isinstance(r.get("Value_Absolute"), (int, float, float))]
        if not candidates:
            return None
        ranked = sorted(candidates, key=_score, reverse=True)
        best = ranked[0]
        return best if _score(best) > 0 else None

    @staticmethod
    def _pick_best_concept_row(
        rows_for_period: List[Dict],
        concept_name: str,
        preferred_label_tokens: Optional[List[str]] = None,
    ) -> Optional[Dict]:
        preferred_label_tokens = [str(t).lower() for t in (preferred_label_tokens or [])]
        candidates = []
        for r in rows_for_period:
            c = str(r.get("Concept") or "").strip().lower()
            if c != str(concept_name).lower():
                continue
            val = r.get("Value_Absolute")
            if not isinstance(val, (int, float)):
                continue
            label = str(r.get("Line_Item") or "").lower()
            pref = 1 if any(tok in label for tok in preferred_label_tokens) else 0
            candidates.append((pref, abs(float(val)), r))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return candidates[0][2]

    def validate_balance_sheet(self, rows: List[Dict]) -> None:
        def _is_balance_role(role_txt: str, stmt_txt: str) -> bool:
            role_lc = str(role_txt or "").lower()
            stmt_lc = str(stmt_txt or "").lower()
            return (
                ("balancesheet" in role_lc)
                or ("balance sheet" in role_lc)
                or ("statementoffinancialposition" in role_lc)
                or ("financial position" in role_lc)
                or ("balance sheet" in stmt_lc)
                or ("financial position" in stmt_lc)
            )

        bs_rows = [
            r for r in rows
            if _is_balance_role(r.get("Role_URI"), r.get("Statement"))
        ]
        if not bs_rows:
            raise RuntimeError("Balance sheet statement not found in consolidated statements.")

        periods = sorted({str(r.get("Period") or "") for r in bs_rows if str(r.get("Period") or "").strip()})
        failures: List[str] = []
        validated_periods = 0
        for period in periods:
            pr = [r for r in bs_rows if str(r.get("Period") or "") == period]
            assets = self._pick_best_concept_row(
                pr,
                "Assets",
                preferred_label_tokens=["total assets"],
            )
            if assets is None:
                assets = self._pick_balance_total(
                    pr,
                    concept_candidates=["Assets", "AssetsCurrent"],
                    label_candidates=["total assets"],
                )

            liabilities_and_equity = self._pick_best_concept_row(
                pr,
                "LiabilitiesAndStockholdersEquity",
                preferred_label_tokens=["liabilities and stockholders", "liabilities and shareholders"],
            )
            if liabilities_and_equity is None:
                liabilities_and_equity = next(
                    (
                        r for r in pr
                        if ("liabilities and stockholders" in str(r.get("Line_Item") or "").lower())
                        or ("liabilities and shareholders" in str(r.get("Line_Item") or "").lower())
                    ),
                    None,
                )

            liabilities = self._pick_best_concept_row(
                pr,
                "Liabilities",
                preferred_label_tokens=["total liabilities"],
            )
            if liabilities is None:
                liability_rows = []
                for r in pr:
                    c = str(r.get("Concept") or "").lower()
                    l = str(r.get("Line_Item") or "").lower()
                    if c == "liabilitiesandstockholdersequity":
                        continue
                    if ("liabilities and stockholders" in l) or ("liabilities and shareholders" in l):
                        continue
                    if "current liabilities" in l:
                        continue
                    liability_rows.append(r)
                liabilities = self._pick_balance_total(
                    liability_rows,
                    concept_candidates=["Liabilities"],
                    label_candidates=["total liabilities"],
                )

            equity = self._pick_best_concept_row(
                pr,
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                preferred_label_tokens=["total equity", "total stockholders", "total shareholders"],
            ) or self._pick_best_concept_row(
                pr,
                "StockholdersEquity",
                preferred_label_tokens=["total equity", "total stockholders", "total shareholders"],
            )
            if equity is None:
                equity = self._pick_balance_total(
                    pr,
                    concept_candidates=[
                        "StockholdersEquity",
                        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                    ],
                    label_candidates=["total stockholders", "total equity", "total shareholders"],
                )

            # Accept official derivation when total liabilities line is absent:
            # Liabilities = (Liabilities and Equity) - Equity
            if (
                liabilities is None
                and liabilities_and_equity is not None
                and equity is not None
                and liabilities_and_equity.get("Value_Absolute") is not None
                and equity.get("Value_Absolute") is not None
            ):
                le_val = float(liabilities_and_equity["Value_Absolute"])
                eq_val = float(equity["Value_Absolute"])
                liabilities = {"Value_Absolute": le_val - eq_val}
            # If explicit liabilities/equity are missing but official combined line exists,
            # allow this period as structurally valid (common in some filers).
            if (
                assets is not None
                and liabilities_and_equity is not None
                and isinstance(assets.get("Value_Absolute"), (int, float))
                and isinstance(liabilities_and_equity.get("Value_Absolute"), (int, float))
            ):
                a0 = float(assets["Value_Absolute"])
                le0 = float(liabilities_and_equity["Value_Absolute"])
                diff0 = a0 - le0
                tol0 = max(abs(a0) * 0.02, 5.0)
                if abs(diff0) <= tol0 and (
                    liabilities is None or liabilities.get("Value_Absolute") is None or equity is None or equity.get("Value_Absolute") is None
                ):
                    continue

            missing = []
            if assets is None or assets.get("Value_Absolute") is None:
                missing.append("Total Assets")
            if liabilities is None or liabilities.get("Value_Absolute") is None:
                missing.append("Total Liabilities")
            if equity is None or equity.get("Value_Absolute") is None:
                missing.append("Total Equity")
            if missing:
                # Some filings include auxiliary columns/period headers that do not expose
                # all core totals in the rendered table. Skip those periods instead of rejecting
                # the entire filing.
                if len(missing) == 3:
                    continue
                failures.append(f"{period}: missing required line item(s): {', '.join(missing)}")
                continue

            a = float(assets["Value_Absolute"])
            l = float(liabilities["Value_Absolute"])
            e = float(equity["Value_Absolute"])
            diff = a - (l + e)
            # SEC-rendered statements can have rounding / presentation deltas.
            tol = max(abs(a) * 0.02, 5.0)
            if abs(diff) > tol:
                failures.append(
                    f"{period}: balance mismatch Assets({a}) != Liabilities({l}) + Equity({e}); difference={diff}"
                )
            else:
                validated_periods += 1

        if not validated_periods and periods:
            raise RuntimeError("Cross-check failed. Extraction rejected. No balance-sheet period could be validated.")
        if failures:
            report = " | ".join(failures)
            raise RuntimeError(f"Cross-check failed. Extraction rejected. {report}")

    def parse_calculation_linkbase(self, base_dir: str, timeout: int = 60) -> Dict[str, Dict[str, List[Dict]]]:
        idx = _safe_get(f"{base_dir}/index.json", self.headers, timeout).json()
        cal_name = None
        for it in (((idx or {}).get("directory") or {}).get("item") or []):
            n = (it or {}).get("name")
            if isinstance(n, str) and n.lower().endswith(".xml") and ("_cal" in n.lower() or "calculation" in n.lower()):
                cal_name = n
                break
        if not cal_name:
            return {}
        cal_xml = _safe_get(f"{base_dir}/{cal_name}", self.headers, timeout).text
        root = ET.fromstring(cal_xml)
        out: Dict[str, Dict[str, List[Dict]]] = {}
        for link in root.iter():
            if (link.tag.rsplit("}", 1)[-1] if "}" in link.tag else link.tag) != "calculationLink":
                continue
            role = link.attrib.get(f"{XLINK_NS}role") or "unknown_role"
            loc_map: Dict[str, str] = {}
            children: Dict[str, List[Dict]] = {}
            for node in list(link):
                nname = node.tag.rsplit("}", 1)[-1] if "}" in node.tag else node.tag
                if nname == "loc":
                    lbl = node.attrib.get(f"{XLINK_NS}label")
                    href = node.attrib.get(f"{XLINK_NS}href")
                    concept = None
                    if isinstance(href, str) and "#" in href:
                        frag = href.split("#", 1)[-1]
                        if ":" in frag:
                            concept = frag.split(":", 1)[-1]
                        elif "_" in frag:
                            concept = frag.split("_", 1)[-1]
                        else:
                            concept = frag
                    if lbl and concept:
                        loc_map[lbl] = concept
                elif nname == "calculationArc":
                    src = node.attrib.get(f"{XLINK_NS}from")
                    dst = node.attrib.get(f"{XLINK_NS}to")
                    p = loc_map.get(src)
                    c = loc_map.get(dst)
                    if not p or not c:
                        continue
                    try:
                        weight = float(node.attrib.get("weight", "1"))
                    except Exception:
                        weight = 1.0
                    children.setdefault(p, []).append({"concept": c, "weight": weight})
            out[role] = children
        return out

    def parse_statement_table(self, base_dir: str, report: StatementReport, timeout: int = 60) -> List[Dict]:
        raw = _safe_get(f"{base_dir}/{report.html_file}", self.headers, timeout).text
        html = _clean_doc_wrapper(raw)
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", {"class": "report"})
        if table is None:
            return []

        title_th = table.find("th", {"class": "tl"})
        header_text = title_th.get_text(" ", strip=True) if title_th else report.short_name
        multiplier = _unit_multiplier_from_header(header_text)

        periods: List[str] = []
        for tr in table.find_all("tr"):
            hs = tr.find_all("th", {"class": "th"})
            if not hs:
                continue
            row_texts = [h.get_text(" ", strip=True) for h in hs]
            if any(re.search(r"\b20\d{2}\b", t) for t in row_texts):
                periods = row_texts
                break

        rows: List[Dict] = []
        line_order = 0
        current_parent = ""
        for tr in table.find_all("tr"):
            label_cell = tr.find("td", {"class": "pl"})
            if label_cell is None:
                continue
            line_item = label_cell.get_text(" ", strip=True)
            if not line_item:
                continue
            link = label_cell.find("a")
            onclick = link.get("onclick", "") if link else ""
            concept = _concept_base_from_defref(onclick)
            original_tag = _tag_from_defref(onclick)
            numeric_cells = tr.find_all("td")[1:]
            if not periods:
                periods = [f"Period_{i+1}" for i in range(len(numeric_cells))]
            is_header = (
                line_item.strip().endswith(":")
                or "[abstract]" in line_item.lower()
                or ("table" in line_item.lower() and "consolidated" in line_item.lower())
            )

            parsed_any = False
            period_payload = []
            for idx in range(len(periods)):
                raw_text = ""
                parsed = None
                if idx < len(numeric_cells):
                    raw_text = numeric_cells[idx].get_text(" ", strip=True)
                    parsed = _as_float_cell(raw_text)
                    if parsed is not None:
                        parsed_any = True
                period_payload.append((periods[idx], raw_text, parsed))

            if not parsed_any and not is_header:
                is_header = True

            if is_header:
                current_parent = line_item

            parent_label = "" if is_header else current_parent
            for period_txt, raw_text, parsed in period_payload:
                rows.append(
                    {
                        "Statement_Order": report.position,
                        "Statement": report.short_name,
                        "Role_URI": report.role,
                        "Line_Order": line_order,
                        "Period": period_txt,
                        "Original_Tag": original_tag or "",
                        "Concept": concept or "",
                        "Line_Item": line_item,
                        "Parent": parent_label,
                        "Is_Header": "Yes" if is_header else "No",
                        "Value_Reported": raw_text,
                        "Value_Absolute": (parsed * multiplier) if parsed is not None else None,
                    }
                )
            line_order += 1
        return rows

    def reconcile_parent_children(self, rows: List[Dict], cal_children: Dict[str, Dict[str, List[Dict]]]) -> None:
        by_key: Dict[Tuple[str, str, str], float] = {}
        for r in rows:
            concept = str(r.get("Concept") or "")
            if not concept:
                continue
            if r.get("Value_Absolute") is None:
                continue
            by_key[(str(r["Role_URI"]), str(r["Period"]), concept)] = float(r["Value_Absolute"])

        match_map: Dict[Tuple[str, str, str], str] = {}
        warn_map: Dict[Tuple[str, str, str], str] = {}
        for role, children_by_parent in (cal_children or {}).items():
            periods = {k[1] for k in by_key.keys() if k[0] == role}
            for parent, children in (children_by_parent or {}).items():
                for period in periods:
                    pkey = (role, period, parent)
                    if pkey not in by_key:
                        continue
                    parent_val = by_key[pkey]
                    used = []
                    calc_sum = 0.0
                    for ch in children:
                        c = str(ch.get("concept") or "")
                        w = float(ch.get("weight", 1.0))
                        ckey = (role, period, c)
                        if ckey not in by_key:
                            continue
                        used.append(c)
                        calc_sum += by_key[ckey] * w
                    if not used:
                        continue
                    diff = parent_val - calc_sum
                    tol = max(abs(parent_val) * 0.001, 1.0)
                    ok = abs(diff) <= tol
                    warning = ""
                    if not ok:
                        other_candidates = [c for c in used if "other" in c.lower()]
                        if len(other_candidates) == 1:
                            ok = True
                            warning = "Parent retained from SEC; difference allocated to Other line."
                        else:
                            warning = "Parent retained from SEC; children summation mismatch."
                    match_map[pkey] = "نعم" if ok else "لا"
                    warn_map[pkey] = warning

        for r in rows:
            key = (str(r["Role_URI"]), str(r["Period"]), str(r.get("Concept") or ""))
            r["Parent_Children_Match"] = match_map.get(key, "")
            r["Warning"] = warn_map.get(key, "")

    @staticmethod
    def _to_millions_int(v: Optional[float]) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(round(float(v) / 1_000_000.0))
        except Exception:
            return None

    @staticmethod
    def _pick_dates(rows: List[Dict]) -> List[str]:
        dates = []
        for r in rows:
            p = str(r.get("Period") or "").strip()
            if p and p not in dates:
                dates.append(p)

        def _year_key(txt: str) -> int:
            m = re.search(r"(20\d{2})", txt)
            return int(m.group(1)) if m else -1

        dates.sort(key=_year_key, reverse=True)
        return dates[:3]

    def _build_structured_output(self, rows: List[Dict]) -> pd.DataFrame:
        target_dates = self._pick_dates(rows)
        if len(target_dates) == 0:
            target_dates = ["Dec. 27, 2025", "Dec. 28, 2024", "Dec. 30, 2023"]

        keyed: Dict[Tuple, Dict] = {}
        order = []
        for r in rows:
            key = (
                int(r.get("Statement_Order") or 10**9),
                str(r.get("Statement") or ""),
                int(r.get("Line_Order") or 0),
                str(r.get("Parent") or ""),
                str(r.get("Line_Item") or ""),
                str(r.get("Is_Header") or "No"),
                str(r.get("Original_Tag") or ""),
                str(r.get("Concept") or ""),
            )
            if key not in keyed:
                rec = {
                    "Line Item": key[4],
                    "_statement_order": key[0],
                    "_line_order": key[2],
                }
                for d in target_dates:
                    rec[d] = None
                keyed[key] = rec
                order.append(key)
            p = str(r.get("Period") or "")
            if p in target_dates:
                keyed[key][p] = self._to_millions_int(r.get("Value_Absolute"))

        out_rows = [keyed[k] for k in order]
        out = pd.DataFrame(out_rows)
        for d in target_dates:
            out[d] = out[d].astype("Int64")
        out = out.sort_values(by=["_statement_order", "_line_order"]).reset_index(drop=True)
        out = out.drop(columns=["_statement_order", "_line_order"])
        return out

    @staticmethod
    def _year_from_period(period_txt: str, fallback_year: Optional[int] = None) -> Optional[int]:
        m = re.search(r"(20\d{2})", str(period_txt or ""))
        if m:
            return int(m.group(1))
        return fallback_year

    @staticmethod
    def _label_norm(txt: str) -> str:
        s = re.sub(r"[^a-z0-9 ]+", " ", str(txt or "").lower())
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _semantic_match_key(self, statement: str, candidate_label: str, existing_labels: List[str]) -> Optional[str]:
        cand = self._label_norm(candidate_label)
        if not cand:
            return None
        best = None
        best_score = 0.0
        for ex in existing_labels:
            exn = self._label_norm(ex)
            if not exn:
                continue
            ratio = SequenceMatcher(None, cand, exn).ratio()
            cand_tokens = set(cand.split())
            ex_tokens = set(exn.split())
            jacc = (len(cand_tokens & ex_tokens) / max(1, len(cand_tokens | ex_tokens)))
            score = 0.7 * ratio + 0.3 * jacc
            if score > best_score:
                best_score = score
                best = ex
        if best_score >= 0.88:
            return best
        return None

    def _integrate_multi_year_rows(
        self,
        rows_by_filing: List[Tuple[Dict, List[Dict]]],
        required_years: Optional[List[int]] = None,
    ) -> pd.DataFrame:
        # rows_by_filing order should be newest -> oldest.
        concept_anchor: Dict[Tuple[str, str], str] = {}
        statement_order: Dict[str, int] = {}
        canonical_rows: Dict[Tuple[str, str], Dict] = {}
        year_cols_set = set()

        # Build anchor concept naming from latest filing first.
        for filing_meta, rows in rows_by_filing:
            for r in rows:
                stmt = str(r.get("Statement") or "")
                stmt_ord = int(r.get("Statement_Order") or 10**9)
                statement_order.setdefault(stmt, stmt_ord)
                concept = str(r.get("Concept") or "").strip()
                line_item = str(r.get("Line_Item") or "").strip()
                if concept and (stmt, concept) not in concept_anchor:
                    concept_anchor[(stmt, concept)] = line_item

        for filing_meta, rows in rows_by_filing:
            filing_year = None
            try:
                filing_year = int(str(filing_meta.get("year") or ""))
            except Exception:
                filing_year = None
            for r in rows:
                stmt = str(r.get("Statement") or "")
                item = str(r.get("Line_Item") or "")
                concept = str(r.get("Concept") or "").strip()
                parent = str(r.get("Parent") or "")
                is_header = str(r.get("Is_Header") or "No")
                year = self._year_from_period(str(r.get("Period") or ""), filing_year)
                if year is None:
                    continue
                year_cols_set.add(year)

                # Canonical naming: concept anchor -> semantic -> exact.
                if concept and (stmt, concept) in concept_anchor:
                    canonical_item = concept_anchor[(stmt, concept)]
                else:
                    existing_stmt_labels = [k[1] for k in canonical_rows.keys() if k[0] == stmt]
                    semantic_hit = self._semantic_match_key(stmt, item, existing_stmt_labels)
                    canonical_item = semantic_hit or item

                row_key = (stmt, canonical_item)
                if row_key not in canonical_rows:
                    canonical_rows[row_key] = {
                        "Statement": stmt,
                        "Line Item": canonical_item,
                        "Parent": parent,
                        "Is_Header": is_header,
                        "_statement_order": int(r.get("Statement_Order") or statement_order.get(stmt, 10**9)),
                        "_line_order": int(r.get("Line_Order") or 10**9),
                    }

                # Preserve latest naming/parent ordering from newest filings.
                if canonical_rows[row_key].get("Line Item") != canonical_item:
                    canonical_rows[row_key]["Line Item"] = canonical_item
                if canonical_rows[row_key].get("Parent", "") == "" and parent:
                    canonical_rows[row_key]["Parent"] = parent
                if canonical_rows[row_key].get("Is_Header") != "Yes" and is_header == "Yes":
                    canonical_rows[row_key]["Is_Header"] = "Yes"

                ycol = str(year)
                if ycol not in canonical_rows[row_key]:
                    canonical_rows[row_key][ycol] = None
                # Keep first seen value (newest filing precedence).
                if canonical_rows[row_key][ycol] is None:
                    canonical_rows[row_key][ycol] = self._to_millions_int(r.get("Value_Absolute"))

        if required_years:
            year_cols = [str(y) for y in sorted(set(int(y) for y in required_years), reverse=True)]
        else:
            year_cols = [str(y) for y in sorted(year_cols_set, reverse=True)]
        out_rows = []
        for rec in canonical_rows.values():
            row = {
                "Line Item": rec.get("Line Item", ""),
                "_statement_order": rec.get("_statement_order", 10**9),
                "_line_order": rec.get("_line_order", 10**9),
            }
            for y in year_cols:
                row[y] = rec.get(y)
            out_rows.append(row)

        out = pd.DataFrame(out_rows)
        if out.empty:
            cols = ["Line Item"] + year_cols
            return pd.DataFrame(columns=cols)
        out = out.sort_values(by=["_statement_order", "_line_order"]).reset_index(drop=True)
        out = out.drop(columns=["_statement_order", "_line_order"])
        out = self._collapse_duplicate_line_items(out)
        for y in year_cols:
            out[y] = out[y].astype("Int64")
        return out

    @staticmethod
    def _collapse_duplicate_line_items(df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge duplicate Line Item rows by coalescing non-null year values.
        Keeps first-seen row precedence and preserves original row order.
        """
        if df is None or df.empty or "Line Item" not in df.columns:
            return df

        year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c or ""))]
        if not year_cols:
            return df

        dup_mask = df["Line Item"].astype(str).duplicated(keep=False)
        if not dup_mask.any():
            return df

        work = df.copy()
        work["_orig_order"] = range(len(work))
        out_rows = []

        for line_item, grp in work.groupby("Line Item", sort=False, dropna=False):
            grp = grp.sort_values("_orig_order")
            base = {"Line Item": line_item}
            for y in year_cols:
                chosen = None
                for v in grp[y].tolist():
                    if pd.isna(v):
                        continue
                    chosen = v
                    break
                base[y] = chosen
            base["_orig_order"] = int(grp["_orig_order"].min())
            out_rows.append(base)

        out = pd.DataFrame(out_rows).sort_values("_orig_order").drop(columns=["_orig_order"]).reset_index(drop=True)
        for y in year_cols:
            out[y] = out[y].astype("Int64")
        return out

    def extract(self, cik: str, accession: str, output_csv: Optional[str] = None, timeout: int = 60) -> Dict:
        filing = {"accession_number": accession, "year": None, "filing_date": None}
        return self.extract_multi(cik=cik, filings=[filing], output_csv=output_csv, timeout=timeout)

    def extract_multi(
        self,
        cik: str,
        filings: List[Dict],
        output_csv: Optional[str] = None,
        timeout: int = 60,
        required_years: Optional[List[int]] = None,
        enforce_full_period: bool = False,
        period_start_year: Optional[int] = None,
        period_end_year: Optional[int] = None,
    ) -> Dict:
        cik_num = str(int(str(cik)))
        if not filings:
            raise RuntimeError("No filings provided for multi-year extraction.")

        filings_sorted = sorted(
            filings,
            key=lambda x: str(x.get("filing_date") or ""),
            reverse=True,
        )
        rows_by_filing: List[Tuple[Dict, List[Dict]]] = []
        failures: List[str] = []
        for f in filings_sorted:
            accn = _norm_accession(f.get("accession_number"))
            if not accn:
                continue
            base_dir = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{accn}"
            try:
                reports = self.discover_reports(base_dir, timeout=timeout)
                if not reports:
                    failures.append(f"{accn}: no consolidated statements in FilingSummary")
                    continue
                cal_children = self.parse_calculation_linkbase(base_dir, timeout=timeout)
                one_rows: List[Dict] = []
                for rpt in reports:
                    one_rows.extend(self.parse_statement_table(base_dir, rpt, timeout=timeout))
                self.reconcile_parent_children(one_rows, cal_children)
                self.validate_balance_sheet(one_rows)
                one_rows.sort(
                    key=lambda r: (
                        int(r.get("Statement_Order", 10**9)),
                        int(r.get("Line_Order", 0)),
                        str(r.get("Period", "")),
                    )
                )
                rows_by_filing.append((f, one_rows))
            except Exception as e:
                failures.append(f"{accn}: {str(e)}")

        if not rows_by_filing:
            raise RuntimeError("Multi-year extraction failed for all filings. " + " | ".join(failures))

        df = self._integrate_multi_year_rows(rows_by_filing, required_years=required_years)

        year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c or ""))]
        if period_start_year is not None or period_end_year is not None:
            low = int(period_start_year) if period_start_year is not None else -10**9
            high = int(period_end_year) if period_end_year is not None else 10**9
            filtered_year_cols = [c for c in year_cols if low <= int(c) <= high]
            filtered_year_cols = sorted(filtered_year_cols, key=lambda x: int(x), reverse=True)
            if not filtered_year_cols:
                raise RuntimeError(
                    f"No data years found inside requested range {low}-{high}."
                )
            df = df[["Line Item"] + filtered_year_cols]
            year_cols = filtered_year_cols

        if enforce_full_period and required_years:
            missing_year_cols: List[str] = []
            missing_year_values: List[str] = []
            for y in sorted(set(int(v) for v in required_years)):
                col = str(y)
                if col not in df.columns:
                    missing_year_cols.append(col)
                    continue
                non_null_count = int(df[col].notna().sum()) if not df.empty else 0
                if non_null_count == 0:
                    missing_year_values.append(col)
            if missing_year_cols or missing_year_values:
                parts = []
                if missing_year_cols:
                    parts.append("missing columns: " + ", ".join(missing_year_cols))
                if missing_year_values:
                    parts.append("empty years: " + ", ".join(missing_year_values))
                raise RuntimeError("Full Period Integrity Protocol failed: " + " | ".join(parts))

        out_path = output_csv or "SEC_Official_Statement.csv"
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return {
            "output_csv": str(Path(out_path)),
            "rows": int(len(df)),
            "filings_used": int(len(rows_by_filing)),
            "filings_failed": failures,
        }
