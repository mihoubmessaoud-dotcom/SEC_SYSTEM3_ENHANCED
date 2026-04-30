from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class SecPoint:
    tag: str
    unit: str
    val: float
    fy: int
    fp: str
    form: str
    start: Optional[str]
    end: Optional[str]
    filed: Optional[str]
    frame: Optional[str]


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def _is_technical_tag(tag: str) -> bool:
    """
    SEC companyfacts uses technical XBRL tags (e.g. Assets, NetIncomeLoss, dei:EntityCommonStockSharesOutstanding).
    Exported workbooks may also contain human labels (e.g. 'Total Assets', 'Accounts receivable') that are aliases.
    Those aliases should not be compared 1:1 against companyfacts keys.
    """
    s = str(tag or "").strip()
    if not s:
        return False
    if re.fullmatch(r"[a-z][a-z0-9\\-]*:[A-Za-z0-9_]+", s):
        return True
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", s):
        return True
    return False


def _normalize_units_for_compare(unit: str, *, target: str) -> Tuple[str, float]:
    """
    Return (normalized_unit, multiplier) such that: normalized_value = raw_value * multiplier.
    """
    u = str(unit or "").strip()
    t = str(target or "").strip().lower()
    if not t:
        return u, 1.0

    # Monetary
    if t in {"usd", "usd_raw"}:
        return "USD", 1.0
    if t in {"usd_million", "usd_millions"}:
        return "USD_million", 1.0 / 1_000_000.0
    if t in {"usd_billion", "usd_billions"}:
        return "USD_billion", 1.0 / 1_000_000_000.0

    # Shares
    if t in {"shares", "shares_raw"}:
        return "shares", 1.0
    if t in {"shares_million", "shares_millions"}:
        return "shares_million", 1.0 / 1_000_000.0

    # Per-share
    if t in {"per_share", "usd_per_share"}:
        return "USD/share", 1.0

    return u, 1.0


def _iter_companyfacts_points(companyfacts: Dict[str, Any], *, taxonomy: str = "us-gaap") -> Iterable[SecPoint]:
    facts = (companyfacts or {}).get("facts", {}) or {}
    tax = facts.get(taxonomy, {}) or {}
    for tag, tag_obj in tax.items():
        units = (tag_obj or {}).get("units", {}) or {}
        for unit, points in units.items():
            for p in points or []:
                fy = p.get("fy")
                fp = p.get("fp")
                form = p.get("form")
                val = p.get("val")
                if fy is None or fp is None or form is None or val is None:
                    continue
                fv = _safe_float(val)
                if fv is None:
                    continue
                yield SecPoint(
                    tag=str(tag),
                    unit=str(unit),
                    val=float(fv),
                    fy=int(fy),
                    fp=str(fp),
                    form=str(form),
                    start=p.get("start"),
                    end=p.get("end"),
                    filed=p.get("filed"),
                    frame=p.get("frame"),
                )


def _best_point(points: List[SecPoint]) -> Optional[SecPoint]:
    if not points:
        return None

    def _key(p: SecPoint) -> Tuple:
        # Prefer amended filings if newer; choose latest filed date.
        filed = str(p.filed or "")
        end = str(p.end or "")
        form_rank = 1 if "10-K/A" in p.form else 0
        # Prefer larger magnitude when duplicates exist for same end/filed (common in companyfacts).
        mag = abs(float(p.val)) if p.val is not None else 0.0
        return (filed, end, form_rank, mag)

    return sorted(points, key=_key)[-1]


def _infer_fye_month(companyfacts: Dict[str, Any], *, taxonomy: str, fp: str, forms: Tuple[str, ...]) -> Optional[int]:
    """
    Infer fiscal year-end month by looking at balance-sheet anchor `end` dates.
    For most issuers this is stable (e.g. NVDA -> Jan).
    """
    facts_root = (companyfacts or {}).get("facts", {}) or {}
    for anchor in ("Assets", "Liabilities", "StockholdersEquity"):
        try:
            tag_obj = (facts_root.get(taxonomy, {}) or {}).get(anchor, {}) or {}
            units = (tag_obj.get("units", {}) or {}).get("USD", []) or []
            months: List[int] = []
            for p in units:
                if str(p.get("fp") or "") != fp:
                    continue
                if forms and str(p.get("form") or "") not in forms:
                    continue
                end = str(p.get("end") or "")
                if len(end) >= 7:
                    try:
                        months.append(int(end[5:7]))
                    except Exception:
                        continue
            if months:
                return max(set(months), key=months.count)
        except Exception:
            continue
    return None


def _parse_date_ymd(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    ss = str(s)
    if len(ss) < 10:
        return None
    try:
        return datetime.fromisoformat(ss[0:10])
    except Exception:
        return None


def _is_annual_candidate(p: SecPoint, *, fye_month: Optional[int], is_instant: bool) -> bool:
    """
    Companyfacts can include multiple points per (tag, fy) from 10-K filings, including
    quarterly comparative values. We only want the annual 10-K value.
    """
    if not p.end:
        return False
    end_dt = _parse_date_ymd(p.end)
    if end_dt is None:
        return False
    if fye_month is not None and int(end_dt.month) != int(fye_month):
        return False

    if is_instant:
        return True

    # Duration fact: require a ~1 year span when start is available.
    start_dt = _parse_date_ymd(p.start)
    if start_dt is None:
        return False
    days = (end_dt - start_dt).days
    return 250 <= days <= 430


def _build_sec_index(
    companyfacts: Dict[str, Any],
    *,
    taxonomy: str,
    forms: Tuple[str, ...],
    fp: str,
    years: List[int],
    year_mode: str = "end_year",
) -> Dict[Tuple[str, int], SecPoint]:
    points = list(_iter_companyfacts_points(companyfacts, taxonomy=taxonomy))
    fye_month = _infer_fye_month(companyfacts, taxonomy=taxonomy, fp=fp, forms=forms)

    KNOWN_INSTANT_TAGS: set[str] = {
        # Balance sheet anchors and common instant facts
        "Assets",
        "AssetsCurrent",
        "AssetsNoncurrent",
        "Liabilities",
        "LiabilitiesCurrent",
        "LiabilitiesNoncurrent",
        "StockholdersEquity",
        "LiabilitiesAndStockholdersEquity",
        "CashAndCashEquivalentsAtCarryingValue",
        "AccountsPayableCurrent",
        "AccountsReceivableNetCurrent",
        "InventoryNet",
        "OtherAssets",
        "OtherAssetsCurrent",
        "RetainedEarningsAccumulatedDeficit",
        "DebtCurrent",
        "LongTermDebtNoncurrent",
        "DebtNoncurrent",
        # Shares outstanding are point-in-time
        "EntityCommonStockSharesOutstanding",
    }

    idx: Dict[Tuple[str, int], List[SecPoint]] = {}
    max_year = max(years) if years else None
    for p in points:
        if p.fp != fp:
            continue
        # Map SEC points to the workbook "Year" columns.
        #
        # Historically this project has used two conventions:
        # - end_year (default): label facts by the calendar year of the `end` date.
        # - fiscal_label: for issuers with fiscal year ending in Jan/Feb, label duration facts as prior-year
        #   (e.g. FY ended 2020-01-26 => 2019). Some older exports used this convention.
        eff_year = None
        try:
            if p.end and len(str(p.end)) >= 10:
                yy = int(str(p.end)[0:4])
                mm = int(str(p.end)[5:7])
                # Legacy exports sometimes applied a strict calendar end-year cutoff before remapping.
                if max_year is not None and str(year_mode).strip().lower() in {"fiscal_label", "legacy_v2"}:
                    if yy > int(max_year):
                        continue
                is_instant = False
                try:
                    if p.frame and str(p.frame).upper().endswith("I"):
                        is_instant = True
                except Exception:
                    is_instant = False
                if (str(p.tag) in KNOWN_INSTANT_TAGS) or (p.start in (None, "", "None")):
                    is_instant = True
                # Filter out non-annual points inside 10-K companyfacts buckets.
                if not _is_annual_candidate(p, fye_month=fye_month, is_instant=is_instant):
                    continue
                ym = str(year_mode).strip().lower()
                if ym == "legacy_v2":
                    # Old parity runs (259/293) used: if fiscal year ends Jan/Feb, label as prior year
                    # for *both* instant and duration facts.
                    eff_year = yy - 1 if mm <= 2 else yy
                elif ym == "fiscal_label":
                    # Label duration facts as prior year for early fiscal-year ends.
                    eff_year = yy - 1 if ((not is_instant) and (mm <= 2)) else yy
                else:
                    eff_year = yy
        except Exception:
            eff_year = None
        if eff_year is None:
            eff_year = int(p.fy)
        if years and eff_year not in years:
            continue
        if forms and p.form not in forms:
            continue
        idx.setdefault((p.tag, int(eff_year)), []).append(p)
    out: Dict[Tuple[str, int], SecPoint] = {}
    for k, lst in idx.items():
        best = _best_point(lst)
        if best:
            out[k] = best
    return out


def _read_excel_layer1(excel_path: Path) -> pd.DataFrame:
    return pd.read_excel(excel_path, sheet_name="Layer1_Raw_SEC")


def compare_excel_vs_companyfacts(
    *,
    excel_path: Path,
    companyfacts_path: Path,
    start_year: int,
    end_year: int,
    taxonomy: str = "us-gaap",
    fp: str = "FY",
    forms: Tuple[str, ...] = ("10-K", "10-K/A"),
    usd_scale_target: str = "usd_million",
    shares_scale_target: str = "shares_million",
    per_share_target: str = "per_share",
    abs_tol: float = 0.01,
    rel_tol: float = 1e-6,
    year_mode: str = "end_year",
) -> pd.DataFrame:
    """
    Compare the workbook `Layer1_Raw_SEC` concept time-series (already normalized for display)
    with SEC companyfacts JSON.
    """
    years = [y for y in range(int(start_year), int(end_year) + 1)]
    df = _read_excel_layer1(excel_path)
    if df.empty:
        return pd.DataFrame([{"Status": "ERROR", "Reason": "EMPTY_LAYER1"}])
    if "Raw Label" not in df.columns:
        # Backward-compatible: first column is raw label
        df = df.rename(columns={df.columns[0]: "Raw Label"})

    year_cols = [c for c in df.columns if str(c).strip().isdigit()]
    year_cols = [c for c in year_cols if int(str(c)) in years]

    companyfacts = _load_json(companyfacts_path)
    # Tag universe in this companyfacts payload (used to ignore local aliases).
    facts_root = (companyfacts or {}).get("facts", {}) or {}
    sec_tag_set = set(((facts_root.get(taxonomy, {}) or {}).keys()))
    sec_index = _build_sec_index(
        companyfacts,
        taxonomy=taxonomy,
        forms=forms,
        fp=fp,
        years=years,
        year_mode=year_mode,
    )

    # Some SEC filers switch tags across years (taxonomy evolution). The workbook may output a stable
    # canonical tag (e.g. "Revenues") while SEC companyfacts uses newer tags (e.g. "RevenueFromContract...").
    # Provide a limited alias map for parity checks.
    TAG_ALIASES: Dict[str, List[str]] = {
        "Revenues": [
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
        ],
        "CostOfRevenue": [
            "CostOfGoodsAndServicesSold",
            "CostOfGoodsSold",
        ],
        "DepreciationDepletionAndAmortization": [
            "DepreciationAndAmortization",
            "Depreciation",
            "AmortizationOfIntangibleAssets",
        ],
    }

    def _normalize_sec_value(point: SecPoint) -> Tuple[str, float, str]:
        sec_unit = str(point.unit or "")
        target = ""
        if sec_unit.lower() == "usd":
            target = usd_scale_target
        elif sec_unit.lower() == "shares":
            target = shares_scale_target
        elif "usd" in sec_unit.lower() and "share" in sec_unit.lower():
            target = per_share_target
        _norm_u, mult = _normalize_units_for_compare(sec_unit, target=target)
        sec_val_norm = float(point.val) * float(mult)
        return sec_unit, sec_val_norm, target

    rows = []
    skipped_aliases: set[str] = set()
    for _, r in df.iterrows():
        tag = str(r.get("Raw Label") or "").strip()
        if not tag:
            continue
        if not _is_technical_tag(tag):
            # Skip human labels/aliases from Layer1_Raw_SEC.
            continue
        if sec_tag_set and tag not in sec_tag_set:
            skipped_aliases.add(tag)
            continue
        for yc in year_cols:
            y = int(str(yc))
            excel_val = _safe_float(r.get(yc))
            sec_point = sec_index.get((tag, int(y)))
            if sec_point is None:
                for alt in TAG_ALIASES.get(tag, []):
                    sec_point = sec_index.get((alt, int(y)))
                    if sec_point is not None:
                        break
            if sec_point is None:
                if excel_val is None:
                    continue
                rows.append(
                    {
                        "Tag": tag,
                        "Year": y,
                        "Excel_Value": excel_val,
                        "SEC_Value": None,
                        "SEC_Unit": None,
                        "SEC_Form": None,
                        "SEC_Filed": None,
                        "SEC_End": None,
                        "Delta": None,
                        "Delta_Pct": None,
                        "Match": False,
                        "Reason": "MISSING_IN_SEC",
                        "Severity": "MISSING",
                    }
                )
                continue
            sec_unit, sec_val_norm, used_target = _normalize_sec_value(sec_point)

            rounding_abs_tol = abs_tol
            if used_target in {"usd_million", "usd_millions"}:
                # Excel exports often round USD_million to integers; allow rounding drift up to ~0.55 million.
                rounding_abs_tol = max(rounding_abs_tol, 0.55)

            if excel_val is None:
                rows.append(
                    {
                        "Tag": tag,
                        "Year": y,
                        "Excel_Value": None,
                        "SEC_Value": sec_val_norm,
                        "SEC_Unit": sec_unit,
                        "SEC_Form": sec_point.form,
                        "SEC_Filed": sec_point.filed,
                        "SEC_End": sec_point.end,
                        "Delta": None,
                        "Delta_Pct": None,
                        "Match": False,
                        "Reason": "MISSING_IN_EXCEL",
                        "Severity": "MISSING",
                    }
                )
                continue

            # Split-basis handling for shares / per-share metrics:
            # the app may output split-adjusted values while companyfacts are raw.
            split_factor = None
            try:
                if used_target in {"shares_million", "shares_millions"} and sec_val_norm and excel_val and sec_val_norm > 0 and excel_val > 0:
                    # Either Excel is split-adjusted (bigger shares) or SEC is split-adjusted.
                    ratio_a = float(excel_val) / float(sec_val_norm)
                    ratio_b = float(sec_val_norm) / float(excel_val)
                    for ratio, mode in ((ratio_a, "sec_up"), (ratio_b, "sec_down")):
                        nearest = int(round(ratio))
                        if nearest in {2, 3, 4, 5, 10, 20, 40} and abs(ratio - nearest) <= (0.05 * nearest):
                            split_factor = nearest
                            if mode == "sec_up":
                                sec_val_norm = float(sec_val_norm) * float(nearest)
                            else:
                                sec_val_norm = float(sec_val_norm) / float(nearest)
                            break
                if used_target in {"per_share", "usd_per_share"} and sec_val_norm and excel_val and sec_val_norm > 0 and excel_val > 0:
                    ratio = float(sec_val_norm) / float(excel_val)
                    nearest = int(round(ratio))
                    if nearest in {2, 3, 4, 5, 10, 20, 40} and abs(ratio - nearest) <= (0.05 * nearest):
                        split_factor = nearest
                        sec_val_norm = float(sec_val_norm) / float(nearest)
            except Exception:
                split_factor = None

            delta = excel_val - sec_val_norm
            denom = max(abs(sec_val_norm), abs(excel_val), 1.0)
            delta_pct = (delta / denom) * 100.0
            ok = (abs(delta) <= rounding_abs_tol) or (abs(delta) <= (rel_tol * denom))
            # Common sign convention: app may store expenses/cash outflows as negative,
            # while XBRL stores them as positive (or vice versa).
            if not ok and excel_val is not None and sec_val_norm is not None:
                if (excel_val * sec_val_norm) < 0:
                    if abs(abs(excel_val) - abs(sec_val_norm)) <= rounding_abs_tol:
                        ok = True
            severity = "OK"
            if not ok:
                if abs(delta) <= 0.55 and used_target in {"usd_million", "usd_millions"}:
                    severity = "ROUNDING_ONLY"
                else:
                    severity = "MISMATCH"
            if split_factor is not None and ok:
                severity = "SPLIT_ADJUSTED"
            rows.append(
                {
                    "Tag": tag,
                    "Year": y,
                    "Excel_Value": excel_val,
                    "SEC_Value": sec_val_norm,
                    "SEC_Unit": sec_unit,
                    "SEC_Form": sec_point.form,
                    "SEC_Filed": sec_point.filed,
                    "SEC_End": sec_point.end,
                    "Delta": delta,
                    "Delta_Pct": delta_pct,
                    "Match": bool(ok),
                    "Reason": "OK" if ok else "VALUE_MISMATCH",
                    "Severity": severity,
                    "Split_Factor": split_factor,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        alias_txt = ", ".join(sorted(list(skipped_aliases))[:40])
        return pd.DataFrame([{"Status": "OK", "Reason": "NO_COMPARABLE_ROWS", "Skipped_Alias_Tags": alias_txt}])
    # Attach alias-skip context for the caller (optional).
    try:
        out.attrs["skipped_alias_tags"] = sorted(list(skipped_aliases))
    except Exception:
        pass
    return out.sort_values(["Tag", "Year"]).reset_index(drop=True)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to exported xlsx (must include Layer1_Raw_SEC).")
    ap.add_argument("--companyfacts", required=True, help="Path to SEC companyfacts JSON (downloaded).")
    ap.add_argument("--start-year", type=int, default=2015)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--taxonomy", default="us-gaap")
    ap.add_argument("--fp", default="FY")
    ap.add_argument("--abs-tol", type=float, default=0.01)
    ap.add_argument("--rel-tol", type=float, default=1e-6)
    ap.add_argument(
        "--year-mode",
        default="end_year",
        choices=["end_year", "fiscal_label", "legacy_v2"],
        help="How to map SEC fact end-dates to workbook Year columns.",
    )
    ap.add_argument("--out", default="", help="Optional output CSV path for the diff table.")
    args = ap.parse_args()

    excel_path = Path(args.excel).expanduser().resolve()
    facts_path = Path(args.companyfacts).expanduser().resolve()

    df = compare_excel_vs_companyfacts(
        excel_path=excel_path,
        companyfacts_path=facts_path,
        start_year=args.start_year,
        end_year=args.end_year,
        taxonomy=args.taxonomy,
        fp=args.fp,
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
        year_mode=args.year_mode,
    )

    mismatches = df[(df.get("Match") == False)] if "Match" in df.columns else df
    print(f"Compared: excel={excel_path.name} vs facts={facts_path.name}")
    if "Match" in df.columns:
        print(f"Rows: {len(df)} | Mismatches: {len(mismatches)}")
    else:
        print(f"Rows: {len(df)}")

    if not mismatches.empty:
        cols = [c for c in ["Tag", "Year", "Excel_Value", "SEC_Value", "SEC_Unit", "Delta", "Delta_Pct", "Reason", "SEC_Form", "SEC_Filed"] if c in mismatches.columns]
        print(mismatches[cols].head(80).to_string(index=False))

    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        # Avoid Windows console encoding issues when printing non-ASCII absolute paths.
        try:
            print(f"Wrote diff CSV: {args.out}")
        except Exception:
            print("Wrote diff CSV.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
