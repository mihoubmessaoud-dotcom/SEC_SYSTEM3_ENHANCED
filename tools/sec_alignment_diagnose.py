from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class SecVal:
    end_year: int
    end: str
    filed: str
    unit: str
    val: float


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


def _load_companyfacts(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_sec_annual_series(
    companyfacts: Dict[str, Any],
    *,
    taxonomy: str,
    tag: str,
    fp: str,
    forms: Tuple[str, ...],
) -> Dict[int, SecVal]:
    facts_root = (companyfacts or {}).get("facts", {}) or {}
    tag_obj = (facts_root.get(taxonomy, {}) or {}).get(tag, {}) or {}
    units = (tag_obj.get("units", {}) or {}).get("USD", []) or []
    best_by_end_year: Dict[int, SecVal] = {}
    for p in units:
        if str(p.get("fp") or "") != fp:
            continue
        if forms and str(p.get("form") or "") not in forms:
            continue
        end = str(p.get("end") or "")
        filed = str(p.get("filed") or "")
        if len(end) < 10:
            continue
        try:
            end_year = int(end[0:4])
        except Exception:
            continue
        val = _safe_float(p.get("val"))
        if val is None:
            continue
        cur = best_by_end_year.get(end_year)
        # prefer latest filed for same end-year; if tie, prefer larger magnitude
        if cur is None or (filed > cur.filed) or (filed == cur.filed and abs(val) > abs(cur.val)):
            best_by_end_year[end_year] = SecVal(end_year=end_year, end=end, filed=filed, unit="USD", val=float(val))
    return best_by_end_year


def main() -> int:
    try:
        import sys

        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True)
    ap.add_argument("--companyfacts", required=True)
    ap.add_argument("--taxonomy", default="us-gaap")
    ap.add_argument("--fp", default="FY")
    ap.add_argument("--start-year", type=int, default=2015)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--out", default="outputs/sec_alignment_report.csv")
    args = ap.parse_args()

    excel_path = Path(args.excel).expanduser().resolve()
    facts_path = Path(args.companyfacts).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(excel_path, sheet_name="Layer1_Raw_SEC")
    if df.empty:
        raise SystemExit("Empty Layer1_Raw_SEC")
    if "Raw Label" not in df.columns:
        df = df.rename(columns={df.columns[0]: "Raw Label"})

    years = [y for y in range(int(args.start_year), int(args.end_year) + 1)]
    year_cols = [c for c in df.columns if str(c).strip().isdigit() and int(str(c)) in years]

    companyfacts = _load_companyfacts(facts_path)
    facts_root = (companyfacts or {}).get("facts", {}) or {}
    sec_tag_set = set(((facts_root.get(args.taxonomy, {}) or {}).keys()))

    forms = ("10-K", "10-K/A")

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        tag = str(r.get("Raw Label") or "").strip()
        if not tag or tag not in sec_tag_set:
            continue
        sec_series = _build_sec_annual_series(companyfacts, taxonomy=args.taxonomy, tag=tag, fp=args.fp, forms=forms)
        if not sec_series:
            continue

        # Evaluate offsets: excel_year y compares to SEC end_year (y + offset)
        offsets = [-2, -1, 0, 1, 2]
        best_offset = None
        best_match = -1
        best_mae = None
        for off in offsets:
            diffs = []
            matches = 0
            for yc in year_cols:
                y = int(str(yc))
                excel_v = _safe_float(r.get(yc))
                if excel_v is None:
                    continue
                sec_v = sec_series.get(y + off)
                if sec_v is None:
                    continue
                sec_m = float(sec_v.val) / 1_000_000.0
                diffs.append(abs(excel_v - sec_m))
                if abs(excel_v - sec_m) <= 0.55:
                    matches += 1
            if not diffs:
                continue
            mae = sum(diffs) / len(diffs)
            if matches > best_match or (matches == best_match and (best_mae is None or mae < best_mae)):
                best_match = matches
                best_mae = mae
                best_offset = off

        rows.append(
            {
                "Tag": tag,
                "Best_Offset": best_offset,
                "Best_Match_Years": best_match,
                "Best_MAE_USD_M": best_mae,
                "Excel_NonNull_Years": int(sum(_safe_float(r.get(c)) is not None for c in year_cols)),
                "SEC_EndYear_Coverage": len(sec_series),
            }
        )

    out = pd.DataFrame(rows).sort_values(["Best_Match_Years", "Best_MAE_USD_M"], ascending=[False, True])
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

