#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Empirical calibration of confidence policy from exported Excel workbooks.

Outputs:
- Updated config/confidence_calibration_policy.json
- Calibration report in outputs/confidence_calibration_fit_*.json
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


REQUIRED_INPUTS = {
    "gross_margin": ["Revenues|Revenue|SalesRevenueNet", "GrossProfit"],
    "operating_margin": ["Revenues|Revenue|SalesRevenueNet", "OperatingIncomeLoss|OperatingIncome"],
    "net_margin": ["Revenues|Revenue|SalesRevenueNet", "NetIncomeLoss|ProfitLoss"],
    "current_ratio": ["AssetsCurrent|CurrentAssets", "LiabilitiesCurrent|CurrentLiabilities"],
    "quick_ratio": ["AssetsCurrent|CurrentAssets", "InventoryNet|Inventory", "LiabilitiesCurrent|CurrentLiabilities"],
    "cash_ratio": ["CashAndCashEquivalentsAtCarryingValue|CashAndCashEquivalents|Cash", "LiabilitiesCurrent|CurrentLiabilities"],
    "debt_to_equity": ["market:total_debt|yahoo:total_debt|Debt|TotalDebt", "StockholdersEquity|TotalEquity"],
    "debt_to_assets": ["market:total_debt|yahoo:total_debt|Debt|TotalDebt", "Assets|TotalAssets"],
    "interest_coverage": ["OperatingIncomeLoss|OperatingIncome", "InterestExpense|InterestAndDebtExpense|InterestExpenseNonOperating|InterestExpenseDebt"],
    "eps_basic": ["NetIncomeLoss|ProfitLoss", "market:shares_outstanding|yahoo:shares_outstanding|WeightedAverageNumberOfSharesOutstandingBasic|CommonStockSharesOutstanding"],
    "book_value_per_share": ["StockholdersEquity|TotalEquity", "market:shares_outstanding|yahoo:shares_outstanding|CommonStockSharesOutstanding"],
    "pe_ratio": ["market:price|yahoo:price", "eps_basic"],
    "pb_ratio": ["market:price|yahoo:price", "book_value_per_share"],
    "enterprise_value": ["market:market_cap|yahoo:market_cap", "market:total_debt|yahoo:total_debt", "CashAndCashEquivalentsAtCarryingValue|CashAndCashEquivalents|Cash"],
    "ev_ebitda": ["enterprise_value", "EBITDA|Ebitda|OperatingIncomeLoss|OperatingIncome"],
    "fcf_yield": ["free_cash_flow", "market:market_cap|yahoo:market_cap"],
}


def _safe_num(v):
    try:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return None
        if s.upper().startswith("N/A"):
            return None
        s = s.replace(",", "").replace("$", "").replace(" ", "")
        if s.endswith("%"):
            return float(s[:-1]) / 100.0
        return float(s)
    except Exception:
        return None


def _sheet_to_map(df: Optional[pd.DataFrame], label_cols: List[str]) -> Dict[int, Dict[str, float]]:
    out: Dict[int, Dict[str, float]] = {}
    if df is None or df.empty:
        return out
    cols = [str(c) for c in df.columns]
    label_col = next((c for c in label_cols if c in cols), cols[0] if cols else None)
    if not label_col:
        return out
    year_cols = []
    for c in cols:
        try:
            y = int(str(c))
            if 1900 <= y <= 2100:
                year_cols.append((c, y))
        except Exception:
            continue
    for _, r in df.iterrows():
        k = str(r.get(label_col) or "").strip()
        if not k:
            continue
        for yc, y in year_cols:
            v = _safe_num(r.get(yc))
            if v is not None:
                out.setdefault(y, {})[k] = v
    return out


def _first(row: Dict[str, float], keys_pipe: str):
    for k in keys_pipe.split("|"):
        if k in row and _safe_num(row.get(k)) is not None:
            return _safe_num(row.get(k))
    return None


def _compute_ratio(metric: str, y: int, l1: Dict[int, Dict[str, float]], l2: Dict[int, Dict[str, float]], l4: Dict[int, Dict[str, float]], ratios: Dict[int, Dict[str, float]]):
    r1 = l1.get(y, {}) or {}
    r2 = l2.get(y, {}) or {}
    r4 = l4.get(y, {}) or {}
    rr = ratios.get(y, {}) or {}

    rev = _first(r1, "Revenues|Revenue|SalesRevenueNet")
    gp = _first(r1, "GrossProfit")
    op = _first(r1, "OperatingIncomeLoss|OperatingIncome")
    ni = _first(r1, "NetIncomeLoss|ProfitLoss")
    ca = _first(r1, "AssetsCurrent|CurrentAssets")
    cl = _first(r1, "LiabilitiesCurrent|CurrentLiabilities")
    inv = _first(r1, "InventoryNet|Inventory")
    cash = _first(r1, "CashAndCashEquivalentsAtCarryingValue|CashAndCashEquivalents|Cash")
    assets = _first(r1, "Assets|TotalAssets")
    eq = _first(r1, "StockholdersEquity|TotalEquity")
    debt = _first(r2, "market:total_debt|yahoo:total_debt")
    if debt is None:
        debt = _first(r4, "yahoo:total_debt")
    mcap = _first(r2, "market:market_cap|yahoo:market_cap")
    if mcap is None:
        mcap = _first(r4, "yahoo:market_cap")
    px = _first(r2, "market:price|yahoo:price")
    if px is None:
        px = _first(r4, "yahoo:price")
    sh = _first(r2, "market:shares_outstanding|yahoo:shares_outstanding")
    if sh is None:
        sh = _first(r4, "yahoo:shares_outstanding")
    if sh is None:
        sh = _first(r1, "WeightedAverageNumberOfSharesOutstandingBasic|CommonStockSharesOutstanding")
    intr = _first(r1, "InterestExpense|InterestAndDebtExpense|InterestExpenseNonOperating|InterestExpenseDebt")
    ebitda = _first(r1, "EBITDA|Ebitda")
    if ebitda is None and op is not None:
        dep = _first(r1, "DepreciationDepletionAndAmortization|DepreciationAmortization")
        ebitda = float(op) + float(dep or 0.0)
    fcf = _safe_num(rr.get("free_cash_flow"))
    if fcf is None:
        ocf = _first(r1, "NetCashProvidedByUsedInOperatingActivities|OperatingCashFlow")
        capex = _safe_num(rr.get("capital_expenditures")) or _first(r1, "PaymentsToAcquirePropertyPlantAndEquipment")
        if ocf is not None and capex is not None:
            fcf = float(ocf) - abs(float(capex))

    if metric == "gross_margin" and rev not in (None, 0) and gp is not None:
        return gp / rev
    if metric == "operating_margin" and rev not in (None, 0) and op is not None:
        return op / rev
    if metric == "net_margin" and rev not in (None, 0) and ni is not None:
        return ni / rev
    if metric == "current_ratio" and cl not in (None, 0) and ca is not None:
        return ca / cl
    if metric == "quick_ratio" and cl not in (None, 0) and ca is not None:
        return (ca - float(inv or 0.0)) / cl
    if metric == "cash_ratio" and cl not in (None, 0) and cash is not None:
        return cash / cl
    if metric == "debt_to_equity" and eq not in (None, 0) and debt is not None:
        return debt / eq
    if metric == "debt_to_assets" and assets not in (None, 0) and debt is not None:
        return debt / assets
    if metric == "interest_coverage" and op is not None and intr not in (None, 0):
        return op / abs(intr)
    if metric == "eps_basic" and ni is not None and sh not in (None, 0):
        return (ni * 1_000_000.0) / sh
    if metric == "book_value_per_share" and eq is not None and sh not in (None, 0):
        return (eq * 1_000_000.0) / sh
    if metric == "pe_ratio":
        eps = _safe_num(rr.get("eps_basic"))
        if eps is None:
            eps = _compute_ratio("eps_basic", y, l1, l2, l4, ratios)
        if px not in (None, 0) and eps not in (None, 0):
            return px / eps
    if metric == "pb_ratio":
        bvps = _safe_num(rr.get("book_value_per_share"))
        if bvps is None:
            bvps = _compute_ratio("book_value_per_share", y, l1, l2, l4, ratios)
        if px not in (None, 0) and bvps not in (None, 0):
            return px / bvps
    if metric == "enterprise_value" and mcap is not None and debt is not None:
        return mcap + debt - float(cash or 0.0)
    if metric == "ev_ebitda":
        ev = _safe_num(rr.get("enterprise_value"))
        if ev is None:
            ev = _compute_ratio("enterprise_value", y, l1, l2, l4, ratios)
        if ev not in (None, 0) and ebitda not in (None, 0):
            return ev / ebitda
    if metric == "fcf_yield" and fcf is not None and mcap not in (None, 0):
        return fcf / mcap
    return None


@dataclass
class Sample:
    p_pred: float
    y_true: int
    source: str
    year: int
    weight: float


def _predict_probability(source: str, n_inputs: int, n_missing: int, n_raw: int, rel_hint: Optional[float], policy: dict) -> float:
    pri = policy.get("source_priors", {})
    ew = policy.get("evidence_weights", {})
    pair = pri.get(source) or pri.get("default") or [12.0, 4.0]
    a0, b0 = float(pair[0]), float(pair[1])
    success = min(float(ew.get("input_concept_max", 6.0)), float(ew.get("input_concept_success", 0.9)) * n_inputs)
    success += min(float(ew.get("raw_value_max", 4.0)), float(ew.get("raw_value_success", 0.6)) * n_raw)
    failure = min(float(ew.get("missing_input_max", 8.0)), float(ew.get("missing_input_penalty", 1.8)) * n_missing)
    if source == "ui_fallback":
        failure += float(ew.get("fallback_penalty", 2.5))
        success += float(ew.get("fallback_success_credit", 1.5))
    if rel_hint is not None:
        hw = float(ew.get("hint_weight", 2.0))
        p = max(0.0, min(1.0, rel_hint / 100.0))
        success += hw * p
        failure += hw * (1 - p)
    a = a0 + success
    b = b0 + failure
    if a <= 0 or b <= 0:
        return 0.0
    mean = a / (a + b)
    var = (a * b) / (((a + b) ** 2) * (a + b + 1.0))
    z = float(policy.get("lcb_z_score", 1.0))
    return max(0.0, min(1.0, mean - z * (var ** 0.5)))


def _fit_curve(samples: List[Sample], bins: int = 10):
    if not samples:
        return [[0.0, 0.0], [1.0, 0.99]], []
    buckets = [[] for _ in range(bins)]
    for s in samples:
        idx = min(bins - 1, max(0, int(s.p_pred * bins)))
        buckets[idx].append(s)
    raw_bins = []
    for i, b in enumerate(buckets):
        if not b:
            continue
        wsum = sum(max(1e-9, x.weight) for x in b)
        p_avg = sum(x.p_pred * max(1e-9, x.weight) for x in b) / wsum
        # Laplace smoothing to avoid overfitting small bins
        acc = (sum(float(x.y_true) * max(1e-9, x.weight) for x in b) + 1.0) / (wsum + 2.0)
        raw_bins.append({"bin": i, "count": len(b), "weight_sum": wsum, "pred_avg": float(p_avg), "emp_acc": float(acc)})

    if not raw_bins:
        return [[0.0, 0.0], [1.0, 0.99]], []

    # Proper isotonic regression (Pool Adjacent Violators) on empirical accuracies.
    blocks = []
    for rb in raw_bins:
        blocks.append({
            "w": float(rb["weight_sum"]),
            "sum_wy": float(rb["weight_sum"]) * float(rb["emp_acc"]),
            "idxs": [rb],
        })
        while len(blocks) >= 2:
            m1 = blocks[-2]["sum_wy"] / blocks[-2]["w"]
            m2 = blocks[-1]["sum_wy"] / blocks[-1]["w"]
            if m1 <= m2:
                break
            b2 = blocks.pop()
            b1 = blocks.pop()
            blocks.append({
                "w": b1["w"] + b2["w"],
                "sum_wy": b1["sum_wy"] + b2["sum_wy"],
                "idxs": b1["idxs"] + b2["idxs"],
            })

    iso_rows = []
    for b in blocks:
        mean = b["sum_wy"] / b["w"]
        for rb in b["idxs"]:
            iso_rows.append({
                "bin": rb["bin"],
                "count": rb["count"],
                "weight_sum": rb["weight_sum"],
                "pred_avg": rb["pred_avg"],
                "emp_acc": rb["emp_acc"],
                "mono_acc": mean,
            })
    iso_rows = sorted(iso_rows, key=lambda r: r["pred_avg"])

    pts = [(0.0, 0.0)]
    bin_rows = []
    for r in iso_rows:
        pts.append((round(r["pred_avg"], 4), round(r["mono_acc"], 4)))
        bin_rows.append({
            "bin": r["bin"],
            "count": r["count"],
            "weight_sum": round(r.get("weight_sum", 0.0), 4),
            "pred_avg": round(r["pred_avg"], 4),
            "emp_acc": round(r["emp_acc"], 4),
            "mono_acc": round(r["mono_acc"], 4),
        })
    pts.append((1.0, 0.99))
    # Ensure strict x-order unique
    uniq = {}
    for x, y in pts:
        uniq[float(x)] = float(y)
    out = [[round(x, 4), round(uniq[x], 4)] for x in sorted(uniq.keys())]
    return out, bin_rows


def _interp(curve, p):
    p = max(0.0, min(1.0, float(p)))
    pts = sorted((float(a), float(b)) for a, b in curve)
    if p <= pts[0][0]:
        return pts[0][1]
    if p >= pts[-1][0]:
        return pts[-1][1]
    for i in range(1, len(pts)):
        x0, y0 = pts[i - 1]
        x1, y1 = pts[i]
        if x0 <= p <= x1:
            if abs(x1 - x0) < 1e-12:
                return y1
            t = (p - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return p


def _brier(samples: List[Sample], curve=None):
    if not samples:
        return None
    s = 0.0
    wtot = 0.0
    for x in samples:
        p = _interp(curve, x.p_pred) if curve else x.p_pred
        w = max(1e-9, x.weight)
        s += w * ((p - float(x.y_true)) ** 2)
        wtot += w
    return s / max(1e-9, wtot)


def _year_weight(year: int, custom: Optional[Dict[int, float]] = None) -> float:
    if custom and year in custom:
        return float(custom[year])
    # Approved recency window weights (2025..2019)
    default = {
        2025: 1.00,
        2024: 0.98,
        2023: 0.95,
        2022: 0.90,
        2021: 0.84,
        2020: 0.78,
        2019: 0.72,
    }
    if year in default:
        return float(default[year])
    if year < 2019:
        return 0.20
    return 0.60


def _discover_workbooks(paths: List[str]) -> List[Path]:
    out: List[Path] = []
    for ptxt in paths:
        p = Path(ptxt)
        if p.is_file() and p.suffix.lower() in (".xlsx", ".xlsm"):
            out.append(p)
        elif p.is_dir():
            out.extend([x for x in p.rglob("*.xlsx") if x.is_file()])
    # uniq
    uniq = {str(x.resolve()).lower(): x for x in out}
    return list(uniq.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="*", default=["exports/manual_exports", "outputs/ui_consistency_check"], help="xlsx files or folders")
    ap.add_argument("--policy", default="config/confidence_calibration_policy.json")
    ap.add_argument("--out-report", default="")
    args = ap.parse_args()

    policy_path = Path(args.policy)
    policy = json.loads(policy_path.read_text(encoding="utf-8")) if policy_path.exists() else {}

    files = _discover_workbooks(args.inputs)
    samples: List[Sample] = []
    sample_count_by_source: Dict[str, int] = {}
    acc_by_source: Dict[str, List[int]] = {}
    weight_by_year: Dict[int, float] = {}

    target_metrics = list(REQUIRED_INPUTS.keys())
    tol_rel = {
        "ev_ebitda": 0.15,
        "pe_ratio": 0.15,
        "pb_ratio": 0.12,
        "enterprise_value": 0.10,
        "fcf_yield": 0.12,
    }

    for wb in files:
        try:
            xl = pd.ExcelFile(wb)
            if "Ratios" not in xl.sheet_names:
                continue
            rat_df = pd.read_excel(wb, sheet_name="Ratios")
            l1_df = pd.read_excel(wb, sheet_name="Layer1_Raw_SEC") if "Layer1_Raw_SEC" in xl.sheet_names else None
            l2_df = pd.read_excel(wb, sheet_name="Layer2_Market") if "Layer2_Market" in xl.sheet_names else None
            l4_df = pd.read_excel(wb, sheet_name="Layer4_Yahoo") if "Layer4_Yahoo" in xl.sheet_names else None
        except Exception:
            continue

        ratios_map = _sheet_to_map(rat_df, ["Metric", "Ratio"])
        l1_map = _sheet_to_map(l1_df, ["Raw Label", "Line Item", "Concept", "Item"])
        l2_map = _sheet_to_map(l2_df, ["Normalized Label", "Item"])
        l4_map = _sheet_to_map(l4_df, ["Normalized Label", "Item"])

        metric_col = "Metric" if "Metric" in rat_df.columns else (rat_df.columns[0] if len(rat_df.columns) else None)
        if not metric_col:
            continue
        years = [int(c) for c in rat_df.columns if str(c).isdigit()]

        rows = {str(r.get(metric_col)).strip().lower(): r for _, r in rat_df.iterrows()}
        for m in target_metrics:
            r = rows.get(m)
            if r is None:
                continue
            for y in years:
                reported = _safe_num(r.get(str(y)) if str(y) in r else r.get(y))
                if reported is None:
                    continue
                truth = _compute_ratio(m, y, l1_map, l2_map, l4_map, ratios_map)
                if truth is None:
                    continue
                err = abs(reported - truth)
                rel_thr = tol_rel.get(m, 0.05)
                ok = int(err <= (1e-9 + rel_thr * max(1e-9, abs(truth))))

                required = REQUIRED_INPUTS.get(m, [])
                n_in = len(required)
                n_missing = 0
                n_raw = 0
                for req in required:
                    val = None
                    if req in ("eps_basic", "book_value_per_share", "enterprise_value", "free_cash_flow"):
                        val = _safe_num((ratios_map.get(y, {}) or {}).get(req))
                    else:
                        val = _first(l1_map.get(y, {}) or {}, req)
                        if val is None:
                            val = _first(l2_map.get(y, {}) or {}, req)
                        if val is None:
                            val = _first(l4_map.get(y, {}) or {}, req)
                    if val is None:
                        n_missing += 1
                    else:
                        n_raw += 1

                src = "ratio_engine" if n_missing == 0 else "ui_fallback"
                p = _predict_probability(src, n_in, n_missing, n_raw, rel_hint=None, policy=policy)
                yw = _year_weight(y)
                samples.append(Sample(p_pred=p, y_true=ok, source=src, year=y, weight=yw))
                weight_by_year[y] = weight_by_year.get(y, 0.0) + yw
                sample_count_by_source[src] = sample_count_by_source.get(src, 0) + 1
                acc_by_source.setdefault(src, []).append(ok)

    curve, bin_rows = _fit_curve(samples, bins=10)
    brier_before = _brier(samples)
    brier_after = _brier(samples, curve=curve)

    # Empirical source priors (weakly regularized)
    src_priors = dict(policy.get("source_priors", {}))
    for src, ys in acc_by_source.items():
        n = len(ys)
        if n < 20:
            continue
        acc = sum(ys) / max(1, n)
        strength = max(8.0, min(30.0, n / 25.0))
        a = max(1.0, acc * strength)
        b = max(1.0, (1.0 - acc) * strength)
        src_priors[src] = [round(a, 4), round(b, 4)]

    updated = dict(policy)
    updated["calibration_curve"] = curve
    updated["source_priors"] = src_priors
    updated["recency_weights"] = {
        "2025": 1.00,
        "2024": 0.98,
        "2023": 0.95,
        "2022": 0.90,
        "2021": 0.84,
        "2020": 0.78,
        "2019": 0.72,
        "<2019": 0.20
    }
    policy_path.write_text(json.dumps(updated, ensure_ascii=False, indent=2), encoding="utf-8")

    rep = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "workbooks_scanned": len(files),
        "samples": len(samples),
        "weighted_samples_sum": round(sum(s.weight for s in samples), 4),
        "samples_by_source": sample_count_by_source,
        "weights_by_year_sum": {str(k): round(v, 4) for k, v in sorted(weight_by_year.items())},
        "brier_before": brier_before,
        "brier_after": brier_after,
        "calibration_curve": curve,
        "bins": bin_rows,
        "updated_policy": str(policy_path),
    }
    out_report = Path(args.out_report) if args.out_report else Path("outputs") / f"confidence_calibration_fit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_report.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(rep, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
