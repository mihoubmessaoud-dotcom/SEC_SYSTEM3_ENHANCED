#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import json
import math
import sys
import time
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from modules.sec_fetcher import SECDataFetcher


TICKERS_NEW_10 = [
    "META",   # internet/tech
    "ORCL",   # enterprise software
    "PFE",    # pharma
    "CVX",    # energy
    "DUK",    # utilities
    "AXP",    # financial services
    "BKNG",   # travel services
    "CAT",    # industrials
    "TRV",    # insurance
    "MCD",    # consumer
]


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(v):
    try:
        f = float(v)
        if math.isfinite(f):
            return f
    except Exception:
        pass
    return None


def _extract_year(result, preferred_year=2024):
    fr = result.get("financial_ratios") or {}
    if preferred_year in fr:
        return preferred_year
    years = sorted([y for y in fr.keys() if isinstance(y, int)])
    return years[-1] if years else None


def _balance_identity(data_by_year, year):
    row = (data_by_year or {}).get(year) or {}
    assets = _safe_float(row.get("Assets") or row.get("TotalAssets"))
    liabilities = _safe_float(row.get("Liabilities") or row.get("TotalLiabilities"))
    equity = _safe_float(
        row.get("StockholdersEquity")
        or row.get("TotalEquity")
        or row.get("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    )
    if assets is None or liabilities is None or equity is None:
        return {"status": "INCONCLUSIVE", "delta_pct": None}
    base = abs(assets) if abs(assets) > 1e-9 else 1.0
    delta_pct = abs(assets - (liabilities + equity)) / base
    return {"status": "PASS" if delta_pct <= 0.01 else "FAIL", "delta_pct": delta_pct}


def _core_status(result, year):
    payload = ((result.get("core_ratio_results") or {}).get(year) or {})
    rr = payload.get("ratio_results") or {}
    total = 0
    computed = 0
    reasons = {}
    for _, item in rr.items():
        if not isinstance(item, dict):
            continue
        total += 1
        st = str(item.get("status") or "").upper()
        if st == "COMPUTED":
            computed += 1
        elif st == "NOT_COMPUTABLE":
            reason = str(item.get("reason") or "UNKNOWN")
            reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "total": total,
        "computed": computed,
        "computed_pct": round((computed / total) * 100.0, 2) if total else 0.0,
        "top_not_computable_reasons": sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:6],
    }


def main():
    out_dir = Path("outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"acceptance_new10_{ts}.json"
    out_md = out_dir / f"acceptance_new10_{ts}.md"

    fetcher = SECDataFetcher()
    rows = []

    for t in TICKERS_NEW_10:
        t0 = time.time()
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                r = fetcher.fetch_company_data(
                    company_name=t,
                    start_year=2024,
                    end_year=2024,
                    filing_type="10-K",
                    include_all_concepts=False,
                )
        except Exception as e:
            rows.append(
                {
                    "ticker": t,
                    "status": "FAIL",
                    "error": f"EXCEPTION: {e}",
                    "elapsed_sec": round(time.time() - t0, 2),
                }
            )
            continue

        if not r.get("success"):
            rows.append(
                {
                    "ticker": t,
                    "status": "FAIL",
                    "error": r.get("error") or "UNKNOWN_FETCH_ERROR",
                    "filing_diagnostics": r.get("filing_diagnostics"),
                    "elapsed_sec": round(time.time() - t0, 2),
                }
            )
            continue

        year_used = _extract_year(r, preferred_year=2024)
        bal = _balance_identity(r.get("data_by_year") or {}, year_used)
        core = _core_status(r, year_used)
        filing_diag = r.get("filing_diagnostics") or {}
        sector = ((r.get("sector_gating") or {}).get("profile") or "unknown")

        # Acceptance rule for this gate
        pass_gate = (
            year_used is not None
            and filing_diag.get("filing_grade") in {"IN_RANGE_ANNUAL", "IN_RANGE_MIXED"}
            and bal.get("status") != "FAIL"
            and core.get("computed", 0) >= 8
        )

        rows.append(
            {
                "ticker": t,
                "status": "PASS" if pass_gate else "REVIEW",
                "year_used": year_used,
                "sector_profile": sector,
                "filing_grade": filing_diag.get("filing_grade"),
                "balance_identity": bal,
                "core_ratio_status": core,
                "elapsed_sec": round(time.time() - t0, 2),
            }
        )

        out_json.write_text(
            json.dumps(
                {
                    "generated_at_utc": _now_utc(),
                    "tickers": TICKERS_NEW_10,
                    "results": rows,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    pass_count = sum(1 for x in rows if x.get("status") == "PASS")
    fail_count = sum(1 for x in rows if x.get("status") == "FAIL")
    review_count = sum(1 for x in rows if x.get("status") == "REVIEW")

    payload = {
        "generated_at_utc": _now_utc(),
        "tickers": TICKERS_NEW_10,
        "summary": {
            "count": len(rows),
            "pass": pass_count,
            "review": review_count,
            "fail": fail_count,
            "pass_pct": round((pass_count / max(1, len(rows))) * 100.0, 2),
        },
        "results": rows,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Acceptance Test - 10 New Companies")
    lines.append("")
    lines.append(f"- Generated: {payload['generated_at_utc']}")
    lines.append(f"- PASS/REVIEW/FAIL: {pass_count}/{review_count}/{fail_count}")
    lines.append(f"- PASS%: {payload['summary']['pass_pct']}%")
    lines.append("")
    lines.append("## Results")
    for r in rows:
        if r.get("status") == "FAIL":
            lines.append(f"- {r['ticker']}: FAIL | error={r.get('error')}")
            continue
        c = r.get("core_ratio_status") or {}
        b = r.get("balance_identity") or {}
        lines.append(
            f"- {r['ticker']}: {r['status']} | year={r.get('year_used')} | profile={r.get('sector_profile')} | "
            f"filing={r.get('filing_grade')} | balance={b.get('status')} | computed={c.get('computed')}/{c.get('total')} ({c.get('computed_pct')}%)"
        )
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(str(out_json))
    print(str(out_md))


if __name__ == "__main__":
    main()

