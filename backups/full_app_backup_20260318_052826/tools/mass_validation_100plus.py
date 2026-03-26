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


TICKERS_120 = [
    # Tech / semis
    "AAPL", "MSFT", "NVDA", "AMD", "INTC", "QCOM", "AVGO", "TXN", "MU", "ADBE",
    "CRM", "ORCL", "CSCO", "IBM", "AMAT", "LRCX", "KLAC", "SNPS", "CDNS", "ANET",
    # Consumer / internet
    "AMZN", "META", "GOOGL", "NFLX", "DIS", "BKNG", "UBER", "ABNB", "PYPL", "EBAY",
    # Auto / industrial growth
    "TSLA", "GM", "F", "RIVN", "NIO", "LI", "XPEV", "CAT", "DE", "GE",
    # Financials (banks + card + brokers)
    "JPM", "BAC", "WFC", "C", "GS", "MS", "USB", "PNC", "TFC", "SCHW",
    "AXP", "COF", "BK", "STT", "BLK",
    # Insurance
    "UNH", "CVS", "CI", "ELV", "HUM", "AIG", "TRV", "PGR", "ALL", "MET",
    # Healthcare / pharma
    "JNJ", "PFE", "MRK", "LLY", "ABBV", "BMY", "AMGN", "GILD", "BIIB", "REGN",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "HAL",
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "SRE", "XEL", "PEG", "ED",
    # Telecom / media
    "T", "VZ", "TMUS", "CMCSA", "CHTR", "WBD", "PARA", "FOXA", "NWSA", "ROKU",
    # Staples / retail / food
    "WMT", "COST", "TGT", "HD", "LOW", "KO", "PEP", "MCD", "SBUX", "NKE",
    # REIT / transport / misc
    "PLD", "AMT", "SPG", "CCI", "EQIX", "CSX", "UNP", "NSC", "DAL", "UAL",
]

# Deep sample: diversified and stricter computational run
DEEP_SAMPLE = [
    "AAPL", "MSFT", "NVDA", "AMD", "INTC", "TSLA", "RIVN",
    "JPM", "BAC", "GS", "AIG", "UNH", "CVS",
    "XOM", "NEE", "KO", "WMT", "PLD",
]


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(v):
    try:
        f = float(v)
        if math.isfinite(f):
            return f
    except Exception:
        return None
    return None


def _extract_year_payload(result, preferred_year=2024):
    fr = result.get("financial_ratios") or {}
    if preferred_year in fr:
        return preferred_year, fr.get(preferred_year) or {}
    years = sorted([y for y in fr.keys() if isinstance(y, int)])
    if years:
        y = years[-1]
        return y, fr.get(y) or {}
    return None, {}


def _count_ratio_status(core_ratio_results, year):
    payload = (core_ratio_results or {}).get(year) or {}
    rr = payload.get("ratio_results") or {}
    total = 0
    computed = 0
    not_computable = 0
    reasons = {}
    for _rid, item in rr.items():
        if not isinstance(item, dict):
            continue
        total += 1
        st = str(item.get("status") or "").upper()
        if st == "COMPUTED":
            computed += 1
        elif st == "NOT_COMPUTABLE":
            not_computable += 1
            reason = str(item.get("reason") or "UNKNOWN")
            reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "total": total,
        "computed": computed,
        "not_computable": not_computable,
        "top_reasons": sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:8],
    }


def _balance_check(data_by_year, year):
    row = (data_by_year or {}).get(year) or {}
    assets = _safe_float(row.get("Assets") or row.get("TotalAssets"))
    liabilities = _safe_float(row.get("Liabilities") or row.get("TotalLiabilities"))
    equity = _safe_float(
        row.get("StockholdersEquity")
        or row.get("TotalEquity")
        or row.get("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    )
    if assets is None or liabilities is None or equity is None:
        return {"status": "INCONCLUSIVE", "delta_abs": None, "delta_pct": None}
    base = abs(assets) if abs(assets) > 1e-9 else 1.0
    delta = assets - (liabilities + equity)
    delta_pct = abs(delta) / base
    status = "PASS" if delta_pct <= 0.01 else "FAIL"
    return {"status": status, "delta_abs": delta, "delta_pct": delta_pct}


def run():
    out_dir = Path("outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"mass_validation_100plus_{ts}.json"
    md_path = out_dir / f"mass_validation_100plus_{ts}.md"

    fetcher = SECDataFetcher()

    report = {
        "generated_at_utc": _now_utc(),
        "phase_light": {"count_requested": len(TICKERS_120), "records": []},
        "phase_deep": {"count_requested": len(DEEP_SAMPLE), "records": []},
        "summary": {},
    }

    # Phase A: lightweight compatibility check for >100 institutions
    for i, t in enumerate(TICKERS_120, start=1):
        t0 = time.time()
        ok = True
        err = None
        info = {}
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                info = fetcher.get_company_info(t)
            if not info:
                ok = False
                err = "NOT_FOUND_IN_SEC_COMPANY_LIST"
        except Exception as e:
            ok = False
            err = f"LIGHT_PHASE_ERROR: {e}"
        rec = {
            "ticker": t,
            "ok": ok,
            "elapsed_sec": round(time.time() - t0, 3),
            "cik": info.get("cik") if isinstance(info, dict) else None,
            "name": info.get("title") if isinstance(info, dict) else None,
            "sic_desc": info.get("sicDescription") if isinstance(info, dict) else None,
            "error": err,
        }
        report["phase_light"]["records"].append(rec)
        if i % 20 == 0:
            json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Phase B: deep computational validation on diversified sample
    for i, t in enumerate(DEEP_SAMPLE, start=1):
        t0 = time.time()
        ok = True
        err = None
        rec = {"ticker": t}
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                result = fetcher.fetch_company_data(
                    company_name=t,
                    start_year=2024,
                    end_year=2024,
                    filing_type="10-K",
                    include_all_concepts=False,
                )
            ok = bool(result.get("success"))
            if not ok:
                err = result.get("error") or "UNKNOWN_FETCH_ERROR"
                rec.update(
                    {
                        "ok": False,
                        "error": err,
                        "elapsed_sec": round(time.time() - t0, 3),
                        "filing_diagnostics": result.get("filing_diagnostics"),
                    }
                )
            else:
                year_used, ratios_row = _extract_year_payload(result, preferred_year=2024)
                core_ratios = result.get("core_ratio_results") or {}
                status_count = _count_ratio_status(core_ratios, year_used)
                sector_profile = ((result.get("sector_gating") or {}).get("profile") or "unknown")
                filing_diag = result.get("filing_diagnostics") or {}
                bal = _balance_check(result.get("data_by_year") or {}, year_used)

                non_null_ratio_values = 0
                numeric_ratio_values = 0
                for _k, v in (ratios_row or {}).items():
                    if v is None:
                        continue
                    non_null_ratio_values += 1
                    if isinstance(v, (int, float)):
                        numeric_ratio_values += 1

                rec.update(
                    {
                        "ok": True,
                        "elapsed_sec": round(time.time() - t0, 3),
                        "year_used": year_used,
                        "sector_profile": sector_profile,
                        "filing_grade": filing_diag.get("filing_grade"),
                        "selected_filings_count": len(result.get("selected_filings") or []),
                        "ratio_values_non_null": non_null_ratio_values,
                        "ratio_values_numeric": numeric_ratio_values,
                        "core_ratio_status": status_count,
                        "balance_identity": bal,
                        "not_computable_top_reasons": status_count.get("top_reasons") or [],
                    }
                )
        except Exception as e:
            ok = False
            err = f"DEEP_PHASE_EXCEPTION: {e}"
            rec.update(
                {
                    "ok": False,
                    "error": err,
                    "elapsed_sec": round(time.time() - t0, 3),
                }
            )

        report["phase_deep"]["records"].append(rec)
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Aggregate summary
    light = report["phase_light"]["records"]
    deep = report["phase_deep"]["records"]

    light_ok = sum(1 for r in light if r.get("ok"))
    deep_ok = sum(1 for r in deep if r.get("ok"))
    deep_bal_pass = sum(1 for r in deep if (r.get("balance_identity") or {}).get("status") == "PASS")
    deep_bal_fail = sum(1 for r in deep if (r.get("balance_identity") or {}).get("status") == "FAIL")

    reason_counter = {}
    for r in deep:
        for reason, c in (r.get("not_computable_top_reasons") or []):
            reason_counter[reason] = reason_counter.get(reason, 0) + int(c)

    profile_counter = {}
    for r in deep:
        p = r.get("sector_profile") or "unknown"
        profile_counter[p] = profile_counter.get(p, 0) + 1

    report["summary"] = {
        "light_coverage_requested": len(light),
        "light_coverage_success": light_ok,
        "light_coverage_success_pct": round((light_ok / max(1, len(light))) * 100.0, 2),
        "deep_coverage_requested": len(deep),
        "deep_coverage_success": deep_ok,
        "deep_coverage_success_pct": round((deep_ok / max(1, len(deep))) * 100.0, 2),
        "deep_balance_pass": deep_bal_pass,
        "deep_balance_fail": deep_bal_fail,
        "deep_sector_profiles": profile_counter,
        "top_not_computable_reasons": sorted(reason_counter.items(), key=lambda kv: kv[1], reverse=True)[:12],
    }

    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Mass Validation 100+ Report")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at_utc']}")
    lines.append(f"- Light coverage: {light_ok}/{len(light)} ({report['summary']['light_coverage_success_pct']}%)")
    lines.append(f"- Deep coverage: {deep_ok}/{len(deep)} ({report['summary']['deep_coverage_success_pct']}%)")
    lines.append(f"- Deep balance PASS/FAIL: {deep_bal_pass}/{deep_bal_fail}")
    lines.append("")
    lines.append("## Deep Phase - Top Not Computable Reasons")
    for reason, c in report["summary"]["top_not_computable_reasons"]:
        lines.append(f"- {reason}: {c}")
    lines.append("")
    lines.append("## Deep Phase - Per Ticker")
    for r in deep:
        if not r.get("ok"):
            lines.append(f"- {r.get('ticker')}: FAIL - {r.get('error')}")
            continue
        cs = r.get("core_ratio_status") or {}
        bal = r.get("balance_identity") or {}
        lines.append(
            f"- {r.get('ticker')}: OK | year={r.get('year_used')} | profile={r.get('sector_profile')} | "
            f"filing={r.get('filing_grade')} | ratios computed={cs.get('computed')}/{cs.get('total')} | "
            f"balance={bal.get('status')}"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(json_path))
    print(str(md_path))


if __name__ == "__main__":
    run()
