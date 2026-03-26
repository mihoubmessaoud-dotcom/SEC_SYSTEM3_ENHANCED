#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Institutional Batch Investor Gate
---------------------------------
Runs a broad, sector-diverse acceptance sweep and emits machine-readable
and human-readable reports suitable for release gating.
"""

import argparse
import io
import json
import math
import multiprocessing as mp
import sys
import time
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DEFAULT_TICKERS_100 = [
    # Mega / Tech
    "AAPL", "MSFT", "NVDA", "AMD", "GOOGL", "AMZN", "META", "ORCL", "ADBE", "CRM",
    # Semis / Hardware
    "INTC", "QCOM", "AVGO", "TXN", "MU", "AMAT", "LRCX", "KLAC", "NXPI", "ADI",
    # Financials / Banks
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "USB", "PNC",
    # Insurance
    "AIG", "ALL", "TRV", "MET", "PRU", "PGR", "CB", "AFL", "HIG", "L",
    # Healthcare
    "UNH", "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY", "CVS", "TMO", "DHR",
    # Consumer
    "KO", "PEP", "MCD", "SBUX", "PG", "COST", "WMT", "HD", "LOW", "NKE",
    # Industrials
    "CAT", "DE", "GE", "HON", "MMM", "UPS", "FDX", "LMT", "RTX", "BA",
    # Energy / Utilities
    "XOM", "CVX", "COP", "EOG", "SLB", "OXY", "DUK", "SO", "NEE", "AEP",
    # Telecom / Media / REIT / Misc
    "T", "VZ", "CMCSA", "DIS", "NFLX", "SPG", "PLD", "O", "EQIX", "DLR",
]


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _f(v):
    try:
        x = float(v)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return None


def _pick_year(result, preferred):
    fr = result.get("financial_ratios") or {}
    ys = sorted([y for y in fr.keys() if isinstance(y, int)])
    if not ys:
        return None
    # Choose the most complete year (core-ratio coverage), not blindly the latest.
    # This avoids selecting a partially-filled trailing year (e.g., unfinished 10-K cycle).
    scored = []
    for y in ys:
        try:
            h = _core_ratio_health(result, y) or {}
            pct = float(h.get("computed_pct") or 0.0)
        except Exception:
            pct = 0.0
        scored.append((pct, y))
    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    best_year = scored[0][1]
    return best_year


def _balance_identity(data_by_year, year):
    row = (data_by_year or {}).get(year) or {}
    assets = _f(row.get("Assets") or row.get("TotalAssets"))
    liab = _f(row.get("Liabilities") or row.get("TotalLiabilities"))
    eq = _f(
        row.get("StockholdersEquity")
        or row.get("TotalEquity")
        or row.get("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    )
    if assets is None or liab is None or eq is None:
        return {"status": "INCONCLUSIVE", "delta_pct": None}
    base = abs(assets) if abs(assets) > 1e-9 else 1.0
    delta = abs(assets - (liab + eq)) / base
    return {"status": "PASS" if delta <= 0.01 else "FAIL", "delta_pct": delta}


def _core_ratio_health(result, year):
    sector = str(((result.get("sector_gating") or {}).get("profile") or "unknown")).lower()
    fr = ((result.get("financial_ratios") or {}).get(year) or {})
    payload = ((result.get("core_ratio_results") or {}).get(year) or {})
    rr = payload.get("ratio_results") or {}

    core_keys_by_sector = {
        "technology": [
            "current_ratio", "gross_margin", "operating_margin", "roa", "roe",
            "eps_basic", "market_cap", "pb_ratio", "fcf_yield", "real_return",
        ],
        "industrial": [
            "current_ratio", "gross_margin", "operating_margin", "roa", "roe",
            "eps_basic", "market_cap", "pb_ratio", "fcf_yield", "real_return",
        ],
        "energy": [
            "current_ratio", "gross_margin", "operating_margin", "roa", "roe",
            "eps_basic", "market_cap", "pb_ratio", "fcf_yield", "real_return",
        ],
        "consumer": [
            "current_ratio", "gross_margin", "operating_margin", "roa", "roe",
            "eps_basic", "market_cap", "pb_ratio", "fcf_yield", "real_return",
        ],
        "healthcare": [
            "current_ratio", "gross_margin", "operating_margin", "roa", "roe",
            "eps_basic", "market_cap", "pb_ratio", "fcf_yield", "real_return",
        ],
        "bank": [
            "roe", "roa", "eps_basic", "market_cap", "pb_ratio",
            "pe_ratio", "dividend_yield", "net_interest_margin",
            "loan_to_deposit_ratio", "capital_ratio_proxy",
        ],
        "insurance": [
            "roe", "roa", "eps_basic", "market_cap", "pb_ratio",
            "pe_ratio", "dividend_yield", "capital_ratio_proxy",
            "real_return", "sharpe_proxy",
        ],
    }
    required = core_keys_by_sector.get(
        sector,
        [
            "current_ratio", "gross_margin", "operating_margin", "roa", "roe",
            "eps_basic", "market_cap", "pb_ratio", "fcf_yield", "real_return",
        ],
    )

    def _has_value(v):
        x = _f(v)
        if x is not None:
            return True
        if isinstance(v, str) and v.strip():
            u = v.strip().upper()
            if u not in {"N/A", "NA", "NONE", "NULL"}:
                return True
        return False

    total = len(required)
    computed = 0
    reasons = {}
    missing_keys = []
    provenance_total = 0
    provenance_ok = 0

    def _prov_complete(item):
        if not isinstance(item, dict):
            return False
        if str(item.get("status") or "").upper() != "COMPUTED":
            return False
        formula = str(item.get("formula_used") or "").strip()
        source = str(item.get("source") or "").strip()
        trace = item.get("data_source_trace") or item.get("decision_tree")
        concepts = item.get("input_concepts") or item.get("input_tags") or []
        values = item.get("raw_values_used") or item.get("inputs") or {}
        return bool(formula and (source or trace) and (concepts or values))

    for k in required:
        val = fr.get(k)
        if _has_value(val):
            computed += 1
            item = rr.get(k) if isinstance(rr, dict) else None
            if isinstance(item, dict) and str(item.get("status") or "").upper() == "COMPUTED":
                provenance_total += 1
                if _prov_complete(item):
                    provenance_ok += 1
            continue
        item = rr.get(k) if isinstance(rr, dict) else None
        if isinstance(item, dict):
            st = str(item.get("status") or "").upper()
            if st == "COMPUTED":
                computed += 1
                provenance_total += 1
                if _prov_complete(item):
                    provenance_ok += 1
                continue
            if st == "NOT_COMPUTABLE":
                r = str(item.get("reason") or "UNKNOWN")
                reasons[r] = reasons.get(r, 0) + 1
                missing_keys.append(k)
                continue
        reasons["MISSING_VALUE"] = reasons.get("MISSING_VALUE", 0) + 1
        missing_keys.append(k)
    return {
        "total": total,
        "computed": computed,
        "computed_pct": round((computed / total) * 100.0, 2) if total else 0.0,
        "provenance_total": provenance_total,
        "provenance_ok": provenance_ok,
        "provenance_pct": round((provenance_ok / provenance_total) * 100.0, 2) if provenance_total else 0.0,
        "top_reasons": sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:8],
        "required_keys": required,
        "missing_keys": missing_keys[:20],
    }


def _field_count_for_year(layer_payload, year):
    if not isinstance(layer_payload, dict):
        return 0
    periods = layer_payload.get("periods") or {}
    p = periods.get(str(year)) or periods.get(year) or {}
    if not isinstance(p, dict):
        return 0
    facts = p.get("facts")
    fields = p.get("fields")
    if isinstance(facts, dict):
        return len(facts)
    if isinstance(fields, dict):
        return len(fields)
    return 0


def _layer_health(result, year):
    payloads = result.get("source_layer_payloads") or {}

    sec = payloads.get("SEC") or {}
    market = payloads.get("MARKET") or {}
    macro = payloads.get("MACRO") or {}
    yahoo = payloads.get("YAHOO") or {}

    sec_n = _field_count_for_year(sec, year)
    mkt_n = _field_count_for_year(market, year)
    mac_n = _field_count_for_year(macro, year)
    yah_n = _field_count_for_year(yahoo, year)

    data_row_n = len(((result.get("data_by_year") or {}).get(year) or {}))
    sec_status_ok = str(sec.get("status") or "").upper() == "OK"
    sec_ok = sec_status_ok and (sec_n >= 8 or data_row_n >= 8)

    return {
        "SEC": {
            "status": sec.get("status"),
            "field_count": sec_n,
            "ok": sec_ok,
            "fallback_data_row_count": data_row_n,
        },
        "MARKET": {
            "status": market.get("status"),
            "field_count": mkt_n,
            "ok": str(market.get("status") or "").upper() == "OK" and mkt_n >= 3,
        },
        "MACRO": {
            "status": macro.get("status"),
            "field_count": mac_n,
            "ok": str(macro.get("status") or "").upper() == "OK" and mac_n >= 2,
        },
        "YAHOO": {
            "status": yahoo.get("status"),
            "field_count": yah_n,
            "ok": str(yahoo.get("status") or "").upper() == "OK" and yah_n >= 5,
        },
    }


def _sector_adjusted_min_ratio_pct(sector_profile, global_min_ratio_pct):
    s = str(sector_profile or "").strip().lower()
    overrides = {
        "bank": 25.0,
        "insurance": 30.0,
        "energy": 40.0,
        "industrial": 45.0,
        "consumer": 45.0,
        "healthcare": 45.0,
        "technology": float(global_min_ratio_pct),
    }
    return float(overrides.get(s, global_min_ratio_pct))


def _investor_gate_decision(row, min_ratio_pct, require_no_na=False):
    filing = str(row.get("filing_grade") or "")
    bal = str((row.get("balance_identity") or {}).get("status") or "")
    core_pct = float((row.get("core_ratio_health") or {}).get("computed_pct") or 0.0)
    missing_keys = list(((row.get("core_ratio_health") or {}).get("missing_keys") or []))
    prov_pct = float((row.get("core_ratio_health") or {}).get("provenance_pct") or 0.0)
    layer_h = row.get("layer_health") or {}
    sector = str(row.get("sector_profile") or "").strip().lower()
    sec_ok = bool((layer_h.get("SEC") or {}).get("ok"))
    market_ok = bool((layer_h.get("MARKET") or {}).get("ok")) or bool((layer_h.get("YAHOO") or {}).get("ok"))
    macro_ok = bool((layer_h.get("MACRO") or {}).get("ok"))
    effective_min = float(row.get("effective_min_ratio_pct") or min_ratio_pct)
    hard_error = row.get("error")
    warnings = []
    if hard_error:
        return "FAIL", ["HARD_ERROR"]
    if require_no_na and missing_keys:
        return "FAIL", ["NA_NOT_ALLOWED"]
    if filing not in {"IN_RANGE_ANNUAL", "IN_RANGE_MIXED"}:
        return "REVIEW", ["FILING_OUT_OF_RANGE"]
    if bal == "FAIL":
        return "FAIL", ["BALANCE_IDENTITY_FAIL"]
    # Banking statements can legitimately produce balance inconclusive in this gate
    # (anchor semantics differ) while all core mandatory metrics are fully computed.
    if bal == "INCONCLUSIVE" and sector == "bank" and core_pct >= effective_min and sec_ok:
        return "PASS", warnings
    if core_pct < effective_min:
        return "REVIEW", ["CORE_COVERAGE_BELOW_MIN"]
    # Multi-layer condition: accounting must pass, and at least one enrichment
    # layer should be healthy for institutional-grade decisioning.
    if not (market_ok or macro_ok):
        return "REVIEW", ["NO_ENRICHMENT_LAYER_OK"]

    # Strong pass
    if sec_ok and bal == "PASS":
        if prov_pct >= 70.0:
            return "PASS", warnings
        return "PASS_WITH_WARNING", ["LOW_PROVENANCE_COVERAGE"]

    # Acceptable institutional pass with explicit warnings:
    # - SEC payload partial but multi-layer + coverage is good.
    if not sec_ok:
        warnings.append("SEC_LAYER_PARTIAL")
    if bal == "INCONCLUSIVE":
        warnings.append("BALANCE_INCONCLUSIVE")
    if prov_pct < 70.0:
        warnings.append("LOW_PROVENANCE_COVERAGE")
    return "PASS_WITH_WARNING", warnings


def _fetch_worker(payload, q):
    """
    Isolated worker to avoid hanging the main process on slow network calls.
    """
    ticker = payload["ticker"]
    start_year = payload["start_year"]
    end_year = payload["end_year"]
    filing_type = payload["filing_type"]
    try:
        # Lazy import inside worker to reduce Windows spawn startup overhead.
        from modules.sec_fetcher import SECDataFetcher

        fetcher = SECDataFetcher()
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = fetcher.fetch_company_data(
                company_name=ticker,
                start_year=start_year,
                end_year=end_year,
                filing_type=filing_type,
                include_all_concepts=False,
            )
        q.put({"ok": True, "result": result})
    except Exception as e:
        q.put({"ok": False, "error": f"EXCEPTION: {e}"})


def _fetch_inline(fetcher, ticker, start_year, end_year, filing_type):
    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        return fetcher.fetch_company_data(
            company_name=ticker,
            start_year=start_year,
            end_year=end_year,
            filing_type=filing_type,
            include_all_concepts=False,
        )


def _is_transient_fetch_error(msg):
    m = str(msg or "").lower()
    transient_tokens = (
        "503",
        "service unavailable",
        "read timed out",
        "timeout",
        "temporarily unavailable",
        "connection reset",
        "connection aborted",
        "remote end closed connection",
        "max retries exceeded",
        "429",
    )
    return any(tok in m for tok in transient_tokens)


def run_batch(
    tickers,
    start_year,
    end_year,
    filing_type,
    min_ratio_pct,
    per_ticker_timeout_sec=120,
    max_retries=2,
    require_no_na=False,
):
    rows = []
    total = len(tickers)
    # Fast/stable mode: reuse one fetcher in-process when timeout <= 0.
    inline_mode = float(per_ticker_timeout_sec) <= 0
    fetcher = None
    if inline_mode:
        from modules.sec_fetcher import SECDataFetcher
        fetcher = SECDataFetcher()

    for idx, t in enumerate(tickers, start=1):
        t0 = time.time()
        print(f"[{idx}/{total}] {t} ...", flush=True)
        out = {
            "ticker": t,
            "status": "FAIL",
            "elapsed_sec": None,
        }
        retries = max(0, int(max_retries))
        attempts_total = retries + 1
        result = {}
        last_error = None
        last_filing_diag = None
        for attempt in range(1, attempts_total + 1):
            if inline_mode:
                try:
                    result = _fetch_inline(fetcher, t, start_year, end_year, filing_type) or {}
                    last_error = None
                except Exception as e:
                    result = {}
                    last_error = f"EXCEPTION: {e}"
            else:
                # Windows/OneDrive can deny named-pipe creation for multiprocessing queues.
                # In that case, fail over to inline fetch so the gate remains runnable.
                try:
                    ctx = mp.get_context("spawn")
                    q = ctx.Queue()
                    p = ctx.Process(
                        target=_fetch_worker,
                        args=(
                            {
                                "ticker": t,
                                "start_year": start_year,
                                "end_year": end_year,
                                "filing_type": filing_type,
                            },
                            q,
                        ),
                    )
                    p.start()
                    p.join(timeout=float(per_ticker_timeout_sec))
                    if p.is_alive():
                        p.terminate()
                        p.join(timeout=2)
                        result = {}
                        last_error = f"TIMEOUT>{per_ticker_timeout_sec}s"
                    else:
                        result_msg = None
                        try:
                            if not q.empty():
                                result_msg = q.get_nowait()
                        except Exception:
                            result_msg = None
                        if not result_msg:
                            result = {}
                            last_error = "WORKER_NO_RESULT"
                        elif not result_msg.get("ok"):
                            result = {}
                            last_error = result_msg.get("error") or "WORKER_EXCEPTION"
                        else:
                            result = result_msg.get("result") or {}
                            last_error = None
                except PermissionError as e:
                    try:
                        from modules.sec_fetcher import SECDataFetcher
                        if fetcher is None:
                            fetcher = SECDataFetcher()
                        result = _fetch_inline(fetcher, t, start_year, end_year, filing_type) or {}
                        last_error = None
                    except Exception as ie:
                        result = {}
                        last_error = f"INLINE_FALLBACK_EXCEPTION: {ie} (mp={e})"

            if result.get("success"):
                break

            if not last_error:
                last_error = result.get("error") or "UNKNOWN_FETCH_ERROR"
                last_filing_diag = result.get("filing_diagnostics")

            retryable = _is_transient_fetch_error(last_error)
            if attempt < attempts_total and retryable:
                sleep_s = min(12.0, 2.0 * attempt)
                print(f"[{idx}/{total}] {t} retry {attempt}/{retries} after transient error: {last_error}", flush=True)
                time.sleep(sleep_s)
                continue
            break

        if not result.get("success"):
            out["error"] = last_error or result.get("error") or "UNKNOWN_FETCH_ERROR"
            out["filing_diagnostics"] = result.get("filing_diagnostics") or last_filing_diag
            out["attempts"] = attempts_total if _is_transient_fetch_error(out["error"]) else 1
            out["elapsed_sec"] = round(time.time() - t0, 2)
            rows.append(out)
            print(f"[{idx}/{total}] {t} => FAIL (fetch)", flush=True)
            continue

        year_used = _pick_year(result, preferred=end_year)
        bal = _balance_identity(result.get("data_by_year") or {}, year_used)
        core = _core_ratio_health(result, year_used)
        filing_diag = result.get("filing_diagnostics") or {}
        sector = ((result.get("sector_gating") or {}).get("profile") or "unknown")
        layer_health = _layer_health(result, year_used)
        effective_min_ratio_pct = _sector_adjusted_min_ratio_pct(sector, min_ratio_pct)

        out.update(
            {
                "year_used": year_used,
                "sector_profile": sector,
                "filing_grade": filing_diag.get("filing_grade"),
                "balance_identity": bal,
                "core_ratio_health": core,
                "layer_health": layer_health,
                "effective_min_ratio_pct": effective_min_ratio_pct,
            }
        )
        status, warnings = _investor_gate_decision(
            out,
            min_ratio_pct=min_ratio_pct,
            require_no_na=bool(require_no_na),
        )
        out["status"] = status
        out["gate_warnings"] = warnings
        out["attempts"] = attempt
        out["elapsed_sec"] = round(time.time() - t0, 2)
        rows.append(out)
        print(f"[{idx}/{total}] {t} => {out['status']} ({out['elapsed_sec']}s)", flush=True)
    return rows


def summarize(rows):
    p = sum(1 for r in rows if r.get("status") == "PASS")
    pw = sum(1 for r in rows if r.get("status") == "PASS_WITH_WARNING")
    rv = sum(1 for r in rows if r.get("status") == "REVIEW")
    f = sum(1 for r in rows if r.get("status") == "FAIL")
    total = len(rows)
    strict_pass_pct = round((p / max(1, total)) * 100.0, 2)
    effective_pass_pct = round(((p + pw) / max(1, total)) * 100.0, 2)
    return {
        "count": total,
        "pass": p,
        "pass_with_warning": pw,
        "review": rv,
        "fail": f,
        "pass_pct": strict_pass_pct,
        "effective_pass_pct": effective_pass_pct,
        "review_pct": round((rv / max(1, total)) * 100.0, 2),
        "fail_pct": round((f / max(1, total)) * 100.0, 2),
    }


def write_reports(rows, out_json, out_md, config):
    payload = {
        "generated_at_utc": _now_utc(),
        "config": config,
        "summary": summarize(rows),
        "results": rows,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Institutional Batch Investor Gate")
    lines.append("")
    lines.append(f"- Generated: {payload['generated_at_utc']}")
    lines.append(f"- Universe: {len(config.get('tickers', []))} tickers")
    lines.append(
        f"- PASS/PASS_WITH_WARNING/REVIEW/FAIL: "
        f"{payload['summary']['pass']}/{payload['summary']['pass_with_warning']}/"
        f"{payload['summary']['review']}/{payload['summary']['fail']}"
    )
    lines.append(f"- Strict PASS%: {payload['summary']['pass_pct']}%")
    lines.append(f"- Effective PASS%: {payload['summary']['effective_pass_pct']}%")
    lines.append("")
    lines.append("## Per-ticker")
    for r in rows:
        if r.get("status") == "FAIL" and r.get("error"):
            lines.append(f"- {r['ticker']}: FAIL | {r['error']}")
            continue
        core = r.get("core_ratio_health") or {}
        bal = r.get("balance_identity") or {}
        layer_h = r.get("layer_health") or {}
        sec_h = layer_h.get("SEC") or {}
        mkt_h = layer_h.get("MARKET") or {}
        mac_h = layer_h.get("MACRO") or {}
        yah_h = layer_h.get("YAHOO") or {}
        lines.append(
            f"- {r.get('ticker')}: {r.get('status')} | year={r.get('year_used')} | "
            f"sector={r.get('sector_profile')} | filing={r.get('filing_grade')} | "
            f"balance={bal.get('status')} | core={core.get('computed')}/{core.get('total')} "
            f"({core.get('computed_pct')}%, min={r.get('effective_min_ratio_pct')}) | "
            f"SEC={sec_h.get('ok')}[{sec_h.get('field_count')}] "
            f"MARKET={mkt_h.get('ok')}[{mkt_h.get('field_count')}] "
            f"MACRO={mac_h.get('ok')}[{mac_h.get('field_count')}] "
            f"YAHOO={yah_h.get('ok')}[{yah_h.get('field_count')}]"
        )
        if r.get("gate_warnings"):
            lines.append(f"  warnings: {', '.join(r.get('gate_warnings') or [])}")
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args():
    ap = argparse.ArgumentParser(description="Run institutional batch investor gate.")
    ap.add_argument("--tickers", nargs="*", default=DEFAULT_TICKERS_100, help="Ticker universe.")
    ap.add_argument("--start-year", type=int, default=2019)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--filing-type", type=str, default="10-K")
    ap.add_argument("--min-ratio-pct", type=float, default=70.0, help="Minimum core computed ratio percentage.")
    ap.add_argument("--min-pass-pct", type=float, default=85.0, help="Minimum PASS percentage for gate.")
    ap.add_argument(
        "--gate-pass-mode",
        type=str,
        default="effective",
        choices=["strict", "effective"],
        help="strict=PASS only, effective=PASS + PASS_WITH_WARNING.",
    )
    ap.add_argument(
        "--per-ticker-timeout-sec",
        type=float,
        default=240.0,
        help="Timeout per ticker to avoid hangs. Set 0 to run inline without process isolation/timeouts.",
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Retry count per ticker for transient SEC/network errors (503/timeouts).",
    )
    ap.add_argument(
        "--require-no-na",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail ticker if any required core ratio is N/A/NOT_COMPUTABLE.",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    out_dir = Path("outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"institutional_batch_gate_{ts}.json"
    out_md = out_dir / f"institutional_batch_gate_{ts}.md"

    rows = run_batch(
        tickers=args.tickers,
        start_year=args.start_year,
        end_year=args.end_year,
        filing_type=args.filing_type,
        min_ratio_pct=args.min_ratio_pct,
        per_ticker_timeout_sec=args.per_ticker_timeout_sec,
        max_retries=args.max_retries,
        require_no_na=args.require_no_na,
    )
    cfg = {
        "tickers": args.tickers,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "filing_type": args.filing_type,
        "min_ratio_pct": args.min_ratio_pct,
        "min_pass_pct": args.min_pass_pct,
        "per_ticker_timeout_sec": args.per_ticker_timeout_sec,
        "max_retries": args.max_retries,
        "require_no_na": args.require_no_na,
        "gate_pass_mode": args.gate_pass_mode,
    }
    write_reports(rows, out_json, out_md, cfg)

    summ = summarize(rows)
    print(str(out_json))
    print(str(out_md))
    print(json.dumps(summ, ensure_ascii=False))

    # Formal gate: non-zero exit if pass threshold not met.
    pass_metric = "pass_pct" if args.gate_pass_mode == "strict" else "effective_pass_pct"
    if float(summ.get(pass_metric) or 0.0) < float(args.min_pass_pct):
        sys.exit(2)


if __name__ == "__main__":
    main()
