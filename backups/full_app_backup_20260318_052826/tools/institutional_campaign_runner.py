#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.sec_fetcher import SECDataFetcher

QUALITY_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMD", "GOOGL", "AMZN", "META", "ORCL", "ADBE", "CRM",
    "INTC", "QCOM", "AVGO", "TXN", "MU", "AMAT", "LRCX", "KLAC", "NXPI", "ADI",
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "USB", "PNC",
    "AIG", "ALL", "TRV", "MET", "PRU", "PGR", "CB", "AFL", "HIG", "L",
    "UNH", "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY", "CVS", "TMO", "DHR",
    "KO", "PEP", "MCD", "SBUX", "PG", "COST", "WMT", "HD", "LOW", "NKE",
    "CAT", "DE", "GE", "HON", "MMM", "UPS", "FDX", "LMT", "RTX", "BA",
    "XOM", "CVX", "COP", "EOG", "SLB", "OXY", "DUK", "SO", "NEE", "AEP",
    "T", "VZ", "CMCSA", "DIS", "NFLX", "SPG", "PLD", "O", "EQIX", "DLR",
]


def now_utc():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def unique_tickers_from_cache(fetcher):
    by_ticker = {}
    for k, v in (fetcher.companies_cache or {}).items():
        if not isinstance(v, dict):
            continue
        t = str(v.get('ticker') or '').upper().strip()
        cik = str(v.get('cik') or '').strip()
        name = str(v.get('name') or '').strip()
        if not t:
            continue
        if not cik:
            continue
        # Keep only standard ticker-like symbols.
        if not (1 <= len(t) <= 6):
            continue
        if not all(ch.isalnum() for ch in t):
            continue
        cur = by_ticker.get(t)
        if cur is None:
            by_ticker[t] = {"ticker": t, "cik": cik, "name": name}
        else:
            # Prefer richer name if available.
            if (not cur.get("name")) and name:
                cur["name"] = name
            if (not cur.get("cik")) and cik:
                cur["cik"] = cik
    out = sorted(by_ticker.keys())
    return [by_ticker[t] for t in out]


def is_probably_non_operating(record):
    t = str(record.get("ticker") or "").upper()
    n = str(record.get("name") or "").upper()
    if not t:
        return True
    # Common non-operating security suffixes.
    if len(t) <= 5 and t.endswith(("W", "R", "U")):
        return True
    bad_tokens = (
        "ACQUISITION CORP",
        "ACQUISITION CORPORATION",
        "BLANK CHECK",
        "SPAC",
        "ETF",
        "TRUST",
        "FUND",
        "PORTFOLIO",
        "WARRANT",
        "RIGHT",
        "NOTE",
    )
    return any(tok in n for tok in bad_tokens)


def _matches_form(form, requested):
    f = str(form or "").upper().replace("-", "")
    r = str(requested or "").upper().replace("-", "")
    if not f or not r:
        return False
    if r == "10K":
        return f.startswith("10K")
    return f.startswith(r)


def filing_count_in_range(fetcher, cik, start_year, end_year, filing_type):
    cik_padded = str(cik).zfill(10)
    url = f"{fetcher.base_url}/submissions/CIK{cik_padded}.json"
    try:
        r = requests.get(url, headers=fetcher.headers, timeout=20)
        r.raise_for_status()
        payload = r.json() or {}
    except Exception:
        return 0
    recent = (payload.get("filings") or {}).get("recent") or {}
    forms = recent.get("form", []) or []
    dates = recent.get("filingDate", []) or []
    n = min(len(forms), len(dates))
    count = 0
    for i in range(n):
        if not _matches_form(forms[i], filing_type):
            continue
        d = str(dates[i] or "")
        if len(d) < 4 or not d[:4].isdigit():
            continue
        y = int(d[:4])
        if start_year <= y <= end_year:
            count += 1
    return count


def core_concept_coverage_in_range(fetcher, cik, start_year, end_year):
    """
    Returns number of covered core accounting families in [start_year, end_year].
    Coverage is based on annual facts in SEC companyfacts.
    """
    cik_padded = str(cik).zfill(10)
    url = f"{fetcher.base_url}/api/xbrl/companyfacts/CIK{cik_padded}.json"
    try:
        r = requests.get(url, headers=fetcher.headers, timeout=20)
        r.raise_for_status()
        payload = r.json() or {}
    except Exception:
        return 0
    facts = (payload.get("facts") or {}).get("us-gaap") or {}
    families = {
        "revenue": [
            "Revenues",
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "NetSales",
        ],
        "net_income": ["NetIncomeLoss", "ProfitLoss"],
        "assets": ["Assets"],
        "equity": [
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        ],
        "current_assets": ["AssetsCurrent"],
        "current_liabilities": ["LiabilitiesCurrent"],
    }

    def _has_annual_fact(concept_name):
        cobj = facts.get(concept_name) or {}
        units = cobj.get("units") or {}
        for entries in units.values():
            for e in (entries or []):
                form = str(e.get("form") or "").upper().replace("-", "")
                if not form.startswith("10K"):
                    continue
                end = str(e.get("end") or "")
                if len(end) < 4 or not end[:4].isdigit():
                    continue
                y = int(end[:4])
                if start_year <= y <= end_year:
                    return True
        return False

    covered = 0
    for _, concept_list in families.items():
        if any(_has_annual_fact(cn) for cn in concept_list):
            covered += 1
    return covered


def _load_prefilter_cache(cache_path: Path):
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_prefilter_cache(cache_path: Path, cache_map: dict):
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(cache_map, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass


def select_campaign_universe(
    fetcher,
    target_count,
    start_year,
    end_year,
    filing_type,
    require_operating=True,
    prefilter_10k=True,
    max_prefilter_probes=0,
    prefilter_workers=12,
    min_filings_in_range=3,
    min_core_concept_coverage=4,
    prioritize_quality_tickers=True,
):
    records = unique_tickers_from_cache(fetcher)
    by_ticker = {str(r.get("ticker") or "").upper(): r for r in records}
    prioritized_records = []
    if prioritize_quality_tickers:
        for t in QUALITY_TICKERS:
            rec = by_ticker.get(str(t).upper())
            if rec:
                prioritized_records.append(rec)

    candidates = []
    seen = set()
    for rec in prioritized_records + records:
        t = str(rec.get("ticker") or "").upper()
        if not t or t in seen:
            continue
        seen.add(t)
        if require_operating and is_probably_non_operating(rec):
            continue
        candidates.append(rec)
        if max_prefilter_probes and len(candidates) >= max_prefilter_probes:
            break

    cache_file = Path('outputs') / f'prefilter_cache_{filing_type}_{start_year}_{end_year}.json'
    cache_map = _load_prefilter_cache(cache_file)
    selected = []
    probed = len(candidates)

    def _check_record(rec):
        cik = str(rec.get('cik') or '').zfill(10)
        ck = f"{cik}:{filing_type}:{start_year}:{end_year}"
        ck_cov = f"{cik}:coverage:{start_year}:{end_year}"
        cached = cache_map.get(ck)
        cached_cov = cache_map.get(ck_cov)
        if isinstance(cached, bool):
            # Backward compatibility with old cache entries.
            base_ok = bool(cached)
            cov_ok = True
            if isinstance(cached_cov, int):
                cov_ok = int(cached_cov) >= int(min_core_concept_coverage or 0)
            return rec, bool(base_ok and cov_ok)
        if isinstance(cached, int):
            base_ok = int(cached) >= int(min_filings_in_range or 1)
            cov_ok = True
            if isinstance(cached_cov, int):
                cov_ok = int(cached_cov) >= int(min_core_concept_coverage or 0)
            return rec, bool(base_ok and cov_ok)
        cnt = filing_count_in_range(fetcher, rec.get("cik"), start_year, end_year, filing_type)
        cov = core_concept_coverage_in_range(fetcher, rec.get("cik"), start_year, end_year)
        cache_map[ck] = int(cnt)
        cache_map[ck_cov] = int(cov)
        base_ok = int(cnt) >= int(min_filings_in_range or 1)
        cov_ok = int(cov) >= int(min_core_concept_coverage or 0)
        return rec, bool(base_ok and cov_ok)

    if not prefilter_10k:
        selected = [str(r.get("ticker")) for r in candidates[:target_count]]
        return selected, len(records), probed

    max_workers = max(1, int(prefilter_workers or 1))
    ok_map = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_check_record, rec): rec for rec in candidates}
        for i, fut in enumerate(as_completed(futures), start=1):
            rec, ok = fut.result()
            t = str(rec.get("ticker") or "").upper()
            if t:
                ok_map[t] = bool(ok)
            if i % 200 == 0:
                cur_ok = sum(1 for v in ok_map.values() if v)
                print(f"[prefilter] candidate_ok={cur_ok} | checked={i}/{len(candidates)}")
    # Preserve candidate priority/order (quality list first), never completion-order.
    for rec in candidates:
        t = str(rec.get("ticker") or "").upper()
        if t and ok_map.get(t):
            selected.append(t)
            if len(selected) >= target_count:
                break
    _save_prefilter_cache(cache_file, cache_map)
    return selected[:target_count], len(records), probed


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


def parse_batch_paths(stdout):
    json_path = None
    md_path = None
    for line in (stdout or '').splitlines():
        s = line.strip()
        if s.startswith('outputs\\') and s.endswith('.json'):
            json_path = s
        if s.startswith('outputs\\') and s.endswith('.md'):
            md_path = s
    return json_path, md_path


def run_chunk(chunk_index, chunk_tickers, args):
    cmd = [
        sys.executable, 'tools/batch_investor_gate.py',
        '--tickers', *chunk_tickers,
        '--start-year', str(args.start_year),
        '--end-year', str(args.end_year),
        '--filing-type', str(args.filing_type),
        '--min-ratio-pct', str(args.min_ratio_pct),
        '--min-pass-pct', str(args.min_pass_pct),
        '--per-ticker-timeout-sec', str(args.per_ticker_timeout_sec),
        '--max-retries', str(args.max_retries),
    ]
    if bool(args.require_no_na):
        cmd.append('--require-no-na')
    else:
        cmd.append('--no-require-no-na')
    p = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    jpath, mpath = parse_batch_paths(p.stdout)
    rec = {
        'index': chunk_index,
        'tickers': chunk_tickers,
        'returncode': p.returncode,
        'json': jpath,
        'md': mpath,
    }
    j = safe_load_json(jpath) if jpath else None
    if j and isinstance(j, dict):
        rec['summary'] = j.get('summary')
        rec['results'] = j.get('results') or []
    else:
        rec['stderr_tail'] = (p.stderr or '')[-1500:]
        rec['stdout_tail'] = (p.stdout or '')[-1500:]
        rec['results'] = []
    return rec


def safe_load_json(path):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:
        return None


def aggregate(all_records):
    total = len(all_records)
    pass_n = sum(1 for r in all_records if r.get('status') == 'PASS')
    pw_n = sum(1 for r in all_records if r.get('status') == 'PASS_WITH_WARNING')
    review_n = sum(1 for r in all_records if r.get('status') == 'REVIEW')
    fail_n = sum(1 for r in all_records if r.get('status') == 'FAIL')
    eff = pass_n + pw_n
    def pct(x):
        return round((x / total) * 100.0, 2) if total else 0.0
    by_sector = {}
    for r in all_records:
        s = str(r.get('sector_profile') or 'unknown')
        d = by_sector.setdefault(s, {'count': 0, 'pass': 0, 'pass_with_warning': 0, 'review': 0, 'fail': 0})
        d['count'] += 1
        st = r.get('status')
        if st == 'PASS':
            d['pass'] += 1
        elif st == 'PASS_WITH_WARNING':
            d['pass_with_warning'] += 1
        elif st == 'REVIEW':
            d['review'] += 1
        elif st == 'FAIL':
            d['fail'] += 1
    for s, d in by_sector.items():
        c = d['count'] or 1
        d['effective_pass_pct'] = round(((d['pass'] + d['pass_with_warning']) / c) * 100.0, 2)
    return {
        'count': total,
        'pass': pass_n,
        'pass_with_warning': pw_n,
        'review': review_n,
        'fail': fail_n,
        'pass_pct': pct(pass_n),
        'effective_pass_pct': pct(eff),
        'review_pct': pct(review_n),
        'fail_pct': pct(fail_n),
        'by_sector': by_sector,
    }


def main():
    ap = argparse.ArgumentParser(description='Run 1000+ institutional gate campaign in chunks.')
    ap.add_argument('--target-count', type=int, default=1000)
    ap.add_argument('--chunk-size', type=int, default=10)
    ap.add_argument('--start-year', type=int, default=2021)
    ap.add_argument('--end-year', type=int, default=2025)
    ap.add_argument('--filing-type', default='10-K')
    ap.add_argument('--min-ratio-pct', type=float, default=60.0)
    ap.add_argument('--min-pass-pct', type=float, default=70.0)
    ap.add_argument('--per-ticker-timeout-sec', type=int, default=0)
    ap.add_argument('--max-retries', type=int, default=3)
    ap.add_argument('--max-chunks', type=int, default=0, help='0 means all chunks')
    ap.add_argument('--prefilter-10k', action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument('--require-operating', action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument('--max-prefilter-probes', type=int, default=0, help='0 means no probe cap')
    ap.add_argument('--prefilter-workers', type=int, default=12)
    ap.add_argument('--parallel-batches', type=int, default=1)
    ap.add_argument('--min-filings-in-range', type=int, default=3, help='Minimum filing count in range to accept a ticker into campaign universe.')
    ap.add_argument('--min-core-concept-coverage', type=int, default=4, help='Minimum covered core concept families from SEC companyfacts.')
    ap.add_argument('--prioritize-quality-tickers', action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument(
        '--require-no-na',
        action=argparse.BooleanOptionalAction,
        default=True,
        help='Fail rows when required core ratios are missing.',
    )
    args = ap.parse_args()

    out_dir = Path('outputs')
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_json = out_dir / f'institutional_campaign_{args.target_count}_{ts}.json'
    out_md = out_dir / f'institutional_campaign_{args.target_count}_{ts}.md'

    fetcher = SECDataFetcher()
    selected, universe_size, probed = select_campaign_universe(
        fetcher=fetcher,
        target_count=args.target_count,
        start_year=args.start_year,
        end_year=args.end_year,
        filing_type=args.filing_type,
        require_operating=bool(args.require_operating),
        prefilter_10k=bool(args.prefilter_10k),
        max_prefilter_probes=args.max_prefilter_probes,
        prefilter_workers=args.prefilter_workers,
        min_filings_in_range=args.min_filings_in_range,
        min_core_concept_coverage=args.min_core_concept_coverage,
        prioritize_quality_tickers=bool(args.prioritize_quality_tickers),
    )
    if not selected:
        raise SystemExit('No ticker universe available from SEC cache')

    campaign = {
        'generated_at_utc': now_utc(),
        'config': vars(args),
        'universe_size': universe_size,
        'probed_candidates': probed,
        'selected_count': len(selected),
        'chunks': [],
        'summary': {},
    }

    merged_results = []
    chunk_iter = list(chunks(selected, args.chunk_size))
    if args.max_chunks and args.max_chunks > 0:
        chunk_iter = chunk_iter[:args.max_chunks]

    max_parallel = max(1, int(args.parallel_batches or 1))
    if max_parallel == 1:
        for i, c in enumerate(chunk_iter, start=1):
            rec = run_chunk(i, c, args)
            merged_results.extend(rec.get('results') or [])
            rec.pop('results', None)
            campaign['chunks'].append(rec)
            campaign['summary'] = aggregate(merged_results)
            out_json.write_text(json.dumps(campaign, ensure_ascii=False, indent=2), encoding='utf-8')
    else:
        with ThreadPoolExecutor(max_workers=max_parallel) as ex:
            futs = {
                ex.submit(run_chunk, i, c, args): i
                for i, c in enumerate(chunk_iter, start=1)
            }
            for fut in as_completed(futs):
                rec = fut.result()
                merged_results.extend(rec.get('results') or [])
                rec.pop('results', None)
                campaign['chunks'].append(rec)
                campaign['chunks'].sort(key=lambda x: int(x.get('index') or 0))
                campaign['summary'] = aggregate(merged_results)
                out_json.write_text(json.dumps(campaign, ensure_ascii=False, indent=2), encoding='utf-8')

    s = campaign.get('summary') or {}
    lines = [
        '# Institutional 1000+ Campaign',
        '',
        f"Generated: {campaign.get('generated_at_utc')}",
        f"Selected tickers: {campaign.get('selected_count')}",
        f"Processed results: {s.get('count', 0)}",
        '',
        '## Summary',
        f"- PASS: {s.get('pass', 0)}",
        f"- PASS_WITH_WARNING: {s.get('pass_with_warning', 0)}",
        f"- REVIEW: {s.get('review', 0)}",
        f"- FAIL: {s.get('fail', 0)}",
        f"- Effective pass %: {s.get('effective_pass_pct', 0.0)}",
        '',
        '## By Sector',
    ]
    for sec, d in sorted((s.get('by_sector') or {}).items(), key=lambda kv: kv[0]):
        lines.append(f"- {sec}: count={d.get('count',0)}, effective_pass_pct={d.get('effective_pass_pct',0.0)}")
    out_md.write_text('\n'.join(lines), encoding='utf-8')

    print(str(out_json))
    print(str(out_md))
    print(json.dumps(campaign.get('summary') or {}, ensure_ascii=False))


if __name__ == '__main__':
    main()
