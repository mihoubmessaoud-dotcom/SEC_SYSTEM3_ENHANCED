#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run institutional-grade SEC pipeline and output all required reports.
"""

import argparse
import json
from pathlib import Path

from modules.sec_fetcher import SECDataFetcher


def main() -> int:
    parser = argparse.ArgumentParser(description='Institutional SEC Intelligence Pipeline')
    parser.add_argument('--company', required=True, help='Ticker or company name (e.g., AAPL)')
    parser.add_argument('--start-year', type=int, required=True)
    parser.add_argument('--end-year', type=int, required=True)
    parser.add_argument('--filing-type', default='10-K')
    parser.add_argument('--output-dir', default='exports/institutional')
    args = parser.parse_args()

    fetcher = SECDataFetcher()
    result = fetcher.fetch_company_data(
        company_name=args.company,
        start_year=args.start_year,
        end_year=args.end_year,
        filing_type=args.filing_type,
    )

    if not result.get('success'):
        print(json.dumps({
            'success': False,
            'error': result.get('error', 'unknown'),
            'filing_diagnostics': result.get('filing_diagnostics')
        }, ensure_ascii=False, indent=2))
        return 1

    files = result.get('institutional_saved_files') or {}
    if files:
        print(json.dumps({
            'success': True,
            'generated_reports': files,
            'filing_diagnostics': result.get('filing_diagnostics')
        }, ensure_ascii=False, indent=2))
        return 0

    # fallback if institutional engine unavailable
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'fallback_notice.txt').write_text(
        'Institutional engine did not generate output. Check logs and dependencies.',
        encoding='utf-8',
    )
    print(json.dumps({'success': True, 'warning': 'institutional output unavailable'}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
