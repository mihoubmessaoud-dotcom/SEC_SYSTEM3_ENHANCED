#!/usr/bin/env python3
from __future__ import annotations

import argparse

from modules.direct_extraction_engine import DirectExtractionEngine


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate SEC_Official_Statement.csv from SEC consolidated statements only.")
    ap.add_argument("--cik", required=True, help="CIK (with or without leading zeros).")
    ap.add_argument("--accession", required=True, help="Accession number (with or without dashes).")
    ap.add_argument("--user-agent", required=True, help="SEC-compliant user agent, e.g. Name email@domain.com")
    ap.add_argument("--output", default="SEC_Official_Statement.csv", help="Output CSV file path.")
    ap.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds.")
    args = ap.parse_args()

    engine = DirectExtractionEngine(user_agent=args.user_agent)
    engine.extract(
        cik=args.cik,
        accession=args.accession,
        output_csv=args.output,
        timeout=args.timeout,
    )
    print("Output written successfully.")


if __name__ == "__main__":
    main()
