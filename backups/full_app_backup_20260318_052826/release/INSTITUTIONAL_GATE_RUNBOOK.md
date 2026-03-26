# Institutional Gate Runbook

## Objective
Run a broad, sector-diverse acceptance gate before release to avoid company-by-company manual fixes.

## Command (full universe)
```powershell
python tools/batch_investor_gate.py --start-year 2019 --end-year 2025 --filing-type 10-K --min-ratio-pct 70 --min-pass-pct 85
```

## Command (quick smoke)
```powershell
python tools/batch_investor_gate.py --tickers AAPL MSFT NVDA AMD JPM BAC UNH KO XOM CAT --start-year 2021 --end-year 2025 --min-pass-pct 70
```

## Outputs
- `outputs/institutional_batch_gate_*.json`
- `outputs/institutional_batch_gate_*.md`

## Large-Scale Campaign (Recommended Before Release)
```powershell
python tools/institutional_campaign_runner.py --target-count 300 --chunk-size 10 --start-year 2021 --end-year 2025 --filing-type 10-K --min-ratio-pct 60 --min-pass-pct 70 --require-no-na --min-filings-in-range 4 --min-core-concept-coverage 5 --prioritize-quality-tickers
```

## Gap Diagnostics
```powershell
python tools/phase2_gap_report.py --campaign-json outputs\institutional_campaign_<...>.json
```

## SLA Monitoring
```powershell
python tools/ops_sla_monitor.py --latest-n 20
```

## Enterprise Sell-Readiness Gate
```powershell
python tools/enterprise_readiness_gate.py --campaign-json outputs\institutional_campaign_<...>.json --sla-json outputs\ops_sla_report_<...>.json
```

## Formal pass criterion
- Process exit code `0`
- `summary.pass_pct >= min_pass_pct`

## Practical policy
- `PASS`: release-ready
- `REVIEW`: acceptable with documented caveats
- `FAIL`: do not release without fix
