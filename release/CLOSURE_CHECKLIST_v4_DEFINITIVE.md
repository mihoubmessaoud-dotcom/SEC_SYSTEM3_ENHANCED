# Official Closure Checklist - v4-definitive

- [x] Release notes issued: `release/RELEASE_NOTES_v4_DEFINITIVE_20260228.md`
- [x] Version stamp locked: `config/release_stamp.json`
- [x] Non-regression baseline test added: `tests/test_release_v4_baseline.py`
- [x] Baseline artifacts referenced:
  - `outputs/quick_eval_20260227_latest.json`
  - `outputs/final_multi_company_validation.json`
  - `outputs/final_integrity_validation_20260226.json`

## Re-run command
```powershell
python -m unittest tests.test_release_v4_baseline -v
```

## Commercial Readiness Commands
```powershell
python tools\mass_validation_100plus.py
python tools\commercial_readiness_gate.py --report outputs\mass_validation_100plus_<timestamp>.json
```

## Closure rule
Any post-closure behavior change must:
1. Create a new version stamp,
2. Update release notes,
3. Refresh baseline artifacts and tests.
