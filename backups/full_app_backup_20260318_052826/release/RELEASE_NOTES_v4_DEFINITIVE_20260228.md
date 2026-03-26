# SEC Financial Intelligence Platform - Release Notes

## Release
- Version: `v4-definitive`
- Date (UTC): `2026-02-28`
- Status: `Final Technical Closure`

## Scope of Closure
- Locked final fixes for:
  - comparison fallback integrity (sector/confidence/filing-grade fallback chain)
  - split handling cutoff behavior for historical market calculations
  - market-cap reconstruction quality gate with safer share anchors
  - EV/EBITDA stabilization and unit-safe candidate selection
  - conservative debt fallback in missing SEC debt edge cases

## Acceptance Criteria (Final)
- `Final_Acceptance` verdict: approved for expert review in targeted outputs.
- `Balance_Check`: no FAIL rows in validated files.
- `Ratio_Audit`: no NOT_COMPUTED rows in validated files.
- comparison sheet no `unknown` sector / no NaN confidence in validated files.

## Validation Artifacts
- `outputs/quick_eval_20260227_latest.json`
- `outputs/final_multi_company_validation.json`
- `outputs/final_integrity_validation_20260226.json`

## Known Boundaries (Documented, Not Blocking)
- Missing 10-K years remain explicitly marked as unavailable and are not backfilled from outside SEC annual scope.
- If source filing omits a concept, status stays explainable/non-computable by design.

## Operational Notes
- Manual export path remains: `exports/manual_exports/`
- UI export action confirms saved path in completion message.

## Final Governance
- This release is closed under `v4-definitive`.
- Any future change must ship as a new stamped version with updated non-regression baseline.
