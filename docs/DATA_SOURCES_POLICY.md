# Data Sources Policy

## Primary Sources
- SEC EDGAR filings (10-K focused)
- SEC XBRL companyfacts/companyconcept endpoints

## Secondary Sources
- Market and macro layers used for market-dependent/hybrid ratios.

## Source Priority
1. Statement-anchored SEC values.
2. SEC companyfacts/companyconcept fallback.
3. Explicit proxy methods (tagged in output metadata).

## Source Integrity Rules
- Non-USD or scale-mismatched inputs are rejected for cross-item formulas.
- Missing critical inputs are surfaced as not computable unless approved fallback exists.
- Fallback usage must be auditable via source/reason tags.

