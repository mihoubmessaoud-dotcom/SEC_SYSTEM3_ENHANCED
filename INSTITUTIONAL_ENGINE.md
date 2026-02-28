# Institutional Intelligent Financial Mapping & Processing Engine

This package provides a modular, profile-aware financial normalization pipeline.

## Modules
- `classification.py`: company profile classification with profile probabilities.
- `ontology.py`: hierarchical financial ontology with parent-child nodes.
- `mapping.py`: intelligent XBRL-to-ontology mapping with confidence scoring.
- `computation.py`: dynamic parent-child computation, reconciliation, restatement tracking.
- `ratios.py`: sector-specific ratio engines with valid profile guards.
- `validation.py`: anomaly detection and quality/risk/confidence scoring.
- `strategic.py`: multi-year strategic intelligence analytics.
- `prediction.py`: forecast + scenario + optional Monte Carlo valuation.
- `learning.py`: unknown extension tag clustering and learning store.
- `engine.py`: orchestrates full pipeline and output generation.
- `api.py`: Python + JSON integration interface.

## Profiles
Supported primary profiles:
- industrial
- bank
- insurance
- reit
- investment_firm
- utility
- energy

## Programmatic API Example
```python
from modules.institutional import InstitutionalEngineAPI

api = InstitutionalEngineAPI()
payload = api.process_company(
    company_meta={"name": "Apple", "ticker": "AAPL", "cik": "0000320193", "filing_type": "10-K"},
    data_by_year={2024: {"Revenues": 100.0, "NetIncomeLoss": 20.0, "Assets": 200.0, "Liabilities": 120.0, "StockholdersEquity": 80.0, "NetCashProvidedByUsedInOperatingActivities": 30.0}},
    save_outputs=True,
)
```

## XBRL Parser Integration
Use `InstitutionalEngineAPI.process_from_xbrl_parser(...)` with parser output shape:
```json
{
  "company_meta": {"name": "...", "ticker": "..."},
  "facts": [{"year": 2024, "concept": "Revenues", "value": 1000.0}],
  "period_facts": [{"period": "2024-FY", "concept": "Revenues", "value": 1000.0}]
}
```

## Outputs
- Normalized financial statements
- Profile-aware ratio dashboard
- Ratio explanations
- Structural integrity annotations
- Mapping confidence levels
- Inconsistency flags and warnings
- Forecast/scenario report
- AI-ready clean dataset

## Testing
Run:
```powershell
python -m unittest tests\test_institutional_engine.py -v
```
