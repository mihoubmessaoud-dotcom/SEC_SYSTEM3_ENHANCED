from pathlib import Path
from pprint import pprint
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.financial_analysis_system import FinancialAnalysisSystem


def _sample_data() -> dict:
    return {
        "NVDA": {
            "gross_margin": {2023: 0.66, 2024: 0.72, 2025: 0.75},
            "capex_to_revenue": {2023: 0.05, 2024: 0.04, 2025: 0.03},
            "rd_to_revenue": {2023: 0.16, 2024: 0.15, 2025: 0.14},
            "leverage": {2023: 0.55, 2024: 0.43, 2025: 0.35},
            "roic": {2023: 0.32, 2024: 0.58, 2025: 0.69},
            "operating_margin": {2023: 0.28, 2024: 0.48, 2025: 0.55},
            "revenue_growth": {2023: 0.25, 2024: 0.55, 2025: 0.40},
            "net_income_growth": {2023: 0.30, 2024: 0.70, 2025: 0.45},
            "asset_turnover": {2023: 0.72, 2024: 0.85, 2025: 0.92},
            "interest_coverage": {2023: 22.0, 2024: 28.0, 2025: 34.0},
            "altman_z": {2023: 5.2, 2024: 6.1, 2025: 7.0},
        },
        "AMD": {
            "gross_margin": {2023: 0.49, 2024: 0.51, 2025: 0.52},
            "capex_to_revenue": {2023: 0.08, 2024: 0.07, 2025: 0.06},
            "rd_to_revenue": {2023: 0.23, 2024: 0.24, 2025: 0.24},
            "leverage": {2023: 1.35, 2024: 1.28, 2025: 1.20},
            "roic": {2023: 0.09, 2024: 0.13, 2025: 0.16},
            "operating_margin": {2023: 0.12, 2024: 0.17, 2025: 0.20},
            "revenue_growth": {2023: 0.08, 2024: 0.12, 2025: 0.14},
            "net_income_growth": {2023: 0.06, 2024: 0.11, 2025: 0.13},
            "asset_turnover": {2023: 0.65, 2024: 0.68, 2025: 0.71},
            "interest_coverage": {2023: 7.8, 2024: 8.5, 2025: 9.2},
            "altman_z": {2023: 3.0, 2024: 3.3, 2025: 3.6},
        },
        "INTC": {
            "gross_margin": {2023: 0.40, 2024: 0.37, 2025: 0.35},
            "capex_to_revenue": {2023: 0.22, 2024: 0.26, 2025: 0.28},
            "rd_to_revenue": {2023: 0.13, 2024: 0.14, 2025: 0.14},
            "leverage": {2023: 1.85, 2024: 1.98, 2025: 2.10},
            "roic": {2023: 0.02, 2024: 0.00, 2025: -0.01},
            "operating_margin": {2023: 0.10, 2024: 0.06, 2025: 0.03},
            "revenue_growth": {2023: -0.04, 2024: -0.01, 2025: 0.01},
            "net_income_growth": {2023: -0.09, 2024: -0.03, 2025: 0.00},
            "asset_turnover": {2023: 0.48, 2024: 0.46, 2025: 0.44},
            "interest_coverage": {2023: 5.0, 2024: 4.4, 2025: 3.8},
            "altman_z": {2023: 2.4, 2024: 2.2, 2025: 2.0},
        },
    }


def main() -> None:
    system = FinancialAnalysisSystem()
    all_data = _sample_data()

    for ticker in ("NVDA", "AMD", "INTC"):
        print(f"\n=== {ticker} ===")
        result = system.analyze(ticker=ticker, raw_metrics_by_year=all_data[ticker])
        pprint(
            {
                "ticker": result["ticker"],
                "model": result["model"]["model"],
                "model_confidence": result["model"]["confidence"],
                "ratios_2025": result["ratios"].get(2025, {}),
                "score": result["score"]["score"],
                "signature": result["signature"]["overall_signature_score"],
                "coverage_pct": result["data_integrity"]["summary"]["coverage_pct"],
                "corrections": len(result["data_integrity"].get("corrections", [])),
            }
        )


if __name__ == "__main__":
    main()
