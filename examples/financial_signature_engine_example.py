from pathlib import Path
from pprint import pprint
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.financial_signature_engine import FinancialSignatureEngine


def main() -> None:
    engine = FinancialSignatureEngine()

    jpm_trends = {
        "revenue_growth": {2023: 0.04, 2024: 0.05, 2025: 0.06},
        "net_income_growth": {2023: 0.03, 2024: 0.04, 2025: 0.05},
        "roe_spread": {2023: 0.030, 2024: 0.040, 2025: 0.055},
        "nim": {2023: 0.022, 2024: 0.026, 2025: 0.029},
        "capex_to_revenue": {2023: 0.025, 2024: 0.023, 2025: 0.022},
        "asset_turnover": {2023: 0.08, 2024: 0.09, 2025: 0.09},
        "leverage": {2023: 11.5, 2024: 11.2, 2025: 10.8},
        "interest_coverage": {2023: 2.1, 2024: 2.3, 2025: 2.5},
        "altman_z": {2023: 2.0, 2024: 2.2, 2025: 2.4},
    }

    nvda_trends = {
        "revenue_growth": {2023: 0.25, 2024: 0.55, 2025: 0.40},
        "net_income_growth": {2023: 0.30, 2024: 0.70, 2025: 0.45},
        "roic": {2023: 0.32, 2024: 0.58, 2025: 0.69},
        "operating_margin": {2023: 0.28, 2024: 0.48, 2025: 0.55},
        "capex_to_revenue": {2023: 0.05, 2024: 0.04, 2025: 0.03},
        "asset_turnover": {2023: 0.72, 2024: 0.85, 2025: 0.92},
        "leverage": {2023: 0.55, 2024: 0.43, 2025: 0.35},
        "interest_coverage": {2023: 22.0, 2024: 28.0, 2025: 34.0},
        "altman_z": {2023: 5.2, 2024: 6.1, 2025: 7.0},
    }

    ko_trends = {
        "revenue_growth": {2023: 0.03, 2024: 0.04, 2025: 0.04},
        "net_income_growth": {2023: 0.02, 2024: 0.03, 2025: 0.03},
        "roic": {2023: 0.10, 2024: 0.12, 2025: 0.13},
        "operating_margin": {2023: 0.24, 2024: 0.25, 2025: 0.26},
        "capex_to_revenue": {2023: 0.06, 2024: 0.06, 2025: 0.05},
        "asset_turnover": {2023: 0.56, 2024: 0.57, 2025: 0.58},
        "leverage": {2023: 1.70, 2024: 1.62, 2025: 1.55},
        "interest_coverage": {2023: 8.5, 2024: 9.3, 2025: 10.1},
        "altman_z": {2023: 3.2, 2024: 3.4, 2025: 3.6},
    }

    print("\nJPM")
    pprint(engine.generate_signature("commercial_bank", jpm_trends))
    print("\nNVDA")
    pprint(engine.generate_signature("semiconductor_fabless", nvda_trends))
    print("\nKO")
    pprint(engine.generate_signature("consumer_staples", ko_trends))


if __name__ == "__main__":
    main()

