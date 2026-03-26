from pathlib import Path
from pprint import pprint
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.business_model_engine import BusinessModelEngine
from modules.kpi_engine import KPIEngine


def main() -> None:
    model_engine = BusinessModelEngine()
    kpi_engine = KPIEngine()

    samples = {
        "NVDA": {
            "gross_margin": 0.74,
            "capex_to_revenue": 0.03,
            "rd_to_revenue": 0.14,
            "leverage": 0.35,
        },
        "AMD": {
            "gross_margin": 0.52,
            "capex_to_revenue": 0.06,
            "rd_to_revenue": 0.24,
            "leverage": 1.20,
        },
        "INTC": {
            "gross_margin": 0.35,
            "capex_to_revenue": 0.28,
            "rd_to_revenue": 0.14,
            "leverage": 2.10,
        },
    }

    for ticker, metrics in samples.items():
        model_result = model_engine.classify(**metrics)
        kpi_result = kpi_engine.assign(model_result)
        print(f"\n{ticker}")
        print("-" * len(ticker))
        pprint(model_result)
        pprint(kpi_result)


if __name__ == "__main__":
    main()

