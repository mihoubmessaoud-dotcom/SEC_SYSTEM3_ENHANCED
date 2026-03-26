from pathlib import Path
from pprint import pprint
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.business_model_engine import BusinessModelEngine


def main() -> None:
    engine = BusinessModelEngine()

    # Example snapshots (illustrative)
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

    for ticker, m in samples.items():
        result = engine.classify(**m)
        print(f"\n{ticker}:")
        pprint(result)


if __name__ == "__main__":
    main()
