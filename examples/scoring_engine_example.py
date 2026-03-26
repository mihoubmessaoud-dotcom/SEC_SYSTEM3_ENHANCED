from pathlib import Path
from pprint import pprint
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.scoring_engine import ScoringEngine


def main() -> None:
    engine = ScoringEngine()

    samples = {
        # Banks -> ROE_spread, NIM
        "JPM": {
            "model": "commercial_bank",
            "metrics": {
                "roe_spread": 0.055,
                "nim": 0.029,
            },
        },
        # Semiconductors -> ROIC, margins
        "NVDA": {
            "model": "semiconductor_fabless",
            "metrics": {
                "roic": 0.69,
                "gross_margin": 0.75,
                "operating_margin": 0.55,
            },
        },
        # Consumer -> ROIC, FCF, margins
        "KO": {
            "model": "consumer_staples",
            "metrics": {
                "roic": 0.14,
                "fcf_yield": 0.028,
                "gross_margin": 0.60,
            },
        },
    }

    for ticker, payload in samples.items():
        result = engine.score(payload["model"], payload["metrics"])
        print(f"\n{ticker}")
        print("-" * len(ticker))
        pprint(result)

    ratio_payload_example = engine.score_from_ratio_engine(
        "commercial_bank",
        {
            "roe_spread": {"value": 0.052, "reason": "", "cached": True},
            "nim": {"value": 0.028, "reason": "", "cached": True},
        },
    )
    print("\nRATIO_ENGINE_PAYLOAD_EXAMPLE")
    print("----------------------------")
    pprint(ratio_payload_example)


if __name__ == "__main__":
    main()
