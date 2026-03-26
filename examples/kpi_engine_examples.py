from modules.kpi_engine import KPIEngine


def main() -> None:
    engine = KPIEngine()

    bank = engine.assign(
        {
            "model": "commercial_bank",
            "confidence": 0.88,
            "alternatives": [{"model": "consumer_staples", "confidence": 0.42}],
        }
    )
    fabless = engine.assign(
        {
            "model": "semiconductor_fabless",
            "confidence": 0.91,
            "alternatives": [{"model": "semiconductor_idm", "confidence": 0.63}],
        }
    )
    consumer = engine.assign(
        {
            "model": "consumer_staples",
            "confidence": 0.78,
            "alternatives": [{"model": "commercial_bank", "confidence": 0.30}],
        }
    )

    print("BANK:", bank)
    print("SEMICONDUCTOR:", fabless)
    print("CONSUMER:", consumer)

    compact_bank = engine.assign_dynamic(
        {
            "model": "commercial_bank",
            "confidence": 0.88,
            "alternatives": [{"model": "consumer_staples", "confidence": 0.42}],
        }
    )
    print("BANK_COMPACT:", compact_bank)


if __name__ == "__main__":
    main()
