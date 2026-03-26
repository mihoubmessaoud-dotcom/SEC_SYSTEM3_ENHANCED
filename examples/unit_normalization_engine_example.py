from modules.unit_normalization_engine import UnitNormalizationEngine


def main() -> None:
    engine = UnitNormalizationEngine()

    print(engine.normalize_value(1_000_000_000, metric="revenue"))  # -> 1000 (millions)
    print(engine.normalize_value(500_000, metric="revenue"))        # -> 0.5 (millions)
    print(engine.normalize_value(None, metric="revenue"))           # -> None, NO_SOURCE_DATA
    print(engine.normalize_value(0.62, metric="gross_margin"))      # ratio, unchanged

    dataset = {
        "revenue": {2024: 250_000_000, 2025: 300_000_000},
        "gross_margin": {2024: 0.62, 2025: 0.64},
    }
    print(engine.normalize_dataset(dataset))


if __name__ == "__main__":
    main()
