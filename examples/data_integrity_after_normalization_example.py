from modules.unit_normalization_engine import UnitNormalizationEngine
from modules.data_integrity_engine import DataIntegrityEngine


def main() -> None:
    raw = {
        "DSO": {2022: 320_000, 2023: 340_000, 2024: None, 2025: 380_000},
        "ap_days": {2022: 90, 2023: 90, 2024: 90, 2025: 90},  # frozen series example
        "gross_margin": {2022: 0.55, 2023: 0.56, 2024: 0.57, 2025: 1.2},  # out of bounds in 2025
        "inventory_days": {2022: 180, 2023: 190, 2024: 195, 2025: 200},
        "net_margin": {2022: 0.20, 2023: 0.21, 2024: 0.22, 2025: 0.23},
    }

    normalizer = UnitNormalizationEngine()
    integrity = DataIntegrityEngine()

    normalized = normalizer.normalize_dataset(raw)["normalized_metrics"]
    validated = integrity.validate_raw_dataset(normalized)

    print("Normalized:")
    print(normalized)
    print("\nValidated:")
    print(validated)


if __name__ == "__main__":
    main()
