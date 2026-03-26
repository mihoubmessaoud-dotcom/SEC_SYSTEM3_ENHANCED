from pprint import pprint

from modules.data_integrity_engine import DataIntegrityEngine


def main() -> None:
    engine = DataIntegrityEngine()

    raw_data = {
        "ap_days": {
            2021: 45,
            2022: 45,
            2023: 45,
            2024: 45,
            2025: 46,
        },
        "DSO": {
            2021: 38,
            2022: 42,
            2023: 410,  # out of bounds
            2024: None,  # no source data
            2025: 36,
        },
        "gross_margin": {
            2021: 0.62,
            2022: 0.61,
            2023: "N/A",
            2024: 1.2,  # out of bounds
            2025: 0.59,
        },
    }

    result = engine.validate_raw_dataset(raw_data)
    pprint(result)


if __name__ == "__main__":
    main()

