from modules.data_integrity_engine import DataIntegrityEngine
from modules.data_repository import DataRepository


def main() -> None:
    engine = DataIntegrityEngine()
    repo = DataRepository()

    dso_series = {
        2022: 42.0,
        2023: 44.0,
        2024: None,   # missing from source
        2025: 401.0,  # impossible, rejected (bounds: 0..365)
    }

    validated = engine.store_validated_series(
        repository=repo,
        metric="DSO",
        values_by_year=dso_series,
    )

    print("Validated:")
    for year, result in validated.items():
        print(year, result)

    print("\nRepository raw:")
    for key in ["dso:2022", "dso:2023", "dso:2024", "dso:2025"]:
        print(key, repo.get_raw(key))

    print("\nRepository clean (only validated numeric values):")
    for key in ["dso:2022", "dso:2023", "dso:2024", "dso:2025"]:
        print(key, repo.get_clean(key))


if __name__ == "__main__":
    main()
