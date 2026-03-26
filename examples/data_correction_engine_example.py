from modules.data_correction_engine import DataCorrectionEngine


def main() -> None:
    engine = DataCorrectionEngine()
    raw = {
        "revenue": {
            2023: 211_915,
            2024: 245_122_000,  # unit slip
            2025: 270_000_000,  # unit slip
        },
        "market_cap": {
            2023: 2_200_000,
            2024: 2_450_000,
            2025: 2_600_000,
        },
        "gross_margin": {
            2023: 0.62,
            2024: None,  # remains missing, never filled
            2025: 0.66,
        },
    }
    out = engine.correct_dataset(raw)
    print("Corrected metrics:")
    for metric, series in out["corrected_metrics"].items():
        print(metric, series)
    print("\nCorrection log:")
    for row in out["corrections"]:
        print(row)
    print("\nFlags:")
    for row in out["flags"]:
        print(row)


if __name__ == "__main__":
    main()
