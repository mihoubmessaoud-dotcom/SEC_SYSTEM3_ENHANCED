from modules.data_correction_engine import DataCorrectionEngine


def main() -> None:
    engine = DataCorrectionEngine(jump_ratio_threshold=8.0)
    raw = {
        "revenue": {2023: 211_915, 2024: 245_122_000, 2025: 270_000_000},
        "market_cap": {2023: 120_000, 2024: 130_000_000},
        "gross_margin": {2023: 0.62, 2024: None, 2025: 0.66},  # missing remains missing
    }
    out = engine.correct_dataset(raw)
    print("corrected_metrics:", out["corrected_metrics"])
    print("corrections:", out["corrections"])
    print("flags:", out["flags"])
    print("log_summary:", out["log_summary"])


if __name__ == "__main__":
    main()
