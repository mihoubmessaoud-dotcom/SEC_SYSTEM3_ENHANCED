from pathlib import Path


FORBIDDEN_PATTERNS = (".fillna(", ".ffill(", ".bfill(")


def test_no_backfill_calls_in_core_pipeline_modules() -> None:
    files = [
        Path("modules/data_repository.py"),
        Path("modules/data_integrity_engine.py"),
        Path("modules/data_correction_engine.py"),
        Path("modules/unit_normalization_engine.py"),
        Path("modules/ratio_engine_cached.py"),
        Path("modules/business_model_engine.py"),
        Path("modules/kpi_engine.py"),
        Path("modules/scoring_engine.py"),
        Path("modules/financial_signature_engine.py"),
        Path("modules/financial_analysis_system.py"),
        Path("layers/yahoo_layer.py"),
    ]

    violations = []
    for file_path in files:
        content = file_path.read_text(encoding="utf-8", errors="ignore")
        lowered = content.lower()
        for pattern in FORBIDDEN_PATTERNS:
            if pattern in lowered:
                violations.append(f"{file_path}: contains '{pattern}'")

    assert not violations, "Backfill policy violations:\n" + "\n".join(violations)
