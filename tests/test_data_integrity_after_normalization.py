from modules.data_integrity_engine import DataIntegrityEngine
from modules.unit_normalization_engine import UnitNormalizationEngine


def test_none_returns_no_source_data() -> None:
    engine = DataIntegrityEngine()
    out = engine.validate_value("net_margin", None).to_dict()
    assert out["value"] is None
    assert out["reason"] == "NO_SOURCE_DATA"


def test_rejects_impossible_bounds_strictly() -> None:
    engine = DataIntegrityEngine()
    assert engine.validate_value("ap_days", 900).to_dict()["reason"] == "OUT_OF_BOUNDS"
    assert engine.validate_value("DSO", 900).to_dict()["reason"] == "OUT_OF_BOUNDS"
    assert engine.validate_value("inventory_days", 2000).to_dict()["reason"] == "OUT_OF_BOUNDS"
    assert engine.validate_value("gross_margin", 1.1).to_dict()["reason"] == "OUT_OF_BOUNDS"
    assert engine.validate_value("net_margin", -2.5).to_dict()["reason"] == "OUT_OF_BOUNDS"


def test_detects_frozen_values_4_years() -> None:
    engine = DataIntegrityEngine(freeze_threshold_years=4)
    series = {2022: 1.5, 2023: 1.5, 2024: 1.5, 2025: 1.5}
    out = engine.validate_series("ap_days", series)
    for y in (2022, 2023, 2024, 2025):
        assert "SUSPECTED_BACKFILL" in out[y]["flags"]
        assert out[y]["status"] == "FLAGGED"


def test_validation_does_not_modify_values() -> None:
    engine = DataIntegrityEngine()
    original = 0.42
    out = engine.validate_value("gross_margin", original).to_dict()
    assert out["value"] == original


def test_integration_after_normalization() -> None:
    raw = {
        "DSO": {2024: 320_000, 2025: 340_000},
        "gross_margin": {2024: 0.60, 2025: 0.62},
    }
    normalizer = UnitNormalizationEngine()
    integrity = DataIntegrityEngine()

    normalized = normalizer.normalize_dataset(raw)["normalized_metrics"]
    validated = integrity.validate_raw_dataset(normalized)

    # DSO converted to millions first -> tiny values, still valid within DSO bounds here only if <=365
    assert 2024 in validated["DSO"]
    assert validated["gross_margin"][2024]["value"] == 0.60
