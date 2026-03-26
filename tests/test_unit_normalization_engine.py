from modules.unit_normalization_engine import UnitNormalizationEngine


def test_examples_required_by_spec() -> None:
    engine = UnitNormalizationEngine()
    a = engine.normalize_value(1_000_000_000, metric="revenue")
    b = engine.normalize_value(500_000, metric="revenue")
    assert a["normalized_value"] == 1000.0
    assert b["normalized_value"] == 0.5
    assert a["target_unit"] == "millions"
    assert b["target_unit"] == "millions"


def test_missing_value_remains_missing() -> None:
    engine = UnitNormalizationEngine()
    out = engine.normalize_value(None, metric="revenue")
    assert out["normalized_value"] is None
    assert out["detected_scale"] == "NO_SOURCE_DATA"


def test_non_monetary_metric_is_not_rescaled() -> None:
    engine = UnitNormalizationEngine()
    out = engine.normalize_value(0.42, metric="gross_margin")
    assert out["normalized_value"] == 0.42
    assert out["detected_scale"] == "unitless_or_ratio"


def test_dataset_normalization_shape() -> None:
    engine = UnitNormalizationEngine()
    raw = {
        "revenue": {2024: 250_000_000, 2025: 300_000_000},
        "gross_margin": {2024: 0.62, 2025: 0.64},
    }
    out = engine.normalize_dataset(raw)
    assert out["target_unit"] == "millions"
    assert out["normalized_metrics"]["revenue"][2024] == 250.0
    assert out["normalized_metrics"]["gross_margin"][2024] == 0.62
