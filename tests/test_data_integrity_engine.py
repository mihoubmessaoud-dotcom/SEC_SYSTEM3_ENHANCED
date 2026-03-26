from modules.data_integrity_engine import DataIntegrityEngine
from modules.data_repository import DataRepository
from modules.data_repository import DuplicateWriteError


def test_missing_returns_no_source_data() -> None:
    engine = DataIntegrityEngine()
    out = engine.validate_value("gross_margin", None).to_dict()
    assert out["value"] is None
    assert out["reason"] == "NO_SOURCE_DATA"
    assert "NO_SOURCE_DATA" in out["display"]
    assert out["status"] == "MISSING"


def test_rejects_impossible_bounds() -> None:
    engine = DataIntegrityEngine()

    dso = engine.validate_value("DSO", 500).to_dict()
    assert dso["value"] is None
    assert dso["reason"] == "OUT_OF_BOUNDS"
    assert "OUT_OF_BOUNDS" in dso["display"]
    assert dso["status"] == "REJECTED"

    gross_margin = engine.validate_value("gross_margin", 1.5).to_dict()
    assert gross_margin["value"] is None
    assert gross_margin["reason"] == "OUT_OF_BOUNDS"


def test_accepts_valid_values_inside_bounds() -> None:
    engine = DataIntegrityEngine()
    ap_days = engine.validate_value("ap_days", 120).to_dict()
    assert ap_days["value"] == 120.0
    assert ap_days["display"] == "120.0"
    assert ap_days["status"] == "OK"


def test_detects_frozen_values_repeated_four_years() -> None:
    engine = DataIntegrityEngine(freeze_threshold_years=4)
    series = {
        2020: 10,
        2021: 10,
        2022: 10,
        2023: 10,
        2024: 11,
    }
    out = engine.validate_series("ap_days", series)
    for y in [2020, 2021, 2022, 2023]:
        assert "SUSPECTED_BACKFILL" in out[y]["flags"]
        assert out[y]["status"] == "FLAGGED"
    assert "flags" not in out[2024]


def test_freeze_detection_breaks_on_missing_value() -> None:
    engine = DataIntegrityEngine(freeze_threshold_years=4)
    series = {
        2020: 8,
        2021: 8,
        2022: None,
        2023: 8,
        2024: 8,
        2025: 8,
        2026: 8,
    }
    out = engine.validate_series("ap_days", series)
    assert "flags" not in out[2020]
    assert out[2022]["reason"] == "NO_SOURCE_DATA"
    for y in [2023, 2024, 2025, 2026]:
        assert "SUSPECTED_BACKFILL" in out[y]["flags"]


def test_validate_raw_dataset_shape() -> None:
    engine = DataIntegrityEngine()
    data = {
        "inventory_days": {2021: 120, 2022: 1800, 2023: 2000},
        "net_margin": {2021: 0.2, 2022: "", 2023: -2.5},
    }
    out = engine.validate_raw_dataset(data)
    assert "inventory_days" in out
    assert "net_margin" in out
    assert out["inventory_days"][2023]["reason"] == "OUT_OF_BOUNDS"
    assert out["net_margin"][2022]["reason"] == "NO_SOURCE_DATA"
    assert out["net_margin"][2023]["reason"] == "OUT_OF_BOUNDS"


def test_store_validated_series_only_writes_valid_to_clean() -> None:
    engine = DataIntegrityEngine()
    repo = DataRepository()
    validated = engine.store_validated_series(
        repository=repo,
        metric="DSO",
        values_by_year={
            2022: 45,      # valid
            2023: None,    # NO_SOURCE_DATA
            2024: 9999,    # OUT_OF_BOUNDS
        },
    )

    assert validated[2022]["value"] == 45.0
    assert validated[2023]["reason"] == "NO_SOURCE_DATA"
    assert validated[2024]["reason"] == "OUT_OF_BOUNDS"

    # Raw is always stored
    assert repo.get_raw("dso:2022")["value"] == 45
    assert repo.get_raw("dso:2023")["value"] is None
    assert repo.get_raw("dso:2024")["value"] == 9999

    # Clean only for valid values
    assert repo.get_clean("dso:2022")["value"] == 45.0
    assert repo.get_clean("dso:2023")["reason"] == "NO_SOURCE_DATA"
    assert repo.get_clean("dso:2024")["reason"] == "NO_SOURCE_DATA"


def test_store_validated_series_respects_no_duplicate_writes() -> None:
    engine = DataIntegrityEngine()
    repo = DataRepository()
    series = {2025: 30}
    engine.store_validated_series(repo, "ap_days", series)
    try:
        engine.store_validated_series(repo, "ap_days", series)
        assert False, "Expected duplicate write rejection from repository"
    except DuplicateWriteError as exc:
        assert "DUPLICATE_WRITE" in str(exc)
