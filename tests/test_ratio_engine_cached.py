from modules.data_repository import DataRepository
from modules.ratio_engine_cached import (
    DuplicateRatioCalculationError,
    RatioEngine,
    RatioRegistry,
)


def _seed_common(repo: DataRepository) -> None:
    repo.set_clean("depreciation_amortization:2025", 18_000, reason="validated")
    repo.set_clean("operating_income:2025", 152_000, reason="validated")
    repo.set_clean("tax_rate:2025", 0.20, reason="validated")
    repo.set_clean("invested_capital:2025", 760_000, reason="validated")
    repo.set_clean("net_income:2025", 114_000, reason="validated")
    repo.set_clean("total_equity:2025", 95_000, reason="validated")
    repo.set_clean("pe_ratio:2025", 30.0, reason="validated")
    repo.set_clean("earnings_growth:2025", 0.15, reason="validated")
    repo.set_clean("gross_profit:2025", 190_000, reason="validated")
    repo.set_clean("revenue:2025", 430_000, reason="validated")


def test_ratios_calculated_once_and_cached() -> None:
    repo = DataRepository()
    _seed_common(repo)
    engine = RatioEngine(repo, RatioRegistry())

    r1 = engine.calculate("roic", 2025)
    r2 = engine.calculate("roic", 2025)
    assert r1["value"] is not None
    assert r2["cached"] is True
    assert engine.get_compute_count("roic") == 1


def test_ebitda_formula() -> None:
    repo = DataRepository()
    _seed_common(repo)
    engine = RatioEngine(repo, RatioRegistry())
    out = engine.calculate("ebitda", 2025)
    assert out["reason"] == ""
    assert out["value"] == 170_000


def test_roe_formula() -> None:
    repo = DataRepository()
    _seed_common(repo)
    engine = RatioEngine(repo, RatioRegistry())
    out = engine.calculate("roe", 2025)
    assert out["reason"] == ""
    assert abs(out["value"] - (114_000 / 95_000)) < 1e-12


def test_peg_uses_growth_percent_points() -> None:
    repo = DataRepository()
    _seed_common(repo)
    engine = RatioEngine(repo, RatioRegistry())
    out = engine.calculate("peg", 2025)
    # 30 / 15 = 2.0
    assert abs(out["value"] - 2.0) < 1e-12


def test_gross_margin_formula() -> None:
    repo = DataRepository()
    _seed_common(repo)
    engine = RatioEngine(repo, RatioRegistry())
    out = engine.calculate("gross_margin", 2025)
    assert abs(out["value"] - (190_000 / 430_000)) < 1e-12


def test_no_source_data_when_missing_clean_inputs() -> None:
    repo = DataRepository()
    # only one input available
    repo.set_clean("gross_profit:2025", 190_000, reason="validated")
    engine = RatioEngine(repo, RatioRegistry())
    out = engine.calculate("gross_margin", 2025)
    assert out["value"] is None
    assert out["reason"] == "NO_SOURCE_DATA"


def test_registry_duplicate_write_blocked() -> None:
    repo = DataRepository()
    _seed_common(repo)
    reg = RatioRegistry()
    engine = RatioEngine(repo, reg)
    _ = engine.calculate("roe", 2025)
    # Force direct duplicate set by repeated set in registry path.
    try:
        reg.set(reg.get("roe", 2025))  # type: ignore[arg-type]
        assert False, "Expected duplicate ratio write rejection"
    except KeyError as exc:
        assert "DUPLICATE_RATIO_WRITE" in str(exc)


def test_strict_mode_blocks_repeated_request() -> None:
    repo = DataRepository()
    _seed_common(repo)
    engine = RatioEngine(repo, RatioRegistry())
    _ = engine.calculate("roe", 2025)
    try:
        _ = engine.calculate("roe", 2025, strict_no_repeat=True)
        assert False, "Expected strict duplicate calculation rejection"
    except DuplicateRatioCalculationError as exc:
        assert "DUPLICATE_RATIO_CALCULATION" in str(exc)
