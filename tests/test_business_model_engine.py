from modules.business_model_engine import BusinessModelEngine
from modules.data_repository import DataRepository


def test_nvda_classified_as_fabless() -> None:
    engine = BusinessModelEngine()
    result = engine.classify(
        gross_margin=0.74,
        capex_to_revenue=0.03,
        rd_to_revenue=0.14,
    )
    assert result["model"] == "semiconductor_fabless"
    assert result["confidence"] >= 0.80


def test_intc_classified_as_idm() -> None:
    engine = BusinessModelEngine()
    result = engine.classify(
        gross_margin=0.35,
        capex_to_revenue=0.28,
        rd_to_revenue=0.14,
    )
    assert result["model"] == "semiconductor_idm"
    assert result["confidence"] >= 0.70


def test_bank_classification() -> None:
    engine = BusinessModelEngine()
    result = engine.classify(
        gross_margin=0.32,
        capex_to_revenue=0.02,
        rd_to_revenue=0.01,
    )
    assert "commercial_bank" in str(result["model"])


def test_hybrid_support_when_two_models_close() -> None:
    engine = BusinessModelEngine()
    result = engine.classify(
        gross_margin=0.40,
        capex_to_revenue=0.11,
        rd_to_revenue=0.075,
    )
    # close to semiconductor_idm + consumer_staples
    assert str(result["model"]).startswith("hybrid:")
    assert 0.62 <= float(result["confidence"]) <= 1.0


def test_output_shape() -> None:
    engine = BusinessModelEngine()
    result = engine.classify(
        gross_margin=0.50,
        capex_to_revenue=0.07,
        rd_to_revenue=0.09,
    )
    assert set(result.keys()) == {"model", "confidence", "alternatives"}
    assert isinstance(result["alternatives"], list)


def test_amd_example_classification() -> None:
    engine = BusinessModelEngine()
    result = engine.classify(
        gross_margin=0.49,
        capex_to_revenue=0.07,
        rd_to_revenue=0.24,
    )
    # AMD should resolve to fabless or hybrid near fabless/idm.
    assert "semiconductor_fabless" in str(result["model"])


def test_classify_from_repository_uses_clean_data_only() -> None:
    repo = DataRepository()
    # Seed only clean values needed for model detection
    repo.set_clean("gross_margin:2025", 0.68, reason="validated")
    repo.set_clean("capex_to_revenue:2025", 0.03, reason="validated")
    repo.set_clean("rd_to_revenue:2025", 0.13, reason="validated")
    engine = BusinessModelEngine()
    result = engine.classify_from_repository(repo, 2025)
    assert result["model"] == "semiconductor_fabless"
