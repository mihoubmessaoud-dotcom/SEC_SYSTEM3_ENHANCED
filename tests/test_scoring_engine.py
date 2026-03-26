from modules.scoring_engine import ScoringEngine


def test_bank_scoring_in_0_100_range() -> None:
    engine = ScoringEngine()
    out = engine.score(
        "commercial_bank",
        {
            "roe_spread": 0.055,
            "nim": 0.029,
        },
    )
    assert out["status"] == "OK"
    assert 0.0 <= float(out["score"]) <= 100.0
    assert out["model_used"] == "commercial_bank"


def test_semiconductor_scoring_uses_roic_and_margins() -> None:
    engine = ScoringEngine()
    out = engine.score(
        "semiconductor_fabless",
        {
            "roic": 0.35,
            "gross_margin": 0.68,
            "operating_margin": 0.30,
        },
    )
    assert out["status"] == "OK"
    metrics_in_details = {d["metric"] for d in out["details"]}
    assert {"roic", "gross_margin", "operating_margin"} <= metrics_in_details


def test_consumer_scoring_uses_roic_fcf_margins() -> None:
    engine = ScoringEngine()
    out = engine.score(
        "consumer_staples",
        {
            "roic": 0.13,
            "fcf_yield": 0.03,
            "gross_margin": 0.58,
        },
    )
    assert out["status"] == "OK"
    metrics_in_details = {d["metric"] for d in out["details"]}
    assert {"roic", "fcf_yield", "gross_margin"} <= metrics_in_details


def test_none_value_rejected() -> None:
    engine = ScoringEngine()
    out = engine.score(
        "commercial_bank",
        {
            "roe_spread": None,
            "nim": 0.025,
        },
    )
    assert out["status"] == "OK"
    assert out["score"] is not None
    assert "roe_spread" in out["ignored_none_metrics"]


def test_hybrid_model_uses_primary_component() -> None:
    engine = ScoringEngine()
    out = engine.score(
        "hybrid:semiconductor_fabless+asset_light",
        {
            "roic": 0.30,
            "gross_margin": 0.65,
            "operating_margin": 0.25,
        },
    )
    assert out["status"] == "OK"
    assert out["model_used"] == "semiconductor_fabless"


def test_score_accepts_ratio_engine_style_payload() -> None:
    engine = ScoringEngine()
    out = engine.score(
        "consumer_staples",
        {
            "roic": {"value": 0.12, "reason": ""},
            "fcf_yield": {"value": 0.03, "reason": ""},
            "gross_margin": {"value": 0.50, "reason": ""},
        },
    )
    assert out["status"] == "OK"
    assert 0.0 <= float(out["score"]) <= 100.0


def test_score_from_ratio_engine_uses_cached_values_without_recalc() -> None:
    engine = ScoringEngine()
    out = engine.score_from_ratio_engine(
        "semiconductor_fabless",
        {
            "roic": {"value": 0.22, "reason": "", "cached": True},
            "gross_margin": {"value": 0.58, "reason": "", "cached": True},
            "operating_margin": {"value": 0.21, "reason": "", "cached": True},
        },
    )
    assert out["status"] == "OK"
    assert out["model_used"] == "semiconductor_fabless"
