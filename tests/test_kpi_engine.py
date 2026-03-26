from modules.kpi_engine import KPIEngine


def test_bank_kpi_mapping_contains_required_primary() -> None:
    engine = KPIEngine()
    out = engine.get_kpis_for_model("commercial_bank")
    assert "nim" in out["primary"]
    assert "roe_spread" in out["primary"]
    assert "gross_margin" in out["ignored"]


def test_semiconductor_kpi_mapping_contains_required_primary() -> None:
    engine = KPIEngine()
    out = engine.get_kpis_for_model("semiconductor_fabless")
    assert "roic" in out["primary"]
    assert "gross_margin" in out["primary"]
    assert "rd_to_revenue" in out["primary"]


def test_consumer_staples_mapping_contains_required_primary() -> None:
    engine = KPIEngine()
    out = engine.get_kpis_for_model("consumer_staples")
    assert "roic" in out["primary"]
    assert "fcf_yield" in out["primary"]
    assert "gross_margin" in out["primary"]


def test_hybrid_model_merges_priorities() -> None:
    engine = KPIEngine()
    out = engine.get_kpis_for_model("hybrid:semiconductor_fabless+asset_light")
    assert "roic" in out["primary"]
    assert "rd_to_revenue" in out["primary"]
    assert "fcf_margin" in out["primary"]
    assert "capex_to_revenue" in out["secondary"]


def test_assign_shape() -> None:
    engine = KPIEngine()
    out = engine.assign({"model": "commercial_bank", "confidence": 0.81, "alternatives": []})
    assert set(out.keys()) == {"model", "confidence", "kpis"}
    assert set(out["kpis"].keys()) == {"primary", "secondary", "ignored"}


def test_context_aware_low_confidence_adds_alt_primary_to_secondary() -> None:
    engine = KPIEngine()
    out = engine.assign(
        {
            "model": "commercial_bank",
            "confidence": 0.55,
            "alternatives": [{"model": "consumer_staples", "confidence": 0.54}],
        }
    )
    # consumer_staples primary metric blended into secondary when confidence is low
    assert "fcf_yield" in out["kpis"]["secondary"]


def test_assign_dynamic_compact_shape_primary_secondary_only() -> None:
    engine = KPIEngine()
    out = engine.assign_dynamic(
        {"model": "commercial_bank", "confidence": 0.88, "alternatives": []}
    )
    assert set(out.keys()) == {"primary", "secondary"}
    assert "roe_spread" in out["primary"]
    assert "nim" in out["primary"]
