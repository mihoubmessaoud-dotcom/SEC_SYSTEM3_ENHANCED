from modules.financial_signature_engine import FinancialSignatureEngine


def test_signature_output_shape() -> None:
    engine = FinancialSignatureEngine()
    data = {
        "revenue_growth": {2023: 0.03, 2024: 0.04, 2025: 0.05},
        "net_income_growth": {2023: 0.02, 2024: 0.03, 2025: 0.04},
        "roic": {2023: 0.10, 2024: 0.12, 2025: 0.14},
        "operating_margin": {2023: 0.12, 2024: 0.13, 2025: 0.14},
        "capex_to_revenue": {2023: 0.08, 2024: 0.07, 2025: 0.06},
        "asset_turnover": {2023: 0.70, 2024: 0.72, 2025: 0.75},
        "leverage": {2023: 1.4, 2024: 1.3, 2025: 1.2},
        "interest_coverage": {2023: 6.0, 2024: 7.0, 2025: 8.0},
        "altman_z": {2023: 3.2, 2024: 3.4, 2025: 3.7},
    }
    out = engine.generate_signature("asset_light", data)
    assert set(out.keys()) == {
        "model_used",
        "growth_profile",
        "profitability",
        "capital_intensity",
        "financial_risk",
        "overall_signature_score",
    }
    assert 0 <= out["overall_signature_score"] <= 100


def test_bank_profile_uses_bank_metrics_path() -> None:
    engine = FinancialSignatureEngine()
    data = {
        "revenue_growth": {2023: 0.04, 2024: 0.05, 2025: 0.06},
        "net_income_growth": {2023: 0.03, 2024: 0.04, 2025: 0.05},
        "roe_spread": {2023: 0.02, 2024: 0.03, 2025: 0.05},
        "nim": {2023: 0.02, 2024: 0.025, 2025: 0.03},
        "capex_to_revenue": {2023: 0.025, 2024: 0.024, 2025: 0.022},
        "asset_turnover": {2023: 0.08, 2024: 0.09, 2025: 0.10},
        "leverage": {2023: 12.0, 2024: 11.5, 2025: 11.0},
        "interest_coverage": {2023: 2.2, 2024: 2.3, 2025: 2.5},
        "altman_z": {2023: 2.0, 2024: 2.2, 2025: 2.4},
    }
    out = engine.generate_signature("commercial_bank", data)
    assert out["model_used"] == "commercial_bank"
    assert out["profitability"] > 0


def test_requires_3year_trends_effectively() -> None:
    engine = FinancialSignatureEngine()
    data = {
        "revenue_growth": {2024: 0.04, 2025: 0.05},  # only 2 points
        "net_income_growth": {2024: 0.03, 2025: 0.04},
        "roic": {2024: 0.12, 2025: 0.13},
        "operating_margin": {2024: 0.14, 2025: 0.15},
        "capex_to_revenue": {2024: 0.08, 2025: 0.07},
        "asset_turnover": {2024: 0.72, 2025: 0.75},
        "leverage": {2024: 1.3, 2025: 1.2},
    }
    out = engine.generate_signature("asset_light", data)
    # with insufficient trend depth, output remains bounded and low/neutral
    assert 0 <= out["overall_signature_score"] <= 100
    assert out["growth_profile"] == 0.0


def test_model_adaptation_changes_overall_score_weighting() -> None:
    engine = FinancialSignatureEngine()
    shared = {
        "revenue_growth": {2023: 0.04, 2024: 0.04, 2025: 0.05},
        "net_income_growth": {2023: 0.03, 2024: 0.03, 2025: 0.04},
        "roic": {2023: 0.10, 2024: 0.11, 2025: 0.12},
        "operating_margin": {2023: 0.10, 2024: 0.12, 2025: 0.13},
        "capex_to_revenue": {2023: 0.06, 2024: 0.05, 2025: 0.05},
        "asset_turnover": {2023: 0.60, 2024: 0.62, 2025: 0.64},
        "leverage": {2023: 1.6, 2024: 1.5, 2025: 1.4},
        "interest_coverage": {2023: 5.0, 2024: 6.0, 2025: 7.0},
        "altman_z": {2023: 2.5, 2024: 2.7, 2025: 2.9},
        "roe_spread": {2023: 0.02, 2024: 0.03, 2025: 0.04},
        "nim": {2023: 0.02, 2024: 0.02, 2025: 0.025},
    }
    bank = engine.generate_signature("commercial_bank", shared)
    semi = engine.generate_signature("semiconductor_fabless", shared)
    assert bank["overall_signature_score"] != semi["overall_signature_score"]

