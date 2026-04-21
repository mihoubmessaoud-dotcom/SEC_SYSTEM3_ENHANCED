from __future__ import annotations


def test_financial_analysis_system_forced_model_semis():
    from modules.financial_analysis_system import FinancialAnalysisSystem

    fas = FinancialAnalysisSystem()

    # Minimal raw dataset; values arbitrary, we only validate model forcing.
    raw = {
        "gross_margin": {2024: 0.10},  # would normally bias away from semis
        "capex_to_revenue": {2024: 0.01},
        "rd_to_revenue": {2024: 0.00},
    }
    out = fas.analyze(ticker="NVDA", raw_metrics_by_year=raw, forced_model="semiconductor_fabless")
    assert out["model"]["model"] == "semiconductor_fabless"
    assert out["model"]["confidence"] >= 0.95


def test_financial_analysis_system_forced_model_banks():
    from modules.financial_analysis_system import FinancialAnalysisSystem

    fas = FinancialAnalysisSystem()
    raw = {
        "gross_margin": {2024: 0.60},
        "capex_to_revenue": {2024: 0.02},
        "rd_to_revenue": {2024: 0.12},
    }
    out = fas.analyze(ticker="JPM", raw_metrics_by_year=raw, forced_model="commercial_bank")
    assert out["model"]["model"] == "commercial_bank"

