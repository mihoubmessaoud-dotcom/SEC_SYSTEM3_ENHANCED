from modules.financial_analysis_system import FinancialAnalysisSystem


def test_orchestrator_output_shape() -> None:
    system = FinancialAnalysisSystem()
    raw = {
        "gross_margin": {2023: 0.66, 2024: 0.72, 2025: 0.75},
        "capex_to_revenue": {2023: 0.05, 2024: 0.04, 2025: 0.03},
        "rd_to_revenue": {2023: 0.16, 2024: 0.15, 2025: 0.14},
        "leverage": {2023: 0.55, 2024: 0.43, 2025: 0.35},
        "roic": {2023: 0.32, 2024: 0.58, 2025: 0.69},
        "operating_margin": {2023: 0.28, 2024: 0.48, 2025: 0.55},
        "revenue_growth": {2023: 0.25, 2024: 0.55, 2025: 0.40},
        "net_income_growth": {2023: 0.30, 2024: 0.70, 2025: 0.45},
        "asset_turnover": {2023: 0.72, 2024: 0.85, 2025: 0.92},
        "interest_coverage": {2023: 22.0, 2024: 28.0, 2025: 34.0},
        "altman_z": {2023: 5.2, 2024: 6.1, 2025: 7.0},
    }
    out = system.analyze("NVDA", raw)
    assert set(out.keys()) == {
        "ticker",
        "model",
        "kpis",
        "ratios",
        "score",
        "signature",
        "financial_signature",
        "data_integrity",
    }
    assert out["ticker"] == "NVDA"
    assert "semiconductor_fabless" in str(out["model"]["model"])
    assert isinstance(out["ratios"], dict)
    assert 0 <= out["signature"]["overall_signature_score"] <= 100


def test_orchestrator_ignores_missing_scoring_metric_without_fabrication() -> None:
    system = FinancialAnalysisSystem()
    raw = {
        "gross_margin": {2023: 0.32, 2024: 0.33, 2025: 0.34},
        "capex_to_revenue": {2023: 0.02, 2024: 0.02, 2025: 0.02},
        "rd_to_revenue": {2023: 0.01, 2024: 0.01, 2025: 0.01},
        "leverage": {2023: 10.0, 2024: 10.5, 2025: 11.0},
        # Missing NIM intentionally; scorer must ignore None without fabricating.
        "roe_spread": {2023: 0.02, 2024: 0.03, 2025: 0.04},
        "revenue_growth": {2023: 0.03, 2024: 0.04, 2025: 0.05},
        "net_income_growth": {2023: 0.02, 2024: 0.03, 2025: 0.04},
    }
    out = system.analyze("JPM", raw)
    if out["model"]["model"] == "commercial_bank":
        assert out["score"]["status"] == "OK"
        assert out["score"]["score"] is not None
        assert "nim" in out["score"].get("ignored_none_metrics", [])


def test_orchestrator_contains_pipeline_audit_artifacts() -> None:
    system = FinancialAnalysisSystem()
    raw = {
        "gross_margin": {2023: 0.40, 2024: 0.42, 2025: 0.43},
        "capex_to_revenue": {2023: 0.07, 2024: 0.06, 2025: 0.06},
        "rd_to_revenue": {2023: 0.04, 2024: 0.04, 2025: 0.03},
        "operating_income": {2023: 90_000, 2024: 95_000, 2025: 100_000},
        "tax_rate": {2023: 0.20, 2024: 0.20, 2025: 0.20},
        "invested_capital": {2023: 600_000, 2024: 610_000, 2025: 620_000},
        "net_income": {2023: 70_000, 2024: 72_000, 2025: 75_000},
        "total_equity": {2023: 200_000, 2024: 205_000, 2025: 210_000},
        "pe_ratio": {2023: 20.0, 2024: 22.0, 2025: 24.0},
        "earnings_growth": {2023: 0.08, 2024: 0.09, 2025: 0.10},
        "gross_profit": {2023: 170_000, 2024: 175_000, 2025: 180_000},
        "revenue": {2023: 400_000, 2024: 410_000, 2025: 430_000},
        "revenue_growth": {2023: 0.03, 2024: 0.04, 2025: 0.05},
        "net_income_growth": {2023: 0.02, 2024: 0.03, 2025: 0.04},
        "operating_margin": {2023: 0.22, 2024: 0.23, 2025: 0.24},
        "leverage": {2023: 1.2, 2024: 1.1, 2025: 1.0},
        "interest_coverage": {2023: 8.0, 2024: 9.0, 2025: 10.0},
        "altman_z": {2023: 3.0, 2024: 3.2, 2025: 3.4},
        "asset_turnover": {2023: 0.70, 2024: 0.72, 2025: 0.74},
    }
    out = system.analyze("KO", raw)
    assert "pre_validation" in out["data_integrity"]
    assert "repository_meta" in out["data_integrity"]
    assert out["data_integrity"]["pipeline_trace"] == [
        "UnitNormalizationEngine",
        "DataIntegrityEngine:pre",
        "DataCorrectionEngine",
        "DataIntegrityEngine:post",
        "DataRepository",
        "RatioEngine",
        "BusinessModelEngine",
        "KPIEngine",
        "ScoringEngine",
        "FinancialSignatureEngine",
    ]
    assert 2025 in out["ratios"]
    assert set(out["ratios"][2025].keys()) == {"ebitda", "roic", "roe", "peg", "gross_margin"}


def test_missing_values_are_allowed_and_never_fabricated() -> None:
    system = FinancialAnalysisSystem()
    raw = {
        "gross_margin": {2024: None, 2025: None},
        "capex_to_revenue": {2024: 0.05, 2025: 0.04},
        "rd_to_revenue": {2024: 0.12, 2025: 0.11},
        # Intentionally omit ratio-input components for 2025
        "revenue_growth": {2024: 0.03, 2025: 0.04},
        "net_income_growth": {2024: 0.02, 2025: 0.03},
        "leverage": {2024: 1.2, 2025: 1.1},
    }
    out = system.analyze("TEST", raw)
    gm_2025 = out["data_integrity"]["validated_metrics"]["gross_margin"][2025]
    assert gm_2025["value"] is None
    assert gm_2025["reason"] == "NO_SOURCE_DATA"

    ratios_2025 = out["ratios"][2025]
    assert ratios_2025["roic"]["value"] is None
    assert ratios_2025["roic"]["reason"] == "NO_SOURCE_DATA"
    assert ratios_2025["roe"]["value"] is None
    assert ratios_2025["roe"]["reason"] == "NO_SOURCE_DATA"
