from modules.data_correction_engine import DataCorrectionEngine


def test_does_not_fill_missing_values() -> None:
    engine = DataCorrectionEngine()
    out = engine.correct_dataset(
        {
            "revenue": {
                2023: 120_000,
                2024: None,
                2025: 140_000,
            }
        }
    )
    corrected = out["corrected_metrics"]["revenue"]
    assert corrected[2024] is None


def test_scale_fix_logs_correction() -> None:
    engine = DataCorrectionEngine()
    out = engine.correct_dataset(
        {
            "revenue": {
                2024: 270_000_000,  # likely unit slip
                2025: 300_000_000,
            }
        }
    )
    corrected = out["corrected_metrics"]["revenue"]
    assert corrected[2024] == 270_000
    assert corrected[2025] == 300_000
    assert len(out["corrections"]) >= 1
    reasons = [c["reason"] for c in out["corrections"]]
    assert any(r.startswith("SCALE_UNIT_FIX_DIV_") for r in reasons)
    assert "timestamp_utc" in out["corrections"][0]


def test_jump_fix_logs_correction_when_unit_related() -> None:
    engine = DataCorrectionEngine(jump_ratio_threshold=8.0)
    out = engine.correct_dataset(
        {
            "market_cap": {
                2023: 120_000,
                2024: 130_000_000,  # unit jump
            }
        }
    )
    corrected = out["corrected_metrics"]["market_cap"]
    assert corrected[2024] == 130_000
    assert len(out["corrections"]) >= 1


def test_abnormal_jump_flagged_when_no_safe_fix() -> None:
    engine = DataCorrectionEngine(jump_ratio_threshold=4.0)
    out = engine.correct_dataset(
        {
            "leverage": {
                2023: 1.0,
                2024: 20.0,  # abnormal for this threshold; no deterministic unit fix
            }
        }
    )
    flags = out["flags"]
    assert any(f["reason"] == "ABNORMAL_JUMP_NO_SAFE_UNIT_FIX" for f in flags)
    assert "timestamp_utc" in flags[0]


def test_integration_with_financial_system_keeps_output_contract() -> None:
    from modules.financial_analysis_system import FinancialAnalysisSystem

    system = FinancialAnalysisSystem()
    raw = {
        "gross_margin": {2023: 0.62, 2024: 0.64, 2025: 0.66},
        "capex_to_revenue": {2023: 0.08, 2024: 0.07, 2025: 0.06},
        "rd_to_revenue": {2023: 0.20, 2024: 0.19, 2025: 0.18},
        "leverage": {2023: 0.45, 2024: 0.40, 2025: 0.35},
        "roic": {2023: 0.20, 2024: 0.24, 2025: 0.28},
        "operating_margin": {2023: 0.24, 2024: 0.26, 2025: 0.29},
        "revenue_growth": {2023: 0.12, 2024: 0.13, 2025: 0.14},
        "net_income_growth": {2023: 0.10, 2024: 0.11, 2025: 0.12},
        "asset_turnover": {2023: 0.80, 2024: 0.83, 2025: 0.85},
        "interest_coverage": {2023: 18.0, 2024: 20.0, 2025: 22.0},
        "altman_z": {2023: 3.2, 2024: 3.4, 2025: 3.6},
    }
    out = system.analyze("AMD", raw)
    assert "data_integrity" in out
    assert "corrections" in out["data_integrity"]
    assert "correction_flags" in out["data_integrity"]


def test_log_summary_available() -> None:
    engine = DataCorrectionEngine()
    out = engine.correct_dataset(
        {
            "revenue": {
                2024: 270_000_000,
                2025: 300_000_000,
            }
        }
    )
    summary = out.get("log_summary", {})
    assert summary.get("total_corrections", 0) >= 1
    assert "total_flags" in summary
