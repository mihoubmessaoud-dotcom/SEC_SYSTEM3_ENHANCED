import os

import main


def _make_app(ticker: str, sector: str):
    app = main.SECFinancialSystem.__new__(main.SECFinancialSystem)
    app.current_data = {
        "company_info": {"ticker": ticker},
        "sector_gating": {"profile": sector, "blocked_ratios": []},
    }
    return app


def test_quality_gate_repairs_enterprise_value_unit_x1000_drift():
    app = _make_app("NVDA", "technology")
    years = [2024]
    data_by_year = {2024: {}}
    ratios_by_year = {2024: {"enterprise_value": 3312.68}}  # looks like billions but labeled as "million"
    data_layers = {
        "layer2_by_year": {2024: {"market:enterprise_value": 3_312_681.0}},  # million USD
        "layer4_by_year": {2024: {}},
    }

    app._apply_pre_export_quality_gate(years, data_by_year, ratios_by_year, data_layers)
    r = ratios_by_year[2024]
    assert abs(float(r["enterprise_value"]) - 3_312_681.0) < 1e-6
    assert "UNIT_REPAIR_X1000" in str(r.get("enterprise_value_source") or "")


def test_quality_gate_eps_prefers_sec_weighted_avg_shares_over_market_shares():
    old_env = os.environ.get("QUALITY_GATE_AGGRESSIVE")
    os.environ["QUALITY_GATE_AGGRESSIVE"] = "1"
    try:
        app = _make_app("AMD", "technology")
        years = [2021]
        # Net income in million USD.
        data_by_year = {
            2021: {
                "NetIncomeLoss": 1_000.0,
                # SEC weighted-average shares (absolute shares).
                "WeightedAverageNumberOfSharesOutstandingBasic": 100_000_000,
                # End-of-period shares (absolute shares) - should NOT be used for EPS.
                "CommonStockSharesOutstanding": 200_000_000,
            }
        }
        ratios_by_year = {
            2021: {
                "eps_basic": None,
            }
        }
        data_layers = {
            "layer2_by_year": {
                2021: {
                    "market:price": 100.0,
                    "market:pe_ratio": 10.0,  # implies EPS ~ 10
                    # Market shares outstanding (million shares) aligned to end shares.
                    "market:shares_outstanding": 200.0,
                }
            },
            "layer4_by_year": {2021: {}},
        }

        app._apply_pre_export_quality_gate(years, data_by_year, ratios_by_year, data_layers)
        eps = ratios_by_year[2021].get("eps_basic")
        assert eps is not None
        # Expected EPS = 1,000 (million) / 100 (million shares) = 10 USD/share.
        assert abs(float(eps) - 10.0) < 1e-6
    finally:
        if old_env is None:
            os.environ.pop("QUALITY_GATE_AGGRESSIVE", None)
        else:
            os.environ["QUALITY_GATE_AGGRESSIVE"] = old_env


def test_quality_gate_does_not_clamp_net_margin_to_operating_margin():
    app = _make_app("AMD", "technology")
    years = [2022]
    data_by_year = {2022: {}}
    ratios_by_year = {
        2022: {
            "gross_margin": 0.50,
            "operating_margin": 0.20,
            "net_margin": 0.30,  # can exceed operating margin
        }
    }
    data_layers = {"layer2_by_year": {2022: {}}, "layer4_by_year": {2022: {}}}

    app._apply_pre_export_quality_gate(years, data_by_year, ratios_by_year, data_layers)
    assert abs(float(ratios_by_year[2022]["net_margin"]) - 0.30) < 1e-12


def test_quality_gate_reconstructs_total_assets_when_assets_is_current_assets():
    app = _make_app("NVDA", "technology")
    years = [2025]
    data_by_year = {
        2025: {
            "Assets": 100.0,  # wrong: actually current assets
            "AssetsCurrent": 100.0,
            "AssetsNoncurrent": 900.0,
        }
    }
    ratios_by_year = {2025: {}}
    data_layers = {"layer2_by_year": {2025: {}}, "layer4_by_year": {2025: {}}}

    app._apply_pre_export_quality_gate(years, data_by_year, ratios_by_year, data_layers)
    assert abs(float(data_by_year[2025]["Assets"]) - 1000.0) < 1e-12


def test_quality_gate_bank_ldr_proxy_is_forbidden_when_anchors_missing():
    app = _make_app("BAC", "bank")
    years = [2020]
    data_by_year = {2020: {}}
    ratios_by_year = {2020: {"loan_to_deposit_ratio": 4.2}}
    data_layers = {"layer2_by_year": {2020: {}}, "layer4_by_year": {2020: {}}}

    app._apply_pre_export_quality_gate(years, data_by_year, ratios_by_year, data_layers)
    r = ratios_by_year[2020]
    assert r.get("loan_to_deposit_ratio") in (None, 0)
    assert "MISSING_BANK_LOANS_OR_DEPOSITS" in str(r.get("loan_to_deposit_ratio_source") or "")

