import main


def test_quality_gate_derives_shares_outstanding_in_millions():
    app = main.SECFinancialSystem.__new__(main.SECFinancialSystem)
    app.current_data = {
        "company_info": {"ticker": "AVGO"},
        "sector_gating": {"profile": "industrial", "blocked_ratios": []},
    }

    years = [2021]
    data_by_year = {2021: {}}
    ratios_by_year = {2021: {"free_cash_flow": 1000.0}}
    data_layers = {
        "layer2_by_year": {
            2021: {
                "market:price": 66.541,
                "market:market_cap": 220_116.0,  # million USD
                "market:shares_outstanding": None,
            }
        },
        "layer4_by_year": {2021: {}},
    }

    issues = app._apply_pre_export_quality_gate(
        years=years,
        data_by_year=data_by_year,
        ratios_by_year=ratios_by_year,
        data_layers=data_layers,
    )

    assert issues, "Expected at least one quality-gate issue log for derived shares."
    sh_m = data_layers["layer2_by_year"][2021].get("market:shares_outstanding")
    assert sh_m is not None
    assert 3_000.0 < float(sh_m) < 4_000.0, "Expected derived shares ~ 3.3B shares -> 3300 million shares."
    assert float(sh_m) < 10_000_000.0, "Guardrail: must not store absolute shares into Layer2."

    r = ratios_by_year[2021]
    assert 3_000.0 < float(r.get("shares_outstanding")) < 4_000.0

