import main


def test_quality_gate_normalizes_fcf_units_to_million_usd():
    app = main.SECFinancialSystem.__new__(main.SECFinancialSystem)
    app.current_data = {
        "company_info": {"ticker": "AVGO"},
        "sector_gating": {"profile": "industrial", "blocked_ratios": []},
    }

    years = [2024]
    data_by_year = {2024: {}}
    ratios_by_year = {2024: {"free_cash_flow": 18_000_000_000.0}}  # absolute USD (should become ~18,000 million)
    data_layers = {
        "layer2_by_year": {
            2024: {
                "market:price": 100.0,
                "market:market_cap": 1_630_823.0,  # million USD
                "market:shares_outstanding": None,
            }
        },
        "layer4_by_year": {2024: {}},
    }

    app._apply_pre_export_quality_gate(
        years=years,
        data_by_year=data_by_year,
        ratios_by_year=ratios_by_year,
        data_layers=data_layers,
    )

    r = ratios_by_year[2024]
    fcf_m = float(r.get("free_cash_flow"))
    assert 10_000.0 < fcf_m < 30_000.0  # million USD
    fy = float(r.get("fcf_yield"))
    assert 0.0 < fy < 0.1
    fcfps = float(r.get("fcf_per_share"))
    assert 0.0 < fcfps < 50.0

