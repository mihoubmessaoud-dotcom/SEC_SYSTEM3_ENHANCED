def test_ev_corrected_after_market_cap_repair(monkeypatch):
    # Ensure export gate debug is off for clean issues list in tests.
    monkeypatch.setenv("EXPORT_GATE_DEBUG", "0")
    # Ensure EV backfill enabled (default).
    monkeypatch.setenv("ALLOW_EV_BACKFILL", "1")

    # Import lazily (main.py is huge); only need the method.
    from main import SECFinancialSystem

    # Build a lightweight instance without running the full Tk UI constructor.
    sys = SECFinancialSystem.__new__(SECFinancialSystem)
    sys.current_lang = "ar"
    sys.current_data = {
        "company_info": {"ticker": "NVDA"},
        "sector_gating": {},
        "source_layer_payloads": {"SEC": {}},
        # Hint to keep non-bank path.
        "sector_profile": "semiconductor",
        "sub_sector_profile": "semiconductor",
    }

    years = [2015]
    # Simulate layer2 having stale/tiny market_cap and stale EV, but with price+shares present.
    layer2_by_year = {
        2015: {
            "market:price": 0.824,
            # Split-adjusted shares (million) used elsewhere in the app for NVDA post-split history.
            "market:shares_outstanding": 24300.0,
            "market:market_cap": 0.8009280009269715,  # stale tiny (bug signature)
            "market:enterprise_value": 860.5219,  # stale EV computed off stale mcap
            "market:total_debt": 1356.375,
        }
    }
    # Provide SEC cash in Layer1.
    layer1_by_year = {
        2015: {
            "CashAndCashEquivalentsAtCarryingValue": 496.654,
            # Provide a share anchor so market_cap can be rebuilt from price * shares.
            # Use absolute shares; the gate will normalize to "million shares".
            "EntityCommonStockSharesOutstanding": 24_300_000_000,
        }
    }
    data_layers = {"layer2_by_year": layer2_by_year, "layer1_by_year": layer1_by_year, "layer4_by_year": {}}

    ratios_by_year = {2015: {}}
    data_by_year = {2015: dict(layer1_by_year[2015])}

    issues = SECFinancialSystem._apply_pre_export_quality_gate(
        sys,
        years=years,
        data_by_year=data_by_year,
        ratios_by_year=ratios_by_year,
        data_layers=data_layers,
    )

    # market_cap repaired from price * shares.
    assert layer2_by_year[2015]["market:market_cap"] > 1000
    # EV corrected using repaired market cap.
    ev = layer2_by_year[2015]["market:enterprise_value"]
    assert ev is not None
    assert abs(float(ev) - (20023.200023174286 + 1356.375 - 496.654)) < 1e-3
    # Issues include EV correction line.
    assert any("market:enterprise_value corrected" in str(x) for x in issues)
