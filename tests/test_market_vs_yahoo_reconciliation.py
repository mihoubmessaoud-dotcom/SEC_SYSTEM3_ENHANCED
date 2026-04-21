from __future__ import annotations


def test_market_vs_yahoo_reconciliation_overrides_extreme_divergence():
    # Local unit test: do not hit network. We validate the reconciliation policy only.
    from modules.sec_fetcher import SECDataFetcher

    fetcher = SECDataFetcher(user_agent_email="test@example.com")

    market_by_year = {
        2024: {
            "market:price": 1463.65,
            "market:market_cap": 6_337_604.0,  # million USD in system base (already scaled)
        }
    }
    yahoo_by_year = {
        2024: {
            "yahoo:price": 20.05,
            "yahoo:market_cap": 86_816.5,  # million USD
        }
    }

    fixed, diag = fetcher._reconcile_market_layer_against_yahoo(market_by_year, yahoo_by_year)

    assert fixed[2024]["market:price"] == yahoo_by_year[2024]["yahoo:price"]
    assert fixed[2024]["market:market_cap"] == yahoo_by_year[2024]["yahoo:market_cap"]
    assert "2024" in diag
    assert "price_override" in diag["2024"]
    assert "market_cap_override" in diag["2024"]


def test_market_vs_yahoo_reconciliation_derives_shares_when_missing():
    # Local unit test: validate shares derivation from market_cap/price.
    from modules.sec_fetcher import SECDataFetcher

    fetcher = SECDataFetcher(user_agent_email="test@example.com")

    market_by_year = {
        2021: {
            "market:price": 66.541,
            "market:market_cap": 220_116.187289,  # million USD
            "market:shares_outstanding": None,
        }
    }
    yahoo_by_year = {2021: {}}

    fixed, diag = fetcher._reconcile_market_layer_against_yahoo(market_by_year, yahoo_by_year)

    sh = fixed[2021].get("market:shares_outstanding")
    assert sh is not None
    assert 3_000.0 < float(sh) < 4_000.0
    assert "2021" in diag
    assert "shares_derived" in diag["2021"]


def test_market_vs_yahoo_reconciliation_derives_shares_when_nan():
    from modules.sec_fetcher import SECDataFetcher

    fetcher = SECDataFetcher(user_agent_email="test@example.com")
    market_by_year = {
        2021: {
            "market:price": 66.541,
            "market:market_cap": 220_116.187289,
            "market:shares_outstanding": float("nan"),
        }
    }
    fixed, diag = fetcher._reconcile_market_layer_against_yahoo(market_by_year, {2021: {}})
    sh = fixed[2021].get("market:shares_outstanding")
    assert sh is not None
    assert 3_000.0 < float(sh) < 4_000.0
    assert "2021" in diag
