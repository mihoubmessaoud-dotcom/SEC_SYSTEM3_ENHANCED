from __future__ import annotations


from modules.sec_fetcher import SECDataFetcher


def test_series_scale_harmonizer_scales_deposits_billion_to_million_when_mixed():
    f = SECDataFetcher()
    layer = {
        2015: {"Deposits": 1_197_259.0},  # correct USD_million
        2016: {"Deposits": 1_260.934},    # accidentally USD_billion
        2017: {"Deposits": 1_309.545},    # accidentally USD_billion
        2018: {"Deposits": 1_381_476.0},  # correct USD_million
    }
    out, diag = f._harmonize_layer1_series_scale_to_million(layer)
    assert "Deposits" in diag
    assert abs(out[2016]["Deposits"] - 1_260_934.0) < 1e-6
    assert abs(out[2017]["Deposits"] - 1_309_545.0) < 1e-6

