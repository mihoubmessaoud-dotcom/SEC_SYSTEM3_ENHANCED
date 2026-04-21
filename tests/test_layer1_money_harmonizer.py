from modules.sec_fetcher import SECDataFetcher


def test_money_harmonizer_scales_raw_usd_when_mixed_with_millions():
    f = SECDataFetcher()
    layer1 = {
        2016: {
            # raw USD (should become 1106.0 million and -51.0 million)
            "Revenues": 1_106_000_000.0,
            "NetIncomeLoss": -51_000_000.0,
            # already in USD_million
            "StockholdersEquity": 611.0,
            "Assets": 3_000.0,
            # non-monetary, must not be scaled
            "WeightedAverageNumberOfSharesOutstandingBasic": 931.0,
            "EarningsPerShareBasic": -0.6,
        }
    }
    out, diag = f._harmonize_layer1_money_units_to_million(layer1)
    assert diag["2016"]["scale_applied"] == 1_000_000.0
    assert abs(out[2016]["Revenues"] - 1106.0) < 1e-9
    assert abs(out[2016]["NetIncomeLoss"] - (-51.0)) < 1e-9
    assert out[2016]["StockholdersEquity"] == 611.0
    assert out[2016]["WeightedAverageNumberOfSharesOutstandingBasic"] == 931.0
    assert out[2016]["EarningsPerShareBasic"] == -0.6


def test_money_harmonizer_scales_single_raw_line_item_even_when_anchors_small():
    f = SECDataFetcher()
    layer1 = {
        2017: {
            # anchors already in USD_million
            "Revenues": 5253.0,
            "NetIncomeLoss": -33.0,
            "Assets": 4556.0,
            "StockholdersEquity": 1266.0,
            # non-anchor mistakenly in raw USD
            "CostOfRevenue": 653_000_000.0,
        }
    }
    out, diag = f._harmonize_layer1_money_units_to_million(layer1)
    assert diag["2017"]["scale_applied"] is None
    assert abs(out[2017]["CostOfRevenue"] - 653.0) < 1e-9
