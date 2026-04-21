from __future__ import annotations

from modules.sec_fetcher import SECDataFetcher


def test_layer_shares_harmonizer_scales_raw_shares_to_million():
    f = SECDataFetcher()
    layer = {
        2015: {
            # raw shares
            "market:shares_outstanding": 24_300_000_000.0,
            # anchors (usd_million and usd)
            "market:price": 0.824,
            "market:market_cap": 20_023.200023,
        },
        2024: {
            # already shares_million
            "market:shares_outstanding": 24_640.0,
            "market:price": 134.29,
            "market:market_cap": 3_308_905.4346,
        },
    }
    out, diag = f._harmonize_layer_shares_units_to_million(layer, layer_name="TEST")
    assert "2015" in diag, "expected diagnostic entry for scaled year"
    assert abs(out[2015]["market:shares_outstanding"] - 24300.0) < 1e-9
    assert out[2015]["market:shares_outstanding"] <= 1e9
    assert abs(out[2024]["market:shares_outstanding"] - 24640.0) < 1e-9
    assert out[2024]["market:shares_outstanding"] <= 1e9


def test_cached_fetch_normalization_never_leaks_raw_share_units():
    f = SECDataFetcher()
    cached = {
        "success": True,
        "company_info": {"ticker": "NVDA", "name": "NVIDIA CORP", "sic": "3674", "sic_description": "Semiconductors"},
        "period": "2015-2025",
        "data_by_year": {2015: {}},
        "financial_ratios": {2015: {}},
        "sector_gating": {"profile": "technology", "sub_profile": "semiconductor_fabless"},
        "data_layers": {
            "layer1_by_year": {2015: {}},
            "layer2_by_year": {
                2015: {
                    "market:shares_outstanding": 24_300_000_000.0,
                    "market:price": 0.824,
                    "market:market_cap": 20_023.200023,
                }
            },
            "layer4_by_year": {
                2015: {
                    "yahoo:shares_outstanding": 24_300_000_000.0,
                    "yahoo:price": 0.824,
                    "yahoo:market_cap": 20_023.200023,
                }
            },
        },
    }
    normalized = f._normalize_cached_fetch_result(cached)
    l2 = (normalized.get("data_layers", {}) or {}).get("layer2_by_year", {}) or {}
    l4 = (normalized.get("data_layers", {}) or {}).get("layer4_by_year", {}) or {}
    assert l2[2015]["market:shares_outstanding"] <= 1e9
    assert abs(l2[2015]["market:shares_outstanding"] - 24300.0) < 1e-6
    assert l4[2015]["yahoo:shares_outstanding"] <= 1e9
    assert abs(l4[2015]["yahoo:shares_outstanding"] - 24300.0) < 1e-6

