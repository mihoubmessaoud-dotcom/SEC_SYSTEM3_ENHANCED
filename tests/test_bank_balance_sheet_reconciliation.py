from __future__ import annotations


from modules.sec_fetcher import SECDataFetcher


def test_reconcile_balance_sheet_totals_overrides_assets_and_liabilities_when_1000x_off():
    f = SECDataFetcher()
    layer = {
        2017: {
            # Wrong-scale anchors (million labeled but actually billions-scale)
            "Assets": 2281.234,
            "Liabilities": 2014.088,
            "StockholdersEquity": 267146.0,
            # Correct total assets scale appears under liabilities+equity key
            "Total liabilities and shareholders’ equity": 2_281_234.0,
        }
    }
    out, diag = f._reconcile_balance_sheet_totals(layer)
    assert "2017" in diag
    assert abs(out[2017]["Assets"] - 2_281_234.0) < 1e-6
    # liabilities = total - equity
    assert abs(out[2017]["Liabilities"] - (2_281_234.0 - 267_146.0)) < 1e-6


def test_cache_normalization_applies_balance_reconciliation_and_recomputes_ratios():
    f = SECDataFetcher()
    cached = {
        "success": True,
        "company_info": {"ticker": "BAC", "name": "BANK", "sic": "6021", "sic_description": "Banks"},
        "period": "2015-2025",
        "data_by_year": {
            2017: {
                "Assets": 2281.234,
                "Liabilities": 2014.088,
                "StockholdersEquity": 267146.0,
                "NetIncomeLoss": 18232.0,
                "Total liabilities and shareholders’ equity": 2_281_234.0,
            }
        },
        "financial_ratios": {2017: {"roa": 8.15}},
        "strategic_analysis": {},
        "sector_gating": {"profile": "bank", "sub_profile": "commercial_bank"},
        "data_layers": {
            "layer1_by_year": {
                2017: {
                    "Assets": 2281.234,
                    "Liabilities": 2014.088,
                    "StockholdersEquity": 267146.0,
                    "NetIncomeLoss": 18232.0,
                    "Total liabilities and shareholders’ equity": 2_281_234.0,
                }
            },
            "layer2_by_year": {},
            "layer3_by_year": {},
            "layer4_by_year": {},
        },
    }
    normalized = f._normalize_cached_fetch_result(cached)
    l1 = (normalized.get("data_layers", {}) or {}).get("layer1_by_year", {}) or {}
    assert abs(l1[2017]["Assets"] - 2_281_234.0) < 1e-6
    # ROA should no longer be absurd (net_income/assets ~ 0.008)
    rr = (normalized.get("financial_ratios", {}) or {}).get(2017, {}) or {}
    roa = rr.get("roa")
    assert roa is None or float(roa) < 0.5
