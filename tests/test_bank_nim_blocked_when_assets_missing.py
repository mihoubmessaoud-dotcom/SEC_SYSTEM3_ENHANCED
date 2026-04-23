from __future__ import annotations


def test_bank_nim_blocked_when_total_assets_anchor_missing_even_if_other_assets_present():
    """
    Regression guard:
    If total Assets anchor is missing for a bank year, NIM must be blocked (None),
    even if a partial bucket like OtherAssets exists (which must never be used as Assets).
    """
    from modules.institutional import InstitutionalFinancialIntelligenceEngine

    engine = InstitutionalFinancialIntelligenceEngine()
    meta = {
        "name": "Bank-like",
        "ticker": "BANKA",
        "cik": "0000",
        "sic": "6021",
        "naics": "522110",
        "filing_grade": "IN_RANGE_ANNUAL",
        "filing_in_range": True,
    }

    facts = {
        2024: {
            "NetInterestIncome": 600_000.0,
            "InterestAndDividendIncomeOperating": 1_200_000.0,
            "InterestExpense": 600_000.0,
            # Missing Assets / TotalAssets on purpose:
            "OtherAssets": 80_000_000.0,
            "Deposits": 50_000_000.0,
            "CommonEquityTier1CapitalRatio": 0.11,
            "NetIncomeLoss": 200_000.0,
        }
    }

    out = engine.run(meta, facts)
    dash = out["sector_ratio_dashboard"]
    # The classifier may abstain (unknown) on synthetic minimal fixtures,
    # but the ratio engine must still block NIM when total-assets anchor is missing.
    assert dash.loc[0, "net_interest_margin"] is None
