from __future__ import annotations


def test_bank_nim_is_net_based_and_ldr_never_uses_asset_proxy_when_loans_missing():
    """
    Regression guard:
    - Bank NIM must be computed from *net* interest income / avg assets (not gross interest income).
    - Bank LDR must be blocked when Loans anchor is missing; it must never be fabricated from Assets/Cash proxies.
    """
    from modules.institutional import InstitutionalFinancialIntelligenceEngine

    engine = InstitutionalFinancialIntelligenceEngine()

    meta = {
        "name": "Bank-like",
        "ticker": "BANKX",
        "cik": "0000",
        "sic": "6021",
        "naics": "522110",
        "filing_grade": "IN_RANGE_ANNUAL",
        "filing_in_range": True,
    }

    facts = {
        2023: {
            # Net Interest Income present => should drive NIM.
            "NetInterestIncome": 30_000_000,
            "NetIncomeLoss": 10_000_000,
            "Assets": 1_000_000_000,
            "Deposits": 800_000_000,
            # Loans concept exists but missing value => LDR must remain blocked (None).
            "LoansReceivable": None,
            # Proxies that *previously* caused fabricated LDR:
            "CashAndCashEquivalentsAtCarryingValue": 50_000_000,
            "Liabilities": 900_000_000,
        }
    }

    out = engine.run(meta, facts)
    dash = out["sector_ratio_dashboard"]

    # Bank profile must be selected for a bank SIC.
    assert str(dash.loc[0, "profile"]) == "bank"

    nim = dash.loc[0, "net_interest_margin"]
    assert nim is not None
    nim_f = float(nim)
    assert 0.0 <= nim_f <= 0.20
    assert abs(nim_f - 0.03) < 1e-6

    ldr = dash.loc[0, "loan_to_deposit_ratio"]
    # LDR should be blocked (missing loans) and must not be fabricated.
    assert ldr is None or (float(ldr) != float(ldr))  # None or NaN

    rexp = out.get("ratio_explanations")
    assert rexp is not None
    ldr_rows = rexp[rexp["ratio_id"] == "loan_to_deposit_ratio"]
    assert len(ldr_rows.index) > 0
    # Must explicitly note that required loans input is missing.
    assert any("missing_required_input:BS.LOANS" in (r or []) for r in ldr_rows["reasons"].tolist())

