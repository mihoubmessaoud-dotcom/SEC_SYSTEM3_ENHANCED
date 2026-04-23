from __future__ import annotations


def test_bank_interest_expense_prefers_total_over_debt_subset_when_both_present():
    """
    Regression guard:
    Some banks expose both InterestExpenseDebt (subset) and InterestExpense (total).
    The resolver must not blindly pick the smaller subset for banks, because that
    inflates derived Net Interest Income and NIM.
    """
    from modules.sec_fetcher import SECDataFetcher

    f = SECDataFetcher(user_agent_email="test@example.com")

    row = {
        # Anchors for plausibility scoring
        "InterestAndDividendIncomeOperating": 90_000.0,  # USDm
        "Assets": 2_000_000.0,  # USDm
        # Competing candidates:
        "InterestExpenseDebt": 1_200.0,  # too small (subset)
        "InterestExpense": 40_000.0,  # plausible total
    }

    out = f._resolve_interest_expense_fact(row=row, year=2024, is_bank=True)
    assert out.get("status") == "COMPUTED"
    assert abs(float(out.get("value") or 0.0) - 40_000.0) < 1e-6

