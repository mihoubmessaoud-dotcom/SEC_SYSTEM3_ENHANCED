from __future__ import annotations

from core.export_concept_filters import filter_raw_by_year_concepts


def test_interest_expense_anchor_suppresses_subconcepts():
    concepts = [
        "Revenue",
        "InterestExpenseDebt",
        "InterestExpenseNonoperating",
        "InterestExpense",
        "NetIncomeLoss",
    ]
    out = filter_raw_by_year_concepts(concepts)
    assert "InterestExpense" in out
    assert "InterestExpenseDebt" not in out
    assert "InterestExpenseNonoperating" not in out
    assert "Revenue" in out
    assert "NetIncomeLoss" in out

