from __future__ import annotations

from typing import Iterable, List


def filter_raw_by_year_concepts(concepts: Iterable[str]) -> List[str]:
    """
    Raw_by_Year is a canonical analyst-facing view. When both a total/anchor concept and
    more granular sub-concepts exist, prefer the anchor to avoid broken time-series with NaNs.

    This does NOT delete data; granular concepts remain available in Layer1_Raw_SEC.
    """
    items = [str(c) for c in (concepts or []) if str(c).strip()]
    s = set(items)

    # If total interest expense exists, suppress sub-variants that split by classification
    # (Debt/Nonoperating/etc.), because they often appear only in some years.
    if "InterestExpense" in s:
        out = []
        for c in items:
            if c == "InterestExpense":
                out.append(c)
                continue
            if c.startswith("InterestExpense"):
                continue
            out.append(c)
        items = out

    return items

