from __future__ import annotations

import pandas as pd


INVESTMENT_BANK_REVENUE_LABELS = [
    "NetRevenues",
    "Net revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "TotalNetRevenues",
    "NonInterestRevenue",
    "TotalRevenues",
    "Revenues",
]


def _extract_revenue_investment_bank(raw_sec: dict) -> float | None:
    for label in INVESTMENT_BANK_REVENUE_LABELS:
        if label in (raw_sec or {}):
            val = raw_sec.get(label)
            if val is not None and not pd.isna(val):
                return float(val)
    nii = (raw_sec or {}).get("NetInterestIncome", 0) or 0
    nir = (raw_sec or {}).get("NonInterestRevenue", 0) or 0
    if (nii + nir) > 0:
        return float(nii) + float(nir)
    return None

