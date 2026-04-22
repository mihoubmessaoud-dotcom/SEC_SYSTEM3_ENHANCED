from __future__ import annotations


from modules.ratio_source import UnifiedRatioSource


def test_bank_outlier_ldr_blocked_in_ratio_contract() -> None:
    rs = UnifiedRatioSource()
    # Minimal dataset: ratio engine may not build this ratio, so it will come from raw fallback.
    data_by_year = {2025: {}}
    ratios_by_year = {2025: {"loan_to_deposit_ratio": 38.2787}}
    rs.load("JPM", data_by_year, ratios_by_year, sector_profile="bank")
    c = rs.get_ratio_contract("JPM", 2025, "loan_to_deposit_ratio")
    assert c.get("status") == "NOT_COMPUTABLE"
    assert c.get("reason") == "OUT_OF_RANGE_UNTRUSTED"
    assert c.get("value") is None

