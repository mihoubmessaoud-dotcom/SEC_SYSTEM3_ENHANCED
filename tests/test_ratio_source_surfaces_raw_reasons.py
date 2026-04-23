from __future__ import annotations


def test_ratio_source_surfaces_raw_ratio_reasons_even_when_value_exists():
    from modules.ratio_source import UnifiedRatioSource

    rs = UnifiedRatioSource()

    data_by_year = {2024: {"Assets": 100.0}}
    ratios_by_year = {
        2024: {
            "bank_efficiency_ratio": 0.85,
            "_ratio_reasons": {"bank_efficiency_ratio": "NONINTEREST_EXPENSE_CONTAMINATED_INTEREST_EXPENSE"},
        }
    }

    rs.load("TST", data_by_year, ratios_by_year, sector_profile="bank")
    c = rs.get_ratio_contract("TST", 2024, "bank_efficiency_ratio")
    assert c.get("status") == "COMPUTED"
    assert c.get("reason") == "NONINTEREST_EXPENSE_CONTAMINATED_INTEREST_EXPENSE"
    assert int(c.get("reliability") or 0) <= 30
