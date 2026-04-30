from core.cross_layer_resolver import (
    pick_cash_million,
    pick_total_debt_million_including_leases,
    derive_enterprise_value_million,
)


def test_ev_corrected_when_cash_only_in_layer1_sec():
    years = [2020]
    data_by_year = {2020: {"Revenues": 1000.0}}
    layer1_by_year = {2020: {"CashAndCashEquivalentsAtCarryingValue": 500_000_000.0}}  # absolute USD
    sec_payload = {"periods": {"2020": {"facts": {}}}}

    cash = pick_cash_million(year=2020, data_by_year=data_by_year, layer1_by_year=layer1_by_year, sec_payload=sec_payload)
    assert cash.value_million == 500.0
    assert cash.source.startswith("SEC_LAYER1:")

    ev = derive_enterprise_value_million(market_cap_m=10000.0, total_debt_m=2000.0, cash_m=cash.value_million)
    assert ev == 11500.0

    # Stale EV overwrite rule (same as quality gate): replace if ratio is wildly off.
    stale = 800.0
    ratio = abs(stale) / abs(ev)
    assert ratio < 0.2


def test_cash_pick_supports_string_year_keys():
    data_by_year = {}
    layer1_by_year = {"2020": {"CashAndCashEquivalentsAtCarryingValue": 500_000_000.0}}
    sec_payload = {"periods": {"2020": {"facts": {}}}}
    cash = pick_cash_million(year=2020, data_by_year=data_by_year, layer1_by_year=layer1_by_year, sec_payload=sec_payload)
    assert cash.value_million == 500.0
    assert cash.source.startswith("SEC_LAYER1:")


def test_total_debt_includes_leases_prefers_combined_concept():
    # Debt+leases should be taken from DebtAndCapitalLeaseObligations if present.
    data_by_year = {2021: {}}
    layer1_by_year = {2021: {"DebtAndCapitalLeaseObligations": 3_000_000_000.0}}  # absolute USD
    sec_payload = {"periods": {"2021": {"facts": {}}}}

    td = pick_total_debt_million_including_leases(
        year=2021, data_by_year=data_by_year, layer1_by_year=layer1_by_year, sec_payload=sec_payload
    )
    assert td.value_million == 3000.0
    assert "DebtAndCapitalLeaseObligations" in td.source


def test_total_debt_includes_leases_totaldebt_plus_lease_liabilities():
    # If only TotalDebt exists plus separate leases, include both.
    data_by_year = {2022: {}}
    layer1_by_year = {
        2022: {
            "TotalDebt": 1_500_000_000.0,
            "LeaseLiabilitiesCurrent": 100_000_000.0,
            "LeaseLiabilitiesNoncurrent": 400_000_000.0,
        }
    }
    sec_payload = {"periods": {"2022": {"facts": {}}}}

    td = pick_total_debt_million_including_leases(
        year=2022, data_by_year=data_by_year, layer1_by_year=layer1_by_year, sec_payload=sec_payload
    )
    assert td.value_million == 2000.0
    assert td.source.endswith("+LEASES")
