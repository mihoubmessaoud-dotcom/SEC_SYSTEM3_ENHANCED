import json

from core.audit_pack import build_institutional_audit_pack


def test_audit_pack_passes_when_identities_hold():
    data_by_year = {
        "2025": {"StockholdersEquity": 1000.0},
    }
    financial_ratios = {
        "2025": {
            # Canonical units: usd_million and shares_million
            "market_cap": 2000.0,
            "shares_outstanding": 100.0,
            "eps_basic": 1.0,
            "pe_ratio": 20.0,  # price=20, eps=1 => pe=20
            "book_value_per_share": 10.0,  # price/bvps=2
            "pb_ratio": 2.0,  # market_cap/equity=2 and price/bvps=2
        }
    }

    pack = build_institutional_audit_pack(
        ticker="TST",
        period="2015-2025",
        data_by_year=data_by_year,
        financial_ratios=financial_ratios,
        canonical_money_unit="usd_million",
        canonical_shares_unit="shares_million",
    )
    assert pack["schema"] == "institutional_audit_pack_v1"
    checks = {c["name"]: c for c in pack["by_year"]["2025"]["checks"]}
    assert checks["pe_identity"]["status"] == "PASS"
    assert checks["pb_identity_equity"]["status"] == "PASS"
    assert checks["pb_identity_bvps"]["status"] == "PASS"
    assert pack["by_year"]["2025"]["issues"] == []
    # Ensure pack remains JSON-serializable for audit writing in runtime.
    payload = json.loads(json.dumps(pack))
    assert payload["ticker"] == "TST"


def test_audit_pack_flags_scale_suspects():
    data_by_year = {"2025": {"StockholdersEquity": 1000.0}}
    financial_ratios = {
        "2025": {
            "market_cap": 2000.0,
            "shares_outstanding": 100.0,
            "eps_basic": 1.0,
            "pe_ratio": 20_000_000.0,  # absurd scale vs implied 20
            "book_value_per_share": 10.0,
            "pb_ratio": 2.0,
        }
    }

    pack = build_institutional_audit_pack(
        ticker="TST",
        period="2015-2025",
        data_by_year=data_by_year,
        financial_ratios=financial_ratios,
    )
    issues = pack["by_year"]["2025"]["issues"]
    codes = {i["code"] for i in issues}
    assert "UNIT_SCALE_SUSPECT_PE" in codes or "PE_IDENTITY_FAIL" in codes
