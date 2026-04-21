from __future__ import annotations

from modules.canonical_classification import (
    build_canonical_classification,
    canonical_sector_gating_from_classification,
)


def test_nvda_classifies_as_semiconductors():
    cls = build_canonical_classification(
        ticker="NVDA",
        company_name="NVIDIA Corporation",
        sic="3674",
        naics="334413",
        sic_description="Semiconductor and Related Devices",
        sector_profile_hint="industrial",
        sub_sector_profile_hint="industrial",
        institutional_primary_profile="industrial",
        institutional_diag={},
    ).to_dict()
    assert cls["sector_family"] == "semiconductors"
    assert cls["sector_template"] == "semiconductor"
    assert cls["peer_group"] in {"semiconductor_fabless", "semiconductor_diversified"}
    assert cls["classification_confidence"] >= 0.90
    sg = canonical_sector_gating_from_classification(cls)
    assert sg["profile"] == "technology"
    assert sg["sub_profile"].startswith("semiconductor")


def test_amd_classifies_as_semiconductors():
    cls = build_canonical_classification(
        ticker="AMD",
        company_name="Advanced Micro Devices, Inc.",
        sic="3674",
        naics="334413",
        sic_description="Semiconductors",
        sector_profile_hint="technology",
        sub_sector_profile_hint="technology",
        institutional_primary_profile="industrial",
        institutional_diag={},
    ).to_dict()
    assert cls["sector_family"] == "semiconductors"
    assert cls["peer_group"].startswith("semiconductor")


def test_jpm_bac_wfc_classify_as_banks():
    for t in ("JPM", "BAC", "WFC"):
        cls = build_canonical_classification(
            ticker=t,
            company_name=t,
            sic="6021",
            naics="522110",
            sic_description="National Commercial Banks",
            sector_profile_hint="industrial",
            sub_sector_profile_hint="industrial",
            institutional_primary_profile="industrial",
            institutional_diag={},
        ).to_dict()
        assert cls["entity_type"] == "bank"
        assert cls["sector_family"] == "banks"
        assert cls["peer_group"] in {"commercial_bank", "investment_bank"}
        sg = canonical_sector_gating_from_classification(cls)
        assert sg["profile"] == "bank"
        assert sg["sub_profile"].endswith("_bank") or sg["sub_profile"] in {"commercial_bank", "investment_bank"}


def test_unknown_never_defaults_to_industrial():
    cls = build_canonical_classification(
        ticker="ZZZZ",
        company_name="Unknown Co",
        sic="",
        naics="",
        sic_description="",
        sector_profile_hint="industrial",
        sub_sector_profile_hint="",
        institutional_primary_profile="unknown",
        institutional_diag={},
    ).to_dict()
    assert cls["sector_family"] == "unknown"
    assert cls["sector_template"] == "unknown"
    assert cls["peer_group"] == "unknown"

