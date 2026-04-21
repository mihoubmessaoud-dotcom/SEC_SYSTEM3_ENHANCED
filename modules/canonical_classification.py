from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class CanonicalClassification:
    """
    Single Source of Truth (SSOT) classification object.

    Required fields (per user directive):
    - entity_type
    - sector_family
    - sector_template
    - operating_sub_sector
    - peer_group
    """

    entity_type: str
    sector_family: str
    sector_template: str
    operating_sub_sector: str
    peer_group: str

    classification_confidence: float
    classification_source: str

    diagnostics: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # keep payload stable (explicit keys; avoid None)
        d["diagnostics"] = dict(self.diagnostics or {})
        return d


_SEMIS_OVERRIDES: Dict[str, Dict[str, str]] = {
    # Semiconductors (mandatory explicit fixes)
    "NVDA": {"operating_sub_sector": "semiconductor_fabless", "peer_group": "semiconductor_fabless"},
    "AMD": {"operating_sub_sector": "semiconductor_fabless", "peer_group": "semiconductor_fabless"},
    # INTC is an IDM structurally; keep peer_group aligned with semi universe for comparisons.
    "INTC": {"operating_sub_sector": "semiconductor_idm", "peer_group": "semiconductor_fabless"},
    # AVGO: diversified semis is acceptable per requirement; keep peers in semis universe.
    "AVGO": {"operating_sub_sector": "semiconductor_diversified", "peer_group": "semiconductor_fabless"},
}

_BANK_OVERRIDES: Dict[str, Dict[str, str]] = {
    # Banks (mandatory explicit fixes)
    "JPM": {"operating_sub_sector": "commercial_bank", "peer_group": "commercial_bank"},
    "BAC": {"operating_sub_sector": "commercial_bank", "peer_group": "commercial_bank"},
    "WFC": {"operating_sub_sector": "commercial_bank", "peer_group": "commercial_bank"},
}


def _parent_profile_from_sub_sector(sub_sector: str) -> str:
    s = str(sub_sector or "").strip().lower()
    if not s:
        return "unknown"
    if s.startswith("semiconductor") or s in {"software_saas", "hardware_platform"}:
        return "technology"
    if s.endswith("_bank") or s in {"commercial_bank", "investment_bank"}:
        return "bank"
    if s.startswith("insurance"):
        return "insurance"
    return "unknown"


def _sector_family_from_sub_sector(sub_sector: str, entity_type: str) -> Tuple[str, str]:
    """
    Returns (sector_family, sector_template) from sub-sector and entity_type.
    """
    et = str(entity_type or "").strip().lower()
    ss = str(sub_sector or "").strip().lower()
    if et == "bank":
        return "banks", "bank"
    if et == "insurance":
        return "insurance", "insurance"
    if et == "broker_dealer":
        return "broker_dealer", "broker_dealer"
    if ss.startswith("semiconductor"):
        return "semiconductors", "semiconductor"
    if ss in {"software_saas", "hardware_platform"}:
        return "technology", "technology"
    return "unknown", "unknown"


def build_canonical_classification(
    *,
    ticker: str,
    company_name: str = "",
    sic: str = "",
    naics: str = "",
    sic_description: str = "",
    # raw sector hints (must not be used as SSOT directly)
    sector_profile_hint: str = "",
    sub_sector_profile_hint: str = "",
    institutional_primary_profile: str = "",
    institutional_diag: Optional[Dict[str, Any]] = None,
) -> CanonicalClassification:
    """
    Build the canonical classification object.

    Notes:
    - Never defaults to 'industrial'. If we cannot classify with confidence, we return Unknown/Review.
    - Applies explicit overrides for semiconductors and banks per regression requirements.
    """
    t = str(ticker or "").upper().strip()
    name = str(company_name or "")
    diag: Dict[str, Any] = {
        "ticker": t,
        "company_name": name,
        "sic": str(sic or ""),
        "naics": str(naics or ""),
        "sic_description": str(sic_description or ""),
        "sector_profile_hint": str(sector_profile_hint or ""),
        "sub_sector_profile_hint": str(sub_sector_profile_hint or ""),
        "institutional_primary_profile": str(institutional_primary_profile or ""),
        "institutional_classifier_diagnostics": dict(institutional_diag or {}),
    }

    # A) Hard overrides (highest confidence)
    if t in _BANK_OVERRIDES:
        ss = _BANK_OVERRIDES[t]["operating_sub_sector"]
        pg = _BANK_OVERRIDES[t]["peer_group"]
        sf, st = _sector_family_from_sub_sector(ss, "bank")
        return CanonicalClassification(
            entity_type="bank",
            sector_family=sf,
            sector_template=st,
            operating_sub_sector=ss,
            peer_group=pg,
            classification_confidence=0.99,
            classification_source="ticker_override",
            diagnostics={**diag, "override": "bank"},
        )
    if t in _SEMIS_OVERRIDES:
        ss = _SEMIS_OVERRIDES[t]["operating_sub_sector"]
        pg = _SEMIS_OVERRIDES[t]["peer_group"]
        sf, st = _sector_family_from_sub_sector(ss, "operating_company")
        return CanonicalClassification(
            entity_type="operating_company",
            sector_family=sf,
            sector_template=st,
            operating_sub_sector=ss,
            peer_group=pg,
            classification_confidence=0.99,
            classification_source="ticker_override",
            diagnostics={**diag, "override": "semiconductors"},
        )

    # B) Infer entity_type from institutional profile (strict, fail-closed)
    inst = str(institutional_primary_profile or "").strip().lower()
    if inst in {"bank", "insurance", "broker_dealer"}:
        entity_type = inst
        entity_source = "institutional_classifier"
        entity_conf = 0.90 if inst != "unknown" else 0.55
    elif inst == "unknown":
        entity_type = "unknown"
        entity_source = "institutional_classifier_abstain"
        entity_conf = 0.45
    else:
        entity_type = "operating_company"
        entity_source = "institutional_classifier"
        entity_conf = 0.70

    # C) Use sub-sector hint when it is a known, structured profile (safe)
    ss_hint = str(sub_sector_profile_hint or "").strip().lower()
    ss: str = ss_hint or ""
    if ss in {"commercial_bank", "investment_bank"}:
        entity_type = "bank"
    if ss.startswith("insurance"):
        entity_type = "insurance"

    sf, st = _sector_family_from_sub_sector(ss, entity_type)

    # D) Confidence gating: avoid dangerous fallbacks
    # If we cannot determine sector_family/template beyond unknown, we should abstain.
    confidence = float(entity_conf)
    source = entity_source
    if sf == "unknown" and st == "unknown":
        # We explicitly avoid labeling as "industrial".
        confidence = min(confidence, 0.55)
        source = "abstain_unknown_sector"
        peer_group = "unknown"
        operating_sub_sector = ss or "unknown"
    else:
        # Structured sub-sectors boost confidence
        confidence = max(confidence, 0.75)
        source = f"{source}+sub_sector_profile"
        peer_group = ss or sf
        operating_sub_sector = ss or sf

    # E) If confidence is low, force Unknown/Review Required for sector fields (but keep entity_type if bank/insurance)
    if confidence < 0.60 and entity_type not in {"bank", "insurance", "broker_dealer"}:
        sf, st = "unknown", "unknown"
        peer_group = "unknown"
        operating_sub_sector = "unknown"
        source = "review_required_low_confidence"

    return CanonicalClassification(
        entity_type=entity_type,
        sector_family=sf,
        sector_template=st,
        operating_sub_sector=operating_sub_sector,
        peer_group=peer_group,
        classification_confidence=float(confidence),
        classification_source=source,
        diagnostics=diag,
    )


def canonical_sector_gating_from_classification(cls: Dict[str, Any]) -> Dict[str, Any]:
    """
    Backward-compatible sector_gating dict derived only from canonical classification SSOT.
    """
    cc = cls or {}
    entity_type = str(cc.get("entity_type") or "").strip().lower()
    sub = str(cc.get("operating_sub_sector") or cc.get("peer_group") or "").strip().lower()
    profile = _parent_profile_from_sub_sector(sub)
    if entity_type in {"bank", "insurance", "broker_dealer"}:
        profile = entity_type if entity_type != "broker_dealer" else "bank"
    if profile == "unknown" and entity_type == "operating_company" and sub.startswith("semiconductor"):
        profile = "technology"
    return {
        "profile": profile or "unknown",
        "sub_profile": sub or (profile or "unknown"),
    }
