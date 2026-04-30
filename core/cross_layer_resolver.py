from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import math
import re


def extract_num(v: Any) -> Optional[float]:
    """
    Robust numeric extractor for mixed sources:
    - numbers
    - strings with comma/arabic separators
    - value-objects like {"value": 123, "unit": "USD"}
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            fv = float(v)
        except Exception:
            return None
        if not math.isfinite(fv):
            return None
        return fv
    if isinstance(v, dict):
        for key in ("value", "val", "amount"):
            if key in v:
                return extract_num(v.get(key))
        try:
            if len(v) == 1:
                return extract_num(next(iter(v.values())))
        except Exception:
            return None
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null", "n/a", "—", "-"}:
        return None
    # Extract first numeric token.
    m = re.search(r"[-+]?\d[\d\s.,\u066b\u066c]*", s)
    token = m.group(0).strip() if m else ""
    if not token:
        return None
    token = token.replace("\u00a0", "").replace(" ", "")
    # Arabic separators: \u066c thousands, \u066b decimal
    token = token.replace("\u066c", "").replace("\u066b", ".")
    # Normalize decimal/thousand separators.
    if "," in token and "." in token:
        token = token.replace(",", "")
    elif "," in token and "." not in token:
        token = token.replace(",", ".")
    try:
        fv = float(token)
    except Exception:
        return None
    if not math.isfinite(fv):
        return None
    return fv


def as_million_usd(v: Any) -> Optional[float]:
    """
    Normalize USD to USD_million when magnitude indicates absolute USD.
    If already in million, keep unchanged.
    """
    fv = extract_num(v)
    if fv is None:
        return None
    # Threshold chosen to avoid treating million-scale values (e.g., 200,000m) as absolute USD.
    if abs(fv) >= 100_000_000.0:
        return fv / 1_000_000.0
    return fv


@dataclass(frozen=True)
class PickResult:
    value_million: Optional[float]
    source: str
    confidence: int


def _pick_from_layer1(layer1_by_year: Dict[int, Dict[str, Any]], year: int, tags: Tuple[str, ...]) -> Optional[Tuple[str, Any]]:
    # Some pipelines store years as strings; support both.
    row = (layer1_by_year or {}).get(year)
    if row is None:
        row = (layer1_by_year or {}).get(str(year))
    row = row or {}
    for t in tags:
        if t in row:
            return t, row.get(t)
    return None


def _pick_from_sec_payload(sec_payload: Dict[str, Any], year: int, tags: Tuple[str, ...]) -> Optional[Tuple[str, Any]]:
    try:
        periods = (sec_payload or {}).get("periods") or {}
        facts = ((periods.get(str(year)) or {}).get("facts") or {})
        for t in tags:
            obj = facts.get(t)
            if isinstance(obj, dict) and ("value" in obj or "val" in obj):
                return t, obj
    except Exception:
        return None
    return None


def pick_cash_million(
    *,
    year: int,
    data_by_year: Dict[int, Dict[str, Any]],
    layer1_by_year: Dict[int, Dict[str, Any]],
    sec_payload: Dict[str, Any],
) -> PickResult:
    # Best available per user choice; confidence drops when we must include short-term investments.
    preferred = (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashAndCashEquivalents",
        "CashCashEquivalentsAndShortTermInvestments",
    )
    preferred_payload = (
        "us-gaap:CashAndCashEquivalentsAtCarryingValue",
        "us-gaap:CashAndCashEquivalents",
        "us-gaap:CashCashEquivalentsAndShortTermInvestments",
    )

    hit = _pick_from_layer1(layer1_by_year, year, preferred)
    if hit:
        tag, raw = hit
        v = as_million_usd(raw)
        if v is not None:
            conf = 95 if tag != "CashCashEquivalentsAndShortTermInvestments" else 85
            return PickResult(v, f"SEC_LAYER1:{tag}", conf)

    hit = _pick_from_sec_payload(sec_payload, year, preferred_payload)
    if hit:
        tag, raw = hit
        v = as_million_usd(raw)
        if v is not None:
            conf = 95 if tag.endswith("CashAndCashEquivalentsAtCarryingValue") or tag.endswith("CashAndCashEquivalents") else 85
            return PickResult(v, f"SEC_PAYLOAD:{tag}", conf)

    raw_row = (data_by_year or {}).get(year, {}) or {}
    for tag in preferred:
        if tag in raw_row:
            v = as_million_usd(raw_row.get(tag))
            if v is not None:
                conf = 75 if tag != "CashCashEquivalentsAndShortTermInvestments" else 65
                return PickResult(v, f"DATA_BY_YEAR:{tag}", conf)

    # Fallback: allow English label variants if present (very low confidence).
    for tag in ("Cash and Cash Equivalents",):
        if tag in raw_row:
            v = as_million_usd(raw_row.get(tag))
            if v is not None:
                return PickResult(v, f"DATA_BY_YEAR:{tag}", 55)
    return PickResult(None, "MISSING", 0)


def pick_total_debt_million_including_leases(
    *,
    year: int,
    data_by_year: Dict[int, Dict[str, Any]],
    layer1_by_year: Dict[int, Dict[str, Any]],
    sec_payload: Dict[str, Any],
) -> PickResult:
    """
    Total debt policy: include leases always.
    Preference:
      1) DebtAndCapitalLeaseObligations (best)
      2) TotalDebt (+ lease liabilities if separately available)
      3) Sum debt components + lease liabilities (lower confidence)
    """
    # 1) Combined concept (best)
    combined = (
        "DebtAndCapitalLeaseObligations",
        "DebtAndCapitalLeaseObligationsIncludingCurrentPortion",
        "LongTermDebtAndCapitalLeaseObligations",
    )
    combined_payload = tuple(f"us-gaap:{t}" for t in combined)

    hit = _pick_from_layer1(layer1_by_year, year, combined)
    if hit:
        tag, raw = hit
        v = as_million_usd(raw)
        if v is not None:
            return PickResult(v, f"SEC_LAYER1:{tag}", 95)
    hit = _pick_from_sec_payload(sec_payload, year, combined_payload)
    if hit:
        tag, raw = hit
        v = as_million_usd(raw)
        if v is not None:
            return PickResult(v, f"SEC_PAYLOAD:{tag}", 95)

    # 2) TotalDebt plus explicit lease liabilities if present
    total_debt_tags = ("TotalDebt",)
    total_debt_payload = ("us-gaap:TotalDebt",)
    lease_tags = ("LeaseLiabilities", "LeaseLiabilitiesCurrent", "LeaseLiabilitiesNoncurrent")
    lease_payload = tuple(f"us-gaap:{t}" for t in lease_tags)

    base = None
    base_src = None
    hit = _pick_from_layer1(layer1_by_year, year, total_debt_tags)
    if hit:
        base_src, raw = hit[0], hit[1]
        base = as_million_usd(raw)
        if base is not None:
            src = f"SEC_LAYER1:{base_src}"
            # add leases if separately available
            lease_sum = 0.0
            lease_any = False
            for t in lease_tags:
                h2 = _pick_from_layer1(layer1_by_year, year, (t,))
                if h2:
                    lv = as_million_usd(h2[1])
                    if lv is not None:
                        lease_sum += float(lv)
                        lease_any = True
            if lease_any:
                return PickResult(float(base) + lease_sum, f"{src}+LEASES", 90)
            return PickResult(float(base), src, 90)
    hit = _pick_from_sec_payload(sec_payload, year, total_debt_payload)
    if hit:
        base = as_million_usd(hit[1])
        if base is not None:
            lease_sum = 0.0
            lease_any = False
            for t in lease_payload:
                h2 = _pick_from_sec_payload(sec_payload, year, (t,))
                if h2:
                    lv = as_million_usd(h2[1])
                    if lv is not None:
                        lease_sum += float(lv)
                        lease_any = True
            src = f"SEC_PAYLOAD:{hit[0]}"
            if lease_any:
                return PickResult(float(base) + lease_sum, f"{src}+LEASES", 90)
            return PickResult(float(base), src, 90)

    # 3) Sum components (lower confidence; best-effort, avoid double-count)
    component_tags = (
        "DebtCurrent",
        "DebtNoncurrent",
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "LongTermDebtCurrent",
        "ShortTermBorrowings",
        "CommercialPaper",
        "NotesPayable",
        "CurrentPortionOfLongTermDebt",
        "Debt",
        # leases
        "LeaseLiabilities",
        "LeaseLiabilitiesCurrent",
        "LeaseLiabilitiesNoncurrent",
    )
    component_payload = tuple(f"us-gaap:{t}" for t in component_tags)

    comps = []
    present_any = False
    raw_row = (data_by_year or {}).get(year, {}) or {}
    # layer1_by_year is preferred over data_by_year when present
    for t in component_tags:
        if t in (layer1_by_year or {}).get(year, {}):
            present_any = True
            v = as_million_usd((layer1_by_year.get(year, {}) or {}).get(t))
            if v is not None:
                comps.append(float(v))
    if not comps:
        # SEC payload facts
        try:
            facts = (((sec_payload or {}).get("periods") or {}).get(str(year)) or {}).get("facts") or {}
            for t in component_payload:
                if t in facts:
                    present_any = True
                    v = as_million_usd(facts.get(t))
                    if v is not None:
                        comps.append(float(v))
        except Exception:
            pass
    if not comps:
        for t in component_tags:
            if t in raw_row:
                present_any = True
                v = as_million_usd(raw_row.get(t))
                if v is not None:
                    comps.append(float(v))

    if comps:
        return PickResult(float(sum(comps)), "COMPONENT_SUM", 70)
    if present_any:
        # explicit zeros
        return PickResult(0.0, "EXPLICIT_ZERO", 80)
    return PickResult(None, "MISSING", 0)


def derive_enterprise_value_million(*, market_cap_m: float, total_debt_m: float, cash_m: float) -> Optional[float]:
    if market_cap_m is None or total_debt_m is None or cash_m is None:
        return None
    ev = float(market_cap_m) + float(total_debt_m) - float(cash_m)
    if not math.isfinite(ev) or ev <= 0:
        return None
    return ev
