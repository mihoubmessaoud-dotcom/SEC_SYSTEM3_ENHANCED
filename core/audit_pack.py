from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, (int, float)):
            fv = float(v)
            if math.isfinite(fv):
                return fv
            return None
        s = str(v).strip().replace(",", "")
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return None
        fv = float(s)
        if math.isfinite(fv):
            return fv
        return None
    except Exception:
        return None


def _pick(d: Dict[str, Any], keys: Iterable[str]) -> Optional[float]:
    for k in keys:
        if not isinstance(d, dict):
            return None
        if k in d:
            v = _safe_float(d.get(k))
            if v is not None:
                return v
    return None


def _ratio_gap(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a in (None, 0) or b in (None, 0):
        return None
    aa = abs(float(a))
    bb = abs(float(b))
    if aa == 0 or bb == 0:
        return None
    hi = max(aa, bb)
    lo = max(min(aa, bb), 1e-18)
    return hi / lo


def _near_scale_factor(gap: float, scales: Iterable[float], tol: float = 0.15) -> Optional[float]:
    # Returns a scale S when gap is close to S (or 1/S).
    if gap is None or gap <= 0:
        return None
    for s in scales:
        if s <= 0:
            continue
        if abs(gap - s) / max(s, 1e-18) <= tol:
            return s
        inv = 1.0 / s
        if abs(gap - inv) / max(inv, 1e-18) <= tol:
            return inv
    return None


@dataclass(frozen=True)
class AuditCheck:
    name: str
    status: str  # PASS | FAIL | UNAVAILABLE
    observed: Optional[float]
    implied: Optional[float]
    gap: Optional[float]
    note: Optional[str]


def build_institutional_audit_pack(
    *,
    ticker: str,
    period: str,
    data_by_year: Dict[str, Dict[str, Any]],
    financial_ratios: Dict[str, Dict[str, Any]],
    canonical_money_unit: str = "usd_million",
    canonical_shares_unit: str = "shares_million",
) -> Dict[str, Any]:
    """
    Phase-1 audit pack: diagnostics only.
    - Does not change UI/export outputs.
    - Detects unit/basis inconsistencies and valuation identity breaks.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ticker_u = str(ticker or "").upper().strip()

    data_by_year_s: Dict[str, Dict[str, Any]] = {}
    for k, v in (data_by_year or {}).items():
        data_by_year_s[str(k)] = v or {}
    financial_ratios_s: Dict[str, Dict[str, Any]] = {}
    for k, v in (financial_ratios or {}).items():
        financial_ratios_s[str(k)] = v or {}

    years = sorted(
        {str(y) for y in data_by_year_s.keys() | financial_ratios_s.keys()},
        key=lambda s: int(s) if s.isdigit() else 10**9,
    )

    per_year: Dict[str, Any] = {}
    summary = {
        "years_total": len(years),
        "years_with_any_issue": 0,
        "check_fail_count": 0,
        "check_pass_count": 0,
        "check_unavailable_count": 0,
        "issues_by_code": {},
    }

    def _add_issue(issues: List[Dict[str, Any]], code: str, message: str, context: Optional[Dict[str, Any]] = None):
        issues.append({"code": code, "message": message, "context": context or {}})
        summary["issues_by_code"][code] = int(summary["issues_by_code"].get(code, 0)) + 1

    for y in years:
        row = (data_by_year_s or {}).get(y) or {}
        rr = (financial_ratios_s or {}).get(y) or {}

        market_cap_m = _pick(rr, ["market_cap"])
        shares_m = _pick(rr, ["shares_outstanding"]) or _pick(
            row,
            [
                "EntityCommonStockSharesOutstanding",
                "CommonStockSharesOutstanding",
                "dei:EntityCommonStockSharesOutstanding",
                "WeightedAverageNumberOfSharesOutstandingBasic",
                "SharesBasic",
                "Basic (in shares)",
            ],
        )
        eps = _pick(rr, ["annual_eps", "eps_basic", "eps_diluted", "eps"])
        pe = _pick(rr, ["pe_ratio_used", "pe_ratio"])
        pb = _pick(rr, ["pb_ratio"])
        bvps = _pick(rr, ["book_value_per_share", "bvps"])
        equity_m = _pick(
            row,
            [
                "StockholdersEquity",
                "TotalEquity",
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                "Total Equity",
            ],
        )

        implied_price = None
        if market_cap_m not in (None, 0) and shares_m not in (None, 0):
            # With canonical usd_million and shares_million, price is simply mcap/shares.
            implied_price = market_cap_m / shares_m

        checks: List[AuditCheck] = []
        issues: List[Dict[str, Any]] = []

        # Check 1: PE identity (pe ≈ price/eps)
        implied_pe = None
        if implied_price not in (None, 0) and eps not in (None, 0):
            implied_pe = implied_price / eps
        gap_pe = _ratio_gap(pe, implied_pe)
        if pe is None or implied_pe is None:
            checks.append(AuditCheck("pe_identity", "UNAVAILABLE", pe, implied_pe, gap_pe, "Need pe_ratio and eps + price anchor"))
        else:
            status = "PASS" if (gap_pe is not None and gap_pe <= 1.25) else "FAIL"
            note = None
            if status == "FAIL" and gap_pe is not None:
                near = _near_scale_factor(gap_pe, [1e3, 1e6, 1e9])
                if near is not None:
                    note = f"Scale mismatch suspected (gap~{near:g})"
                    _add_issue(
                        issues,
                        "UNIT_SCALE_SUSPECT_PE",
                        "PE identity gap suggests unit/scale mismatch between price/EPS/PE.",
                        {"gap": gap_pe, "near_scale": near},
                    )
                else:
                    _add_issue(
                        issues,
                        "PE_IDENTITY_FAIL",
                        "PE identity check failed (pe != price/eps).",
                        {"gap": gap_pe},
                    )
            checks.append(AuditCheck("pe_identity", status, pe, implied_pe, gap_pe, note))

        # Check 2: PB identity via equity (pb ≈ market_cap/equity)
        implied_pb_eq = None
        if market_cap_m not in (None, 0) and equity_m not in (None, 0):
            implied_pb_eq = market_cap_m / equity_m
        gap_pb_eq = _ratio_gap(pb, implied_pb_eq)
        if pb is None or implied_pb_eq is None:
            checks.append(AuditCheck("pb_identity_equity", "UNAVAILABLE", pb, implied_pb_eq, gap_pb_eq, "Need pb_ratio + market_cap + equity"))
        else:
            status = "PASS" if (gap_pb_eq is not None and gap_pb_eq <= 1.30) else "FAIL"
            note = None
            if status == "FAIL" and gap_pb_eq is not None:
                near = _near_scale_factor(gap_pb_eq, [1e3, 1e6, 1e9])
                if near is not None:
                    note = f"Scale mismatch suspected (gap~{near:g})"
                    _add_issue(
                        issues,
                        "UNIT_SCALE_SUSPECT_PB_EQUITY",
                        "PB (via equity) gap suggests unit/scale mismatch between market cap and equity.",
                        {"gap": gap_pb_eq, "near_scale": near},
                    )
                else:
                    _add_issue(
                        issues,
                        "PB_IDENTITY_EQUITY_FAIL",
                        "PB identity check failed (pb != market_cap/equity).",
                        {"gap": gap_pb_eq},
                    )
            checks.append(AuditCheck("pb_identity_equity", status, pb, implied_pb_eq, gap_pb_eq, note))

        # Check 3: PB identity via BVPS (pb ≈ price/bvps)
        implied_pb_bvps = None
        if implied_price not in (None, 0) and bvps not in (None, 0):
            implied_pb_bvps = implied_price / bvps
        gap_pb_bvps = _ratio_gap(pb, implied_pb_bvps)
        if pb is None or implied_pb_bvps is None:
            checks.append(AuditCheck("pb_identity_bvps", "UNAVAILABLE", pb, implied_pb_bvps, gap_pb_bvps, "Need pb_ratio + bvps + price anchor"))
        else:
            status = "PASS" if (gap_pb_bvps is not None and gap_pb_bvps <= 1.30) else "FAIL"
            note = None
            if status == "FAIL" and gap_pb_bvps is not None:
                near = _near_scale_factor(gap_pb_bvps, [1e3, 1e6, 1e9])
                if near is not None:
                    note = f"Scale mismatch suspected (gap~{near:g})"
                    _add_issue(
                        issues,
                        "UNIT_SCALE_SUSPECT_PB_BVPS",
                        "PB (via BVPS) gap suggests unit/scale mismatch between price/BVPS/PB.",
                        {"gap": gap_pb_bvps, "near_scale": near},
                    )
                else:
                    _add_issue(
                        issues,
                        "PB_IDENTITY_BVPS_FAIL",
                        "PB identity check failed (pb != price/bvps).",
                        {"gap": gap_pb_bvps},
                    )
            checks.append(AuditCheck("pb_identity_bvps", status, pb, implied_pb_bvps, gap_pb_bvps, note))

        # Sanity: implied price must be plausible if computable.
        if implied_price is not None:
            if implied_price <= 0 or implied_price > 200_000:
                _add_issue(
                    issues,
                    "IMPLIED_PRICE_OUTLIER",
                    "Implied price from market_cap/shares is out of plausible bounds.",
                    {"implied_price": implied_price},
                )

        # Sanity: shares magnitude (in million shares) should not be absurd.
        if shares_m is not None:
            if shares_m <= 0 or shares_m > 2_000_000:  # 2 trillion shares (in millions) is absurd
                _add_issue(
                    issues,
                    "SHARES_OUTLIER",
                    "Shares magnitude is out of plausible bounds (shares_million).",
                    {"shares_million": shares_m},
                )

        # Summary counts
        year_has_issue = bool(issues)
        if year_has_issue:
            summary["years_with_any_issue"] += 1

        for c in checks:
            if c.status == "PASS":
                summary["check_pass_count"] += 1
            elif c.status == "FAIL":
                summary["check_fail_count"] += 1
            else:
                summary["check_unavailable_count"] += 1

        per_year[y] = {
            "inputs": {
                "market_cap": market_cap_m,
                "shares_outstanding": shares_m,
                "equity": equity_m,
                "eps": eps,
                "pe_ratio": pe,
                "pb_ratio": pb,
                "bvps": bvps,
            },
            "derived": {
                "implied_price": implied_price,
                "implied_pe": implied_pe,
                "implied_pb_equity": implied_pb_eq,
                "implied_pb_bvps": implied_pb_bvps,
            },
            "checks": [
                {
                    "name": c.name,
                    "status": c.status,
                    "observed": c.observed,
                    "implied": c.implied,
                    "gap": c.gap,
                    "note": c.note,
                }
                for c in checks
            ],
            "issues": issues,
        }

    pack = {
        "schema": "institutional_audit_pack_v1",
        "generated_at_utc": now,
        "ticker": ticker_u,
        "period": period,
        "canonical_units": {"money": canonical_money_unit, "shares": canonical_shares_unit},
        "summary": summary,
        "by_year": per_year,
    }
    return pack


def write_audit_pack_to_outputs(*, audit_pack: Dict[str, Any], outputs_dir: str = "outputs") -> str:
    ticker = str(audit_pack.get("ticker") or "UNKNOWN").upper()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(outputs_dir, "audits")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"audit_{ticker}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(audit_pack, f, ensure_ascii=False, indent=2)
    return path
