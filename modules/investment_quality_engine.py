from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CompanyDataView:
    ratios: dict
    strategic: dict
    ai: object | None = None


QUALITY_MODELS = {
    "hardware_platform": {"name": "hardware_platform"},
    "consumer_staples": {"name": "consumer_staples"},
    "commercial_bank": {"name": "commercial_bank"},
    "investment_bank": {"name": "investment_bank"},
    "insurance_life": {"name": "insurance_life"},
    "insurance_broker": {"name": "insurance_broker"},
    "insurance_pc": {"name": "insurance_pc"},
}


def _get_economic_spread(company_data, sub_sector):
    spread_raw = (company_data.strategic or {}).get("Economic_Spread")
    if spread_raw is not None and spread_raw is not False:
        try:
            v = float(spread_raw)
            if abs(v) < 5:
                return v
        except (TypeError, ValueError):
            pass

    if sub_sector == "insurance_life":
        roe = (company_data.ratios or {}).get("roe")
        wacc = (company_data.strategic or {}).get("WACC")
        if roe and wacc:
            try:
                return float(roe) - float(wacc)
            except Exception:
                pass
        fcf = (company_data.ratios or {}).get("fcf_yield")
        if fcf:
            try:
                return float(fcf) - 0.05
            except Exception:
                pass
        return 0.02

    if sub_sector == "insurance_broker":
        net_margin = (company_data.ratios or {}).get("net_margin")
        if net_margin:
            try:
                return float(net_margin) - 0.10
            except Exception:
                pass
        return 0.05

    if sub_sector in ("commercial_bank", "investment_bank"):
        roe = (company_data.ratios or {}).get("roe")
        if roe:
            try:
                return float(roe) - 0.10
            except Exception:
                pass
        return 0.00

    if sub_sector == "insurance_pc":
        cr = (company_data.ratios or {}).get("combined_proxy") or (company_data.ratios or {}).get("combined_ratio")
        if cr:
            try:
                return max(0.0, 1.0 - float(cr))
            except Exception:
                pass
        return 0.02

    if sub_sector == "consumer_staples":
        roic = (company_data.strategic or {}).get("ROIC")
        wacc = (company_data.strategic or {}).get("WACC")
        if roic and wacc:
            try:
                return float(roic) - float(wacc)
            except Exception:
                pass
        return 0.05

    return 0.0


def calculate_investment_score(company_data, sub_sector):
    _ = QUALITY_MODELS.get(sub_sector, QUALITY_MODELS["hardware_platform"])
    spread = _get_economic_spread(company_data, sub_sector)
    company_data._computed_spread = spread
    return spread

