from __future__ import annotations


VERDICT_CONTEXT_RULES = {
    "hardware_platform": {
        "ignore_low_current_ratio": True,
        "ignore_low_fcf_yield": True,
        "pb_penalty_threshold": 100.0,
        "roic_override_threshold": 0.40,
    },
    "consumer_staples": {
        "mandatory_ratio_missing_tolerance": 12,
        "ignore_low_fcf_yield_if_dividend": True,
        "roic_override_threshold": 0.09,
        "high_debt_normal": True,
    },
    "commercial_bank": {
        "ignore_fcf_in_verdict": True,
        "ignore_working_capital_risk": True,
        "fraud_flag_tolerance": 10,
        "ignore_pre_2017_debt": True,
    },
    "investment_bank": {
        "ignore_fcf_in_verdict": True,
        "ignore_working_capital_risk": True,
        "use_roe_when_margin_missing": True,
        "debt_penalty_threshold": 5.0,
        "override_failure_prob": True,
        "failure_prob_cap": 0.15,
    },
    "insurance_broker": {
        "ignore_negative_pb_in_zscore": True,
        "failure_prob_override_when_buyback": True,
        "failure_prob_cap": 0.12,
        "negative_equity_context": "buyback",
    },
    "insurance_life": {
        "use_roe_when_roic_missing": True,
        "combined_ratio_override": True,
        "high_fcf_yield_positive": True,
    },
}


def apply_context_rules(raw_verdict, confidence, company_data, sub_sector):
    rules = VERDICT_CONTEXT_RULES.get(sub_sector, {})
    roic = (company_data.strategic or {}).get("ROIC") if hasattr(company_data, "strategic") else None
    override_threshold = rules.get("roic_override_threshold")
    if roic and override_threshold:
        try:
            if float(roic) > float(override_threshold) and raw_verdict == "FAIL":
                raw_verdict = "WATCH"
                confidence = max(float(confidence or 0), 65.0)
        except (TypeError, ValueError):
            pass

    fp_cap = rules.get("failure_prob_cap")
    if fp_cap and getattr(company_data, "ai", None) is not None:
        try:
            company_data.ai.failure_prob_3y = min(float(company_data.ai.failure_prob_3y), float(fp_cap))
        except Exception:
            pass
    return raw_verdict, confidence

