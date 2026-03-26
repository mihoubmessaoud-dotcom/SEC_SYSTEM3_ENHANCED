from financial_analyzer.models.ratio_result import RatioResult


RATIO_BOUNDS = {
    "roe": {"soft": (-1.0, 2.0), "hard": (-5.0, 5.0)},
    "roa": {"soft": (-0.3, 0.5), "hard": (-1.0, 1.0)},
    "roic": {"soft": (-0.5, 1.5), "hard": (-2.0, 3.0)},
    "gross_margin": {"soft": (0.0, 0.9), "hard": (-0.5, 1.0)},
    "operating_margin": {"soft": (-0.5, 0.7), "hard": (-2.0, 1.0)},
    "net_margin": {"soft": (-0.5, 0.7), "hard": (-2.0, 1.0)},
    "current_ratio": {"soft": (0.5, 10.0), "hard": (0.0, 50.0)},
    "debt_to_equity": {"soft": (0.0, 5.0), "hard": (-1.0, 20.0)},
    "pe_ratio": {"soft": (-100, 200), "hard": (-500, 1000)},
    "pb_ratio": {"soft": (-10, 50), "hard": (-50, 200)},
    "interest_coverage": {"soft": (-10, 100), "hard": (-50, 500)},
    "fcf_yield": {"soft": (-0.5, 0.5), "hard": (-2.0, 2.0)},
}

BLOCKED_BY_SECTOR = {
    "commercial_bank": ["gross_margin", "fcf_yield", "current_ratio", "inventory_days", "combined_ratio"],
    "investment_bank": ["gross_margin", "fcf_yield", "current_ratio", "combined_ratio"],
    "insurance_life": ["combined_ratio", "gross_margin", "current_ratio", "capex_ratio"],
    "insurance_pc": ["gross_margin", "current_ratio"],
    "insurance_broker": ["combined_ratio", "loss_ratio", "expense_ratio", "capital_adequacy_proxy"],
    "integrated_oil": ["combined_ratio"],
    "consumer_staples": ["combined_ratio"],
    "semiconductor_fabless": ["combined_ratio", "nim"],
    "software_saas": ["combined_ratio", "nim", "inventory_days"],
    "hardware_platform": ["combined_ratio", "nim"],
    "ev_automaker": ["combined_ratio", "nim"],
}

IMPOSSIBLE_VALUE_RULES = {
    "days_sales_outstanding": {"hard_max": 3650, "hard_min": 0, "action": "DELETE", "reason": "DSO > 3650 يوم مستحيل رياضياً"},
    "days_inventory_outstanding": {"hard_max": 1825, "hard_min": 0, "action": "DELETE"},
    "roa": {"hard_max": 2.0, "hard_min": -1.0, "action": "FLAG"},
    "altman_z_score": {"hard_max": 50, "hard_min": -20, "action": "FLAG", "note": "راجع مكونات Z-Score"},
    "interest_coverage": {"hard_max": 500, "hard_min": -100, "action": "CAP", "display_note": "تغطية فوائد استثنائية"},
    "pe_ratio": {
        "hard_max": 1000,
        "hard_min": -1000,
        "action": "FLAG",
        "display_when_negative": "خسارة صافية",
        "display_when_extreme": "غير قابل للتطبيق",
    },
}

IMPOSSIBLE_BOUNDS = {
    "days_sales_outstanding": (0, 365),
    "days_inventory_outstanding": (0, 730),
    "days_payable_outstanding": (0, 365),
    "ccc_days": (-180, 400),
    "interest_coverage": (-200, 500),
    "altman_z_score": (-20, 50),
    "net_debt_ebitda": (-50, 50),
}


def enforce_impossible_bounds(metric: str, value, ticker: str, year: int, audit: object):
    if value is None:
        return {"value": None, "reason": "MISSING_INPUT"}
    try:
        v = float(value)
    except (TypeError, ValueError):
        return {"value": None, "reason": "MISSING_INPUT"}
    bounds = IMPOSSIBLE_BOUNDS.get(metric)
    if not bounds:
        return {"value": v, "reason": ""}
    lo, hi = bounds
    if lo <= v <= hi:
        return {"value": v, "reason": ""}
    if audit is not None:
        audit.flag(
            year,
            f"IMPOSSIBLE_{metric.upper()}",
            "CRITICAL",
            f"{ticker} {year}: {metric}={v:.1f} خارج [{lo},{hi}]",
        )
    return {"value": None, "reason": "IMPOSSIBLE_VALUE"}

def apply_impossible_rules(metric: str, value: float, ticker: str, year: int, audit: object) -> dict:
    rule = IMPOSSIBLE_VALUE_RULES.get(metric)
    if not rule or value is None:
        return {"value": value, "action": "KEEP"}

    hard_min = rule.get("hard_min", float("-inf"))
    hard_max = rule.get("hard_max", float("inf"))
    action = rule.get("action", "FLAG")
    if hard_min <= value <= hard_max:
        return {"value": value, "action": "KEEP"}

    if audit is not None:
        audit.flag(year, f"IMPOSSIBLE_{metric.upper()}", "CRITICAL", f"{metric}={value:.2f} خارج [{hard_min},{hard_max}]")

    if action == "DELETE":
        if audit is not None:
            audit.correction(year, metric, value, value, "impossible_value_flagged")
        return {
            "value": value,
            "action": "FLAGGED",
            "flag": "IMPOSSIBLE_VALUE",
            "display": f"{value:.1f} ⚠️",
            "note": "قيمة مستحيلة — معروضة للتحذير",
        }

    if action == "CAP":
        capped = hard_max if value > hard_max else hard_min
        if audit is not None:
            audit.correction(year, metric, value, capped, "capped_at_boundary")
        return {
            "value": capped,
            "action": "CAPPED",
            "original": value,
            "display": f"{capped:.1f}+ ⚠️",
            "note": rule.get("display_note", ""),
        }

    return {
        "value": value,
        "action": "FLAGGED",
        "flag": "OUTSIDE_HARD_BOUNDS",
        "display": f"{value:.2f} ⚠️",
        "note": rule.get("reason", ""),
    }


def validate_ccc(value, ticker, year, audit):
    if value is None:
        return value
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v > 3650 or v < -365:
        if audit is not None:
            audit.flag(year, "CCC_IMPOSSIBLE", "CRITICAL", f"CCC={v:.0f} مستحيل — معروض بتحذير")
        return v
    return v


class RatioEngine:
    def __init__(self):
        self._audit_entries = []

    def _safe_divide(self, num, den, metric: str, ticker: str = "", year: int = 0, min_den: float = 1.0, audit: object = None) -> RatioResult:
        if num is None or den is None:
            return RatioResult(
                value=None,
                status="NOT_COMPUTABLE",
                reliability=0,
                reason="MISSING_INPUT",
                metric=metric,
            )

        den_f = float(den)
        if abs(den_f) < min_den:
            return RatioResult(
                value=None,
                status="NOT_COMPUTABLE",
                reliability=0,
                reason="ZERO_DENOMINATOR",
                note=f"مقام={den_f:.4f}",
                metric=metric,
            )

        val = float(num) / den_f
        bounds = RATIO_BOUNDS.get(metric)
        if bounds:
            soft_min, soft_max = bounds["soft"]
            hard_min, hard_max = bounds["hard"]
            if not (hard_min <= val <= hard_max):
                return RatioResult(
                    value=val,
                    status="OUTLIER_HARD",
                    reliability=30,
                    flag="OUTSIDE_HARD_BOUNDS",
                    display=f"{val:.1%}",
                    note="قيمة متطرفة — راجع المصدر",
                    metric=metric,
                )
            if not (soft_min <= val <= soft_max):
                return RatioResult(
                    value=val,
                    status="OUTLIER_SOFT",
                    reliability=60,
                    flag="OUTSIDE_SOFT_BOUNDS",
                    display=f"{val:.1%}",
                    metric=metric,
                )

        result = RatioResult(
            value=val,
            status="COMPUTED",
            reliability=80,
            display=f"{val:.2f}",
            metric=metric,
        )
        return self._apply_impossible_to_result(result, metric, ticker, year, audit)

    def _apply_impossible_to_result(self, rr: RatioResult, metric: str, ticker: str, year: int, audit: object):
        if not isinstance(rr, RatioResult) or rr.value is None:
            return rr
        policy = apply_impossible_rules(metric, rr.value, ticker, year, audit)
        act = policy.get("action")
        if act == "KEEP":
            return rr
        if act == "DELETED":
            rr.value = None
            rr.status = "NOT_COMPUTABLE"
            rr.reason = "IMPOSSIBLE_VALUE"
            rr.display = policy.get("display", rr.display)
            rr.note = policy.get("reason", rr.note)
            return rr
        if act == "CAPPED":
            rr.value = policy.get("value")
            rr.status = "OUTLIER_HARD"
            rr.flag = "CAPPED_TO_BOUNDARY"
            rr.display = policy.get("display", rr.display)
            rr.note = policy.get("note", rr.note)
            return rr
        if act == "FLAGGED":
            rr.status = "OUTLIER_HARD"
            rr.flag = policy.get("flag", rr.flag)
            rr.display = policy.get("display", rr.display)
            rr.note = policy.get("note", rr.note)
            return rr
        return rr

    def is_blocked(self, metric: str, sub_sector: str) -> bool:
        return metric in BLOCKED_BY_SECTOR.get(sub_sector, [])

    def calc_roe(self, net_income, total_equity, ratios_backup=None, ticker="", year=0, audit: object = None) -> RatioResult:
        if ratios_backup is not None:
            try:
                v = float(ratios_backup)
                if abs(v) <= 5.0:
                    return RatioResult(
                        value=v,
                        status="COMPUTED",
                        reliability=80,
                        source="ratios_sheet",
                        display=f"{v:.1%}",
                        flag="EXTREME_ROE_BUYBACK" if abs(v) > 1.5 else "",
                        metric="roe",
                    )
            except (TypeError, ValueError):
                pass
        return self._safe_divide(net_income, total_equity, "roe", ticker, year, min_den=1.0, audit=audit)

    def calc_ccc(self, inv_days, ar_days, ap_days, rev_inferred=False, ticker: str = "", year: int = 0, audit: object = None) -> RatioResult:
        components = [v for v in [inv_days, ar_days, ap_days] if v is not None]
        if len(components) == 3:
            ccc = float(inv_days) + float(ar_days) - float(ap_days)
            enforced = enforce_impossible_bounds("ccc_days", ccc, ticker, year, audit)
            v = enforced.get("value")
            rr = RatioResult(
                value=v,
                status="NOT_COMPUTABLE" if v is None and enforced.get("reason") else "COMPUTED",
                reliability=0 if v is None and enforced.get("reason") else (70 if rev_inferred else 88),
                reason=enforced.get("reason") if v is None else "",
                display=f"{ccc:.1f} يوم" if v is not None else "— (قيمة مستحيلة) ⚠️",
                note="مكونات من إيراد مستنتج" if rev_inferred and v is not None else "",
                metric="ccc_days",
            )
            return rr
        if len(components) == 2:
            partial_sum = sum(float(v) for v in components)
            enforced = enforce_impossible_bounds("ccc_days", partial_sum, ticker, year, audit)
            partial_sum = enforced.get("value")
            return RatioResult(
                value=partial_sum,
                status="NOT_COMPUTABLE" if partial_sum is None and enforced.get("reason") else "PARTIAL",
                reliability=0 if partial_sum is None and enforced.get("reason") else 45,
                flag="PARTIAL_CCC",
                display=f"~{partial_sum:.0f} يوم" if partial_sum is not None else "— (قيمة مستحيلة) ⚠️",
                note="مكون واحد مفقود" if partial_sum is not None else "",
                metric="ccc_days",
            )
        if len(components) == 1:
            partial_sum = float(components[0])
            enforced = enforce_impossible_bounds("ccc_days", partial_sum, ticker, year, audit)
            partial_sum = enforced.get("value")
            return RatioResult(
                value=partial_sum,
                status="NOT_COMPUTABLE" if partial_sum is None and enforced.get("reason") else "PARTIAL",
                reliability=0 if partial_sum is None and enforced.get("reason") else 30,
                flag="PARTIAL_CCC",
                display=f"~{partial_sum:.0f} يوم" if partial_sum is not None else "— (قيمة مستحيلة) ⚠️",
                note="مكونان مفقودان" if partial_sum is not None else "",
                metric="ccc_days",
            )
        return RatioResult(
            value=None,
            status="NOT_COMPUTABLE",
            reliability=0,
            reason="MISSING_CCC_COMPONENTS",
            metric="ccc_days",
        )

    def calc_gross_margin(self, resolved: dict, revenue, ticker: str = "", year: int = 0, audit: object = None) -> RatioResult:
        if not revenue:
            return RatioResult(
                value=None,
                status="NOT_COMPUTABLE",
                reliability=0,
                reason="MISSING_REVENUE",
                metric="gross_margin",
            )
        rev = float(revenue)
        gm_ratio = resolved.get("gross_margin_ratio") if isinstance(resolved, dict) else None
        if gm_ratio is not None:
            try:
                gm_f = float(gm_ratio)
                if 0 <= gm_f <= 1.5:
                    return RatioResult(
                        value=gm_f,
                        status="COMPUTED",
                        reliability=70,
                        source="gross_margin_ratio_label",
                        display=f"{gm_f:.1%}",
                        metric="gross_margin",
                    )
            except (TypeError, ValueError):
                pass
        gp = resolved.get("gross_profit") if isinstance(resolved, dict) else None
        if gp is not None:
            try:
                gp_f = float(gp)
                if abs(gp_f) <= 1.5 and rev > 10:
                    return RatioResult(
                        value=gp_f,
                        status="COMPUTED",
                        reliability=70,
                        source="gross_margin_ratio_label",
                        display=f"{gp_f:.1%}",
                        metric="gross_margin",
                    )
                cogs = resolved.get("cost_of_revenue") if isinstance(resolved, dict) else None
                if gp_f < 0 and cogs is not None:
                    try:
                        cogs_f = float(cogs)
                        if 0 < cogs_f < rev:
                            num = rev - cogs_f
                            return self._safe_divide(num, rev, "gross_margin", ticker=ticker, year=year, audit=audit)
                    except (TypeError, ValueError):
                        pass
                return self._safe_divide(gp_f, rev, "gross_margin", ticker=ticker, year=year, audit=audit)
            except (TypeError, ValueError):
                pass

        cogs = resolved.get("cost_of_revenue") if isinstance(resolved, dict) else None
        if cogs:
            num = rev - float(cogs)
            return self._safe_divide(num, rev, "gross_margin", ticker=ticker, year=year, audit=audit)

        cogs_product = resolved.get("cost_revenue_product", 0) if isinstance(resolved, dict) else 0
        cogs_service = resolved.get("cost_revenue_service", 0) if isinstance(resolved, dict) else 0
        if (cogs_product or cogs_service) and (float(cogs_product) + float(cogs_service) > 0):
            total_cogs = float(cogs_product) + float(cogs_service)
            num = rev - total_cogs
            return self._safe_divide(
                num,
                rev,
                "gross_margin",
                ticker=ticker,
                year=year,
                audit=audit,
            )

        op_income = resolved.get("operating_income") if isinstance(resolved, dict) else None
        op_ex = resolved.get("operating_expenses") if isinstance(resolved, dict) else None
        if op_income and op_ex:
            gp_est = float(op_income) + float(op_ex)
            r = self._safe_divide(gp_est, rev, "gross_margin", ticker=ticker, year=year, audit=audit)
            r.reliability = 55
            r.note = "تقدير من نفقات تشغيلية"
            return r

        return RatioResult(
            value=None,
            status="NOT_COMPUTABLE",
            reliability=0,
            reason="MISSING_GROSS_PROFIT_AND_COGS",
            metric="gross_margin",
        )

    @staticmethod
    def calc_total_debt(resolved: dict, ticker: str = "", year: int = 0):
        if not isinstance(resolved, dict):
            return None
        td = resolved.get("total_debt")
        if td:
            try:
                return float(td)
            except (TypeError, ValueError):
                return None
        ltd = float(resolved.get("long_term_debt", 0) or 0)
        stb = float(resolved.get("short_term_borrowings", 0) or 0)
        ltdc = float(resolved.get("long_term_debt_current", 0) or 0)
        total = ltd + stb + ltdc
        if total > 0:
            return total
        np_val = resolved.get("notes_payable")
        if np_val:
            try:
                return float(np_val)
            except (TypeError, ValueError):
                return None
        return None

    def calc_operating_margin(self, op_income, revenue, ticker: str = "", year: int = 0, audit: object = None) -> RatioResult:
        return self._safe_divide(op_income, revenue, "operating_margin", ticker=ticker, year=year, audit=audit)

    def calc_net_margin(self, net_income, revenue, ticker: str = "", year: int = 0, audit: object = None) -> RatioResult:
        return self._safe_divide(net_income, revenue, "net_margin", ticker=ticker, year=year, audit=audit)

    def calc_roic(self, ebit, tax_rate, invested_capital, ticker: str = "", year: int = 0, audit: object = None) -> RatioResult:
        if ebit and tax_rate is not None and invested_capital:
            nopat = float(ebit) * (1 - float(tax_rate))
            return self._safe_divide(nopat, invested_capital, "roic", ticker=ticker, year=year, min_den=1.0, audit=audit)
        return RatioResult(value=None, status="NOT_COMPUTABLE", reliability=0, reason="MISSING_INPUT", metric="roic")

    def calc_fcf_yield(self, fcf, market_cap, sub_sector: str, ticker: str = "", year: int = 0, audit: object = None) -> RatioResult:
        blocked = ["commercial_bank", "investment_bank", "insurance_life"]
        if sub_sector in blocked:
            return RatioResult(
                value=None,
                status="NOT_COMPUTABLE",
                reliability=0,
                reason="BLOCKED_BY_SECTOR",
                note=f"FCF غير مطبَّق لـ {sub_sector}",
                metric="fcf_yield",
            )
        return self._safe_divide(fcf, market_cap, "fcf_yield", ticker=ticker, year=year, audit=audit)
