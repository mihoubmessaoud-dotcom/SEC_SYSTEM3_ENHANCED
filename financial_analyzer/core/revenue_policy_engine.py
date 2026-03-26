FISCAL_YEAR_CALENDAR = {
    "NVDA": {"month_end": 1, "note": "FY يغلق في يناير — إيرادات مستنتجة طبيعية"},
    "INTC": {"month_end": 12, "note": "FY ديسمبر — استنتاج بسبب تعريف SEC"},
    "MSFT": {"month_end": 6, "note": "FY يونيو"},
    "AAPL": {"month_end": 9, "note": "FY سبتمبر"},
    "AMD": {"month_end": 12},
    "JPM": {"month_end": 12},
    "KO": {"month_end": 12},
}

REVENUE_SANITY_CHECKS = {
    "MSFT": {"min_2019": 100_000, "min_2024": 200_000},
    "AAPL": {"min_2020": 200_000, "min_2024": 350_000},
    "NVDA": {"min_2024": 50_000},
    "INTC": {"min_2020": 60_000, "max_2025": 80_000},
}


def validate_revenue_magnitude(ticker: str, year: int, revenue: float, audit: object):
    if revenue is None:
        return None
    checks = REVENUE_SANITY_CHECKS.get(str(ticker or "").upper(), {})
    if not checks:
        return revenue
    try:
        rev = float(revenue)
    except (TypeError, ValueError):
        return revenue

    for check_key, threshold in checks.items():
        check_year = int(check_key.split("_")[1])
        if year >= check_year:
            if "min" in check_key and rev < threshold:
                corrected = None
                for factor in (10, 100, 1000):
                    candidate = rev * factor
                    if candidate >= threshold:
                        corrected = candidate
                        if audit is not None:
                            audit.correction(
                                year,
                                "revenue",
                                rev,
                                corrected,
                                f"unit_magnitude_fix_x{factor}",
                            )
                        break
                return corrected if corrected is not None else rev
            if "max" in check_key and rev > threshold:
                if audit is not None:
                    audit.flag(
                        year,
                        "REVENUE_UNUSUALLY_HIGH",
                        "HIGH",
                        f"Rev={rev:,.0f}M > {threshold:,.0f}M",
                    )
    return rev


def sanity_check_revenue(ticker: str, year: int, revenue: float, prev_year_revenue: float, audit: object):
    if not revenue or not prev_year_revenue:
        return revenue
    try:
        rev = float(revenue)
        prev = float(prev_year_revenue)
    except (TypeError, ValueError):
        return revenue
    if prev == 0:
        return revenue
    ratio = rev / prev
    if ratio < 0.30:
        corrected = rev * 4
        if audit is not None:
            audit.correction(year, "revenue", rev, corrected, "quarterly_to_annual_estimate")
            audit.flag(
                year,
                "REVENUE_QUARTERLY_SUSPECTED",
                "HIGH",
                f"Rev={rev:,.0f} = {ratio:.0%} من {year-1}. تم ضرب × 4 → {corrected:,.0f}",
            )
        return corrected
    if ratio > 3.0 and audit is not None:
        audit.flag(
            year,
            "REVENUE_SPIKE_SUSPECTED",
            "HIGH",
            f"Rev={rev:,.0f} = {ratio:.0%}× من {year-1}",
        )
    return rev

class RevenuePolicyEngine:
    def get_revenue(
        self,
        ticker: str,
        year: int,
        resolved: dict,
        audit: object,
        prev_year_revenue: float = None,
    ) -> dict:
        rev = resolved.get("revenue")
        if rev and float(rev) > 0:
            val = validate_revenue_magnitude(ticker, year, float(rev), audit)
            val = sanity_check_revenue(ticker, year, val, prev_year_revenue, audit)
            return {
                "value": val,
                "inferred": False,
                "reliability": 100,
                "source": "sec_direct",
            }

        gp = resolved.get("gross_profit")
        cogs = resolved.get("cost_of_revenue")
        if gp and cogs:
            val = float(gp) + float(cogs)
            if audit is not None:
                audit.correction(year, "revenue", None, val, "inferred_from_gp_cogs")
            val = validate_revenue_magnitude(ticker, year, val, audit)
            val = sanity_check_revenue(ticker, year, val, prev_year_revenue, audit)
            return {
                "value": val,
                "inferred": True,
                "reliability": 85,
                "flag": "INFERRED_GP_COGS",
                "source": "gross_profit+cogs",
            }

        op_inc = resolved.get("operating_income")
        op_margin = resolved.get("_operating_margin_ref")
        if op_inc and op_margin and float(op_margin) != 0:
            val = float(op_inc) / float(op_margin)
            if val > 0:
                if audit is not None:
                    audit.correction(year, "revenue", None, val, "inferred_from_op_margin")
                    audit.flag(year, "INFERRED_REVENUE", "MEDIUM", "إيراد مستنتج — موثوقية مخفضة")
                val = validate_revenue_magnitude(ticker, year, val, audit)
                val = sanity_check_revenue(ticker, year, val, prev_year_revenue, audit)
                return {
                    "value": val,
                    "inferred": True,
                    "reliability": 65,
                    "flag": "INFERRED_OP_MARGIN",
                    "source": "operating_income/margin",
                    "warning": "إيراد مستنتج",
                }

        if audit is not None:
            audit.flag(year, "REVENUE_MISSING", "HIGH")
        return {
            "value": None,
            "inferred": False,
            "reliability": 0,
            "status": "MISSING",
        }

    def get_revenue_with_context(
        self,
        ticker: str,
        year: int,
        resolved: dict,
        audit: object,
        prev_year_revenue: float = None,
    ) -> dict:
        cal = FISCAL_YEAR_CALENDAR.get(str(ticker or "").upper(), {})
        fy_note = cal.get("note", "")
        result = self.get_revenue(ticker, year, resolved, audit, prev_year_revenue=prev_year_revenue)

        if result.get("inferred") and fy_note:
            result["inference_context"] = "FISCAL_YEAR_MISMATCH"
            result["inference_expected"] = True
            result["note"] = fy_note
            result["reliability"] = max(result.get("reliability", 65), 75)
        return result
