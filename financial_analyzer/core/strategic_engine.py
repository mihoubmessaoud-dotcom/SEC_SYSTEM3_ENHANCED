def validate_altman_z_series(ticker: str, z_scores: dict, audit: object) -> dict:
    """
    Prevents implausible Altman Z jumps from propagating silently.
    """
    validated = {}
    years = sorted([y for y in (z_scores or {}).keys() if isinstance(y, int)])

    for i, year in enumerate(years):
        z = z_scores.get(year)
        try:
            z = float(z) if z is not None else None
        except Exception:
            z = None

        if z is not None and abs(z) > 50:
            if audit is not None:
                audit.flag(year, "ALTMAN_Z_EXTREME", "HIGH", f"Z={z:.1f} مشبوه")
            z = 50.0 if z > 0 else -50.0

        if i > 0:
            prev_year = years[i - 1]
            prev_z = validated.get(prev_year)
            if prev_z is not None and z is not None and abs(z - prev_z) > 20:
                if audit is not None:
                    audit.flag(
                        year,
                        "ALTMAN_Z_JUMP",
                        "HIGH",
                        f"Z قفز من {prev_z:.1f} إلى {z:.1f} — راجع المكونات",
                    )
                # Keep display realistic and stable between consecutive years.
                if z > prev_z:
                    z = prev_z + 20.0
                else:
                    z = prev_z - 20.0

        validated[year] = z
    return validated


class StrategicEngine:
    def build(self, strategic_sheet_by_year: dict) -> dict:
        return strategic_sheet_by_year or {}


def compute_roe_for_strategic(
    ticker: str,
    year: int,
    net_income: float,
    total_equity_balance: float,
    ratios_sheet_roe: float = None,
    bvps: float = None,
    shares: float = None,
):
    # 1) Ratios sheet (most reliable)
    if ratios_sheet_roe is not None:
        try:
            v = float(ratios_sheet_roe)
            if -5.0 <= v <= 5.0:
                return {
                    "value": v,
                    "reliability": 85,
                    "source": "ratios_sheet",
                    "display": f"{v:.1%}",
                    "flag": "EXTREME_ROE_BUYBACK" if abs(v) > 1.5 else "",
                }
        except (TypeError, ValueError):
            pass

    # 2) Balance sheet equity
    if total_equity_balance is not None:
        try:
            eq = float(total_equity_balance)
            if abs(eq) >= 1.0:
                ni = float(net_income) if net_income else 0.0
                roe = ni / eq
                return {
                    "value": roe,
                    "reliability": 80,
                    "source": "balance_sheet_total",
                    "display": f"{roe:.1%}",
                    "note": "مرتفع بسبب buybacks" if abs(roe) > 1.5 else "",
                }
        except (TypeError, ValueError):
            pass

    # 3) BVPS × Shares (unit-corrected)
    if bvps is not None and shares is not None:
        try:
            bvps_f = float(bvps)
            shares_f = float(shares)
            eq_computed = bvps_f * shares_f
            if eq_computed < 1.0:
                eq_computed *= 1000
            if eq_computed >= 1.0 and net_income:
                roe = float(net_income) / eq_computed
                return {
                    "value": roe,
                    "reliability": 60,
                    "source": "bvps_corrected",
                    "display": f"{roe:.1%}",
                    "flag": "UNIT_CORRECTED",
                }
        except (TypeError, ValueError):
            pass

    return {
        "value": None,
        "reliability": 0,
        "reason": "MISSING_EQUITY_DATA",
        "display": "— (حقوق ملكية غير متاحة)",
    }
