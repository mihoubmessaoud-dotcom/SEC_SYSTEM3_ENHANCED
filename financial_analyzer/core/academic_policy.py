ACADEMIC_DISCLAIMER = (
    "هذه المنصة للأغراض الأكاديمية والبحثية حصراً.\n"
    "لا تُمثّل توصية استثمارية من أي نوع.\n"
    "For academic and research purposes only.\n"
    "Not financial advice."
)


def get_data_quality_grade(coverage_pct: float, blocked_years: list, total_years: int) -> str:
    total = max(1, int(total_years or 0))
    blocked_ratio = len(blocked_years or []) / total
    effective_cov = float(coverage_pct or 0.0) * (1.0 - blocked_ratio)
    if effective_cov >= 95:
        return "A"
    if effective_cov >= 85:
        return "B"
    if effective_cov >= 70:
        return "C"
    return "D"


def get_financial_health(roic, altman_z, operating_margin, sub_sector) -> str:
    # Non-investment academic health indicator.
    score = 0
    try:
        if roic is not None:
            roic = float(roic)
            if roic > 0.10:
                score += 2
            elif roic > 0:
                score += 1
    except Exception:
        pass

    try:
        if altman_z is not None:
            altman_z = float(altman_z)
            if altman_z > 3:
                score += 2
            elif altman_z > 1.81:
                score += 1
    except Exception:
        pass

    try:
        if operating_margin is not None and float(operating_margin) > 0.10:
            score += 1
    except Exception:
        pass

    if score >= 4:
        return "STRONG"
    if score >= 3:
        return "MODERATE"
    if score >= 1:
        return "WEAK"
    return "DISTRESS"

