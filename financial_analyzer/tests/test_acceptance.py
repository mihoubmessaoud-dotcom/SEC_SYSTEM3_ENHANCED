import pytest
from pathlib import Path

from financial_analyzer.core.pipeline_orchestrator import PipelineOrchestrator
from financial_analyzer.core.academic_policy import get_financial_health
from financial_analyzer.models.ratio_result import RatioResult
from financial_analyzer.core.sector_quality_models import QUALITY_MODELS
from financial_analyzer.core.ratio_engine import RatioEngine
from financial_analyzer.core.revenue_policy_engine import sanity_check_revenue
from financial_analyzer.core.canonical_label_map import CanonicalLabelResolver


def _resolve_file(ticker: str) -> str:
    roots = [
        Path("exports/manual_exports"),
        Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\test 3\TEQST"),
    ]
    candidates = []
    for root in roots:
        if not root.exists():
            continue
        candidates.extend(root.glob(f"{ticker}_analysis_*.xlsx"))
    if not candidates:
        pytest.skip(f"No analysis file found for {ticker}")
    latest = sorted(candidates, key=lambda p: p.stat().st_mtime)[-1]
    return str(latest)


def run_pipeline(ticker: str):
    orch = PipelineOrchestrator()
    return orch.run(_resolve_file(ticker), {})


@pytest.fixture(scope="module")
def all_results():
    files = {
        "AAPL": "exports/manual_exports/AAPL_analysis_20260318_180212.xlsx",
        "MSFT": "exports/manual_exports/MSFT_analysis_20260312_071533.xlsx",
        "NVDA": "exports/manual_exports/NVDA_analysis_20260321_151137.xlsx",
        "AIG": "exports/manual_exports/AIG_analysis_20260317_193812.xlsx",
        "TSLA": "exports/manual_exports/TSLA_analysis_20260306_171858.xlsx",
    }
    orch = PipelineOrchestrator()
    cache = {}
    out = {}
    for t, f in files.items():
        result = orch.run(f, cache)
        cache[t] = {"ratios": result.ratios}
        out[t] = result
    return out


def _latest(result):
    return max(result.valid_years) if result.valid_years else None


def test_01_fixture_loaded(all_results):
    assert len(all_results) >= 5


def test_02_all_status_ok(all_results):
    for r in all_results.values():
        assert r.status == "OK"


def test_03_aapl_sector(all_results):
    assert all_results["AAPL"].sub_sector == "hardware_platform"


def test_04_msft_sector(all_results):
    assert all_results["MSFT"].sub_sector == "software_saas"


def test_05_nvda_sector(all_results):
    assert all_results["NVDA"].sub_sector == "semiconductor_fabless"


def test_06_aig_sector(all_results):
    assert all_results["AIG"].sub_sector == "insurance_pc"


def test_07_tsla_sector(all_results):
    assert all_results["TSLA"].sub_sector == "ev_automaker"


def test_08_nvda_2015_blocked(all_results):
    assert 2015 in all_results["NVDA"].blocked_years


def test_09_aapl_not_blocked(all_results):
    assert all_results["AAPL"].blocked_years == []


def test_10_latest_verdict_exists(all_results):
    for r in all_results.values():
        y = _latest(r)
        assert y is not None
        assert r.verdicts.get(y, {}).get("verdict") in {"PASS", "WATCH", "FAIL"}


def test_11_aapl_not_fail(all_results):
    r = all_results["AAPL"]
    y = _latest(r)
    assert r.verdicts[y]["verdict"] != "FAIL"


def test_12_msft_not_fail(all_results):
    r = all_results["MSFT"]
    y = _latest(r)
    assert r.verdicts[y]["verdict"] != "FAIL"


def test_13_nvda_not_fail(all_results):
    r = all_results["NVDA"]
    y = _latest(r)
    assert r.verdicts[y]["verdict"] != "FAIL"


def test_14_aapl_score_high(all_results):
    assert all_results["AAPL"].quality_score >= 70


def test_15_msft_score_high(all_results):
    assert all_results["MSFT"].quality_score >= 60


def test_16_nvda_score_high(all_results):
    assert all_results["NVDA"].quality_score >= 70


def test_17_tsla_score_reasonable(all_results):
    assert all_results["TSLA"].quality_score >= 35


def test_18_aig_score_reasonable(all_results):
    assert all_results["AIG"].quality_score >= 30


def test_19_nvda_roe_not_na(all_results):
    r = all_results["NVDA"]
    for y in r.valid_years:
        roe = r.ratios[y].get("roe")
        if isinstance(roe, RatioResult):
            assert roe.value is not None


def test_20_nvda_ccc_2025(all_results):
    r = all_results["NVDA"]
    assert 2025 in r.valid_years
    ccc = r.ratios[2025].get("ccc_days")
    assert isinstance(ccc, RatioResult)
    assert ccc.value is not None
    assert 50 < ccc.value < 150


def test_21_no_silent_na(all_results):
    for r in all_results.values():
        for y in r.valid_years:
            for _, v in r.ratios[y].items():
                if isinstance(v, RatioResult) and v.value is None:
                    assert v.reason != ""


def test_22_peer_info_present(all_results):
    for r in all_results.values():
        info = (r.peers or {}).get("info", {})
        assert "all" in info
        assert isinstance(info.get("all"), list)


def test_23_prof_score_positive(all_results):
    for r in all_results.values():
        assert r.professional_score > 0


def test_24_intc_classified_as_idm():
    result = run_pipeline("INTC")
    assert result.sub_sector == "semiconductor_idm", f"INTC sub_sector={result.sub_sector}"


def test_25_dso_impossible_deleted():
    result = run_pipeline("INTC")
    dso = result.ratios.get(2025, {}).get("days_sales_outstanding")
    if isinstance(dso, RatioResult):
        assert dso.value is None or dso.value <= 3650, f"DSO={dso.value} مستحيل — لم يُحذف"


def test_26_altman_z_no_extreme_jumps():
    for ticker in ["NVDA", "AMD", "INTC"]:
        result = run_pipeline(ticker)
        z_vals = {year: result.strategic.get(year, {}).get("Altman_Z_Score") for year in result.valid_years}
        years = sorted(z_vals.keys())
        for i in range(1, len(years)):
            z1 = z_vals.get(years[i - 1])
            z2 = z_vals.get(years[i])
            if z1 is not None and z2 is not None:
                assert abs(float(z2) - float(z1)) <= 25, f"{ticker}: Z قفز {z1:.1f}→{z2:.1f}"


def test_27_pre_2014_not_in_scoring():
    for ticker in ["NVDA", "AMD", "INTC"]:
        result = run_pipeline(ticker)
        pre_2014 = [y for y in getattr(result, "scoring_years", []) if y < 2014]
        assert len(pre_2014) == 0, f"{ticker}: سنوات قبل 2014 في الدرجة: {pre_2014}"


def test_28_no_investment_recommendation():
    for ticker in ["NVDA", "AMD", "INTC"]:
        result = run_pipeline(ticker)
        action = getattr(result, "recommended_action", None)
        assert action is None, f"{ticker}: action='{action}' يجب حذفه"


def test_29_intc_financial_health_distress():
    result = run_pipeline("INTC")
    latest = max(result.valid_years)
    roic = result.ratios.get(latest, {}).get("roic")
    roic_val = roic.value if isinstance(roic, RatioResult) else None
    z = result.strategic.get(latest, {}).get("Altman_Z_Score")
    health = get_financial_health(roic_val, float(z) if z is not None else None, None, "semiconductor_idm")
    assert health in ["WEAK", "DISTRESS"], f"INTC health={health} — يجب WEAK أو DISTRESS"


def test_30_revenue_inferred_has_context():
    result = run_pipeline("NVDA")
    for year in result.valid_years:
        rev = result.ratios.get(year, {}).get("revenue")
        if isinstance(rev, dict) and rev.get("inferred"):
            assert "inference_context" in rev, f"NVDA {year}: إيراد مستنتج بدون سياق"


def test_31_optimal_range_default():
    for ticker in ["NVDA", "AMD", "INTC", "AAPL", "MSFT"]:
        result = run_pipeline(ticker)
        scoring = result.scoring_years
        pre_2015 = [y for y in scoring if y < 2015]
        assert len(pre_2015) == 0, f"{ticker}: {pre_2015} في سنوات التقييم"


def test_32_intc_classified_idm():
    result = run_pipeline("INTC")
    assert result.sub_sector == "semiconductor_idm"


def test_33_nvda_roe_zero_na():
    result = run_pipeline("NVDA")
    for year in [y for y in result.scoring_years if y >= 2015]:
        roe = result.strategic.get(year, {}).get("ROE")
        if isinstance(roe, dict):
            roe = roe.get("value")
        assert roe is not None, f"NVDA {year}: ROE=None"


def test_34_nvda_ccc_all_scoring_years():
    result = run_pipeline("NVDA")
    for year in result.scoring_years:
        ccc = result.ratios.get(year, {}).get("ccc_days")
        if isinstance(ccc, RatioResult):
            assert ccc.value is not None, f"NVDA {year}: CCC=None"


def test_35_intc_dso_not_impossible():
    result = run_pipeline("INTC")
    for year in result.valid_years:
        dso = result.ratios.get(year, {}).get("days_sales_outstanding")
        if isinstance(dso, RatioResult):
            v = dso.value
            assert v is None or v <= 3650, f"INTC {year}: DSO={v} مستحيل"


def test_36_no_insufficient_peers_for_common():
    for ticker in ["NVDA", "AMD", "INTC"]:
        result = run_pipeline(ticker)
        pb = result.peers.get("benchmark", [])
        insuff = [m["metric"] for m in pb if m.get("position") == "INSUFFICIENT_PEERS"]
        assert len(insuff) == 0, f"{ticker}: {insuff} بدون أقران"


def test_37_scoring_uses_2015_2025():
    for ticker in ["NVDA", "AMD", "INTC"]:
        result = run_pipeline(ticker)
        years = result.scoring_years
        assert min(years) >= 2015, f"{ticker}: scoring starts at {min(years)}"
        assert max(years) <= 2025, f"{ticker}: scoring ends at {max(years)}"


def test_intc_ccc_not_impossible():
    result = run_pipeline("INTC")
    for year in result.valid_years:
        ccc = result.ratios.get(year, {}).get("ccc_days")
        if isinstance(ccc, RatioResult) and ccc.value is not None:
            assert float(ccc.value) <= 3650, f"INTC {year}: CCC={ccc.value} مستحيل"


def test_intc_is_idm():
    result = run_pipeline("INTC")
    assert result.sub_sector == "semiconductor_idm"


def test_intc_capex_no_penalty():
    result = run_pipeline("INTC")
    model = QUALITY_MODELS[result.sub_sector]
    assert model.get("capex_penalty_override") is True


def test_msft_gross_margin_not_na():
    result = run_pipeline("MSFT")
    for year in range(2019, 2026):
        if year in result.valid_years:
            gm = result.ratios.get(year, {}).get("gross_margin")
            if isinstance(gm, RatioResult):
                assert gm.value is not None, f"MSFT {year}: gross_margin=None"
                assert 0.60 <= float(gm.value) <= 0.80, f"MSFT {year}: gross_margin={gm.value:.1%} خارج النطاق"


def test_msft_revenue_magnitude():
    result = run_pipeline("MSFT")
    rev_2025 = result.ratios.get(2025, {}).get("revenue")
    if isinstance(rev_2025, dict):
        rev_2025 = rev_2025.get("value")
    assert rev_2025 and float(rev_2025) > 200_000, f"MSFT Revenue 2025 = {rev_2025:,.0f}M — يجب > $200B"


def test_msft_not_fail_2025():
    result = run_pipeline("MSFT")
    verdict_2025 = result.verdicts.get(2025, {}).get("verdict")
    assert verdict_2025 != "FAIL", f"MSFT 2025 = FAIL بسبب gross_margin مفقودة"


def test_msft_dso_not_impossible():
    result = run_pipeline("MSFT")
    for year in result.valid_years:
        dso = result.ratios.get(year, {}).get("days_sales_outstanding")
        if isinstance(dso, RatioResult) and dso.value is not None:
            assert float(dso.value) <= 365, f"MSFT {year}: DSO={dso.value:.0f} مستحيل"


def test_intc_dso_deleted():
    result = run_pipeline("INTC")
    dso_2025 = result.ratios.get(2025, {}).get("days_sales_outstanding")
    if isinstance(dso_2025, RatioResult):
        assert dso_2025.value is None, f"INTC 2025: DSO={dso_2025.value} يجب حذفه"


def test_normalized_label_matching():
    resolver = CanonicalLabelResolver()
    test_cases = [
        ("Gross Profit", "gross_profit"),
        ("GrossProfit", "gross_profit"),
        ("COGS", "cost_of_revenue"),
        ("Cost of Revenue", "cost_of_revenue"),
        ("CostOfRevenue", "cost_of_revenue"),
        ("Long-term debt", "long_term_debt"),
        ("LongTermDebt", "long_term_debt"),
    ]
    for raw, expected in test_cases:
        result = resolver.resolve(raw)
        assert result["canonical"] == expected, f'"{raw}" \u2192 {result["canonical"]} (\u064a\u062c\u0628 {expected})'


def test_revenue_quarterly_detection():
    class _Audit:
        def correction(self, *args, **kwargs):
            pass

        def flag(self, *args, **kwargs):
            pass

    rev_corrected = sanity_check_revenue("AAPL", 2025, 52_896, 233_715, _Audit())
    assert rev_corrected > 200_000, f"AAPL Revenue = {rev_corrected} (\u064a\u062c\u0628 > 200B)"


def test_total_debt_from_components():
    resolved = {"long_term_debt": 100_000, "short_term_borrowings": 10_000}
    debt = RatioEngine.calc_total_debt(resolved, "TEST", 2025)
    assert debt == 110_000
