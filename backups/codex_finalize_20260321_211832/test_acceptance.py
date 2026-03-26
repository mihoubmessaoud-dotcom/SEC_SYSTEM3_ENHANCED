import pytest

from financial_analyzer.core.pipeline_orchestrator import PipelineOrchestrator
from financial_analyzer.models.ratio_result import RatioResult


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
