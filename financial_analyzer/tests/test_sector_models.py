from financial_analyzer.core.sector_quality_models import detect_sub_sector


def test_detect_sub_sector_smoke():
    assert detect_sub_sector('AAPL','unknown') == 'hardware_platform'

