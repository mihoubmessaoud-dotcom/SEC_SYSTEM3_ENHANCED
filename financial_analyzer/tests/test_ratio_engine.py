from financial_analyzer.core.ratio_engine import RatioEngine


def test_ratio_engine_smoke():
    e=RatioEngine()
    assert e is not None

