from financial_analyzer.core.verdict_engine import VerdictEngine


def test_verdict_engine_smoke():
    v=VerdictEngine()
    assert v is not None

