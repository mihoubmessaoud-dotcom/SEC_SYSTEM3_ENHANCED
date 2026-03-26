from financial_analyzer.core.data_anchor_validator import DataAnchorValidator


def test_anchor_validator_smoke():
    v=DataAnchorValidator()
    assert v is not None

