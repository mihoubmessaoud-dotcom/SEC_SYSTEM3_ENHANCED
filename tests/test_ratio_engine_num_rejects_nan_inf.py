import math

from modules.ratio_engine import RatioEngine


def test_ratio_engine_num_rejects_nan_and_inf():
    assert RatioEngine._num(float("nan")) is None
    assert RatioEngine._num(float("inf")) is None
    assert RatioEngine._num(float("-inf")) is None
    assert math.isclose(RatioEngine._num(123.45), 123.45)

