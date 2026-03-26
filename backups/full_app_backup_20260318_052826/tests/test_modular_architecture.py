"""Tests for modular multi-layer analytics architecture."""

from __future__ import annotations

from core.ratio_engine import RatioEngine
from core.strategy_engine import StrategicAnalysisEngine


def _sec_payload() -> dict:
    return {
        "status": "OK",
        "source_endpoint": "sec-endpoint",
        "periods": {
            "2024": {
                "facts": {
                    "us-gaap:AssetsCurrent": {
                        "value": 200.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:LiabilitiesCurrent": {
                        "value": 100.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:Liabilities": {
                        "value": 300.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:StockholdersEquity": {
                        "value": 200.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:GrossProfit": {
                        "value": 180.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:Revenues": {
                        "value": 400.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:NetIncomeLoss": {
                        "value": 120.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:Assets": {
                        "value": 500.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:OperatingIncomeLoss": {
                        "value": 110.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:EarningsPerShareBasic": {
                        "value": 5.0,
                        "unit": "USD/share",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:NetCashProvidedByUsedInOperatingActivities": {
                        "value": 130.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment": {
                        "value": 30.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                    "us-gaap:DepreciationAndAmortization": {
                        "value": 15.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                        "accn": "0000",
                    },
                }
            }
        },
    }


def _market_payload() -> dict:
    return {
        "status": "OK",
        "source_endpoint": "polygon-endpoint",
        "periods": {
            "2024": {
                "fields": {
                    "market:price": {
                        "value": 100.0,
                        "unit": "USD/share",
                        "period_end": "2024-12-31",
                    },
                    "market:market_cap": {
                        "value": 1000.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                    },
                    "market:enterprise_value": {
                        "value": 1200.0,
                        "unit": "USD",
                        "period_end": "2024-12-31",
                    },
                    "market:dividend_yield": {
                        "value": 0.02,
                        "unit": "ratio",
                        "period_end": "2024-12-31",
                    },
                    "market:cost_of_equity": {
                        "value": 0.1,
                        "unit": "ratio",
                        "period_end": "2024-12-31",
                    },
                    "market:cost_of_debt": {
                        "value": 0.05,
                        "unit": "ratio",
                        "period_end": "2024-12-31",
                    },
                    "market:total_return": {
                        "value": 0.08,
                        "unit": "ratio",
                        "period_end": "2024-12-31",
                    },
                    "market:beta": {
                        "value": 1.2,
                        "unit": "ratio",
                        "period_end": "2024-12-31",
                    },
                }
            }
        },
    }


def _macro_payload() -> dict:
    return {
        "status": "OK",
        "source_endpoint": "fred-endpoint",
        "periods": {
            "2024": {
                "fields": {
                    "macro:risk_free_rate": {
                        "value": 0.04,
                        "unit": "ratio",
                        "period_end": "2024-12-31",
                    },
                    "macro:inflation_rate": {
                        "value": 0.03,
                        "unit": "ratio",
                        "period_end": "2024-12-31",
                    },
                }
            }
        },
    }


def test_accounting_only_ratios_work_with_only_sec_layer() -> None:
    engine = RatioEngine(layer_registry={"SEC": object()})
    payloads = {"SEC": _sec_payload()}

    out = engine.compute_all(layer_payloads=payloads, year=2024)

    assert out["ratio_results"]["current_ratio"]["status"] == "COMPUTED"
    assert out["ratio_results"]["debt_to_equity"]["status"] == "COMPUTED"
    assert out["ratio_results"]["gross_margin"]["status"] == "COMPUTED"


def test_market_dependent_ratios_fail_when_market_layer_disabled() -> None:
    engine = RatioEngine(layer_registry={"SEC": object()})
    payloads = {"SEC": _sec_payload()}

    out = engine.compute_all(layer_payloads=payloads, year=2024)

    assert out["ratio_results"]["pe_ratio"]["status"] == "NOT_COMPUTABLE"
    assert out["ratio_results"]["pe_ratio"]["reason"] == "MISSING_MARKET_LAYER"


def test_hybrid_ratios_fail_when_macro_missing() -> None:
    engine = RatioEngine(layer_registry={"SEC": object(), "MARKET": object()})
    payloads = {"SEC": _sec_payload(), "MARKET": _market_payload()}

    out = engine.compute_all(layer_payloads=payloads, year=2024)

    assert out["ratio_results"]["wacc"]["status"] == "NOT_COMPUTABLE"
    assert out["ratio_results"]["wacc"]["reason"] == "MISSING_REQUIRED_LAYER"


def test_strategy_engine_handles_incomplete_ratios() -> None:
    ratio_engine = RatioEngine(layer_registry={"SEC": object()})
    payloads = {"SEC": _sec_payload()}
    ratio_out = ratio_engine.compute_all(layer_payloads=payloads, year=2024)

    strategy_engine = StrategicAnalysisEngine()
    strategy_out = strategy_engine.analyze(
        ratio_results=ratio_out["ratio_results"],
        layer_payloads=payloads,
        year=2024,
    )

    assert strategy_out["profitability_analysis"]["status"] == "COMPUTED"
    assert strategy_out["valuation_classification"]["status"] == "NOT_COMPUTABLE"
    assert strategy_out["valuation_classification"]["reason"] == "MISSING_REQUIRED_LAYER"


def test_hybrid_ratios_compute_when_all_layers_available() -> None:
    engine = RatioEngine(layer_registry={"SEC": object(), "MARKET": object(), "MACRO": object()})
    payloads = {
        "SEC": _sec_payload(),
        "MARKET": _market_payload(),
        "MACRO": _macro_payload(),
    }

    out = engine.compute_all(layer_payloads=payloads, year=2024)

    assert out["ratio_results"]["wacc"]["status"] == "COMPUTED"
    assert out["ratio_results"]["real_return"]["status"] == "COMPUTED"
