"""Centralized ratio engine consuming only structured layer outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from core.na_decision_tree import NADecisionTree


@dataclass(frozen=True)
class RatioDefinition:
    """Defines formula metadata and dependency requirements for one ratio."""

    name: str
    ratio_type: str
    required_layers: List[str]
    required_fields: List[str]
    denominator_fields: List[str]
    formula_used: str
    calculator: Callable[[Dict[str, Dict[str, Any]]], float]


class RatioEngine:
    """Computes ratios from standardized layer payloads with full auditability."""

    def __init__(self, layer_registry: Dict[str, Any]) -> None:
        self.layer_registry = layer_registry
        self.decision_tree = NADecisionTree()
        self.definitions = self._build_definitions()

    def compute_all(
        self,
        layer_payloads: Dict[str, Dict[str, Any]],
        year: int,
    ) -> Dict[str, Any]:
        """Compute every supported ratio for a single fiscal year."""
        results: Dict[str, Any] = {}
        raw_inputs: Dict[str, Any] = {}
        decision_report: Dict[str, Any] = {}

        for ratio in self.definitions:
            inputs, trace = self._collect_inputs(ratio, layer_payloads, year)
            decision = self.decision_tree.evaluate(
                ratio_name=ratio.name,
                ratio_type=ratio.ratio_type,
                required_layers=ratio.required_layers,
                required_fields=ratio.required_fields,
                layer_payloads=layer_payloads,
                inputs=inputs,
                denominator_keys=ratio.denominator_fields,
            )

            period_used = self._period_from_inputs(inputs)
            unit_info = {key: value.get("unit") for key, value in inputs.items()}
            dependency_map = {
                "required_layers": ratio.required_layers,
                "required_fields": ratio.required_fields,
                "resolved_inputs": list(inputs.keys()),
            }

            raw_inputs[ratio.name] = {
                "period": period_used,
                "inputs": inputs,
                "data_source_trace": trace,
            }
            decision_report[ratio.name] = decision.decision_path

            if not decision.computable:
                results[ratio.name] = self.decision_tree.build_not_computable(
                    reason=decision.reason or "DATA_NOT_APPLICABLE",
                    missing_inputs=decision.missing_inputs,
                    period_used=period_used,
                    unit_info=unit_info,
                    audit_trace=trace,
                    formula_used=ratio.formula_used,
                    dependency_map=dependency_map,
                    decision_path=decision.decision_path,
                )
                results[ratio.name]["ratio_type"] = ratio.ratio_type
                continue

            try:
                value = ratio.calculator(inputs)
            except ZeroDivisionError:
                results[ratio.name] = self.decision_tree.build_not_computable(
                    reason="ZERO_DENOMINATOR",
                    missing_inputs=[],
                    period_used=period_used,
                    unit_info=unit_info,
                    audit_trace=trace,
                    formula_used=ratio.formula_used,
                    dependency_map=dependency_map,
                    decision_path=decision.decision_path,
                )
                results[ratio.name]["ratio_type"] = ratio.ratio_type
                continue
            except Exception:
                results[ratio.name] = self.decision_tree.build_not_computable(
                    reason="DATA_NOT_APPLICABLE",
                    missing_inputs=[],
                    period_used=period_used,
                    unit_info=unit_info,
                    audit_trace=trace,
                    formula_used=ratio.formula_used,
                    dependency_map=dependency_map,
                    decision_path=decision.decision_path,
                )
                results[ratio.name]["ratio_type"] = ratio.ratio_type
                continue
            results[ratio.name] = {
                "status": "COMPUTED",
                "value": value,
                "ratio_type": ratio.ratio_type,
                "formula_used": ratio.formula_used,
                "input_concepts": ratio.required_fields,
                "raw_values_used": inputs,
                "period_used": period_used,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data_source_trace": trace,
                "dependency_map": dependency_map,
                "decision_tree": decision.decision_path,
            }

        return {
            "ratio_results": results,
            "raw_inputs": raw_inputs,
            "na_decision_tree_report": decision_report,
        }

    def _collect_inputs(
        self,
        ratio: RatioDefinition,
        layer_payloads: Dict[str, Dict[str, Any]],
        year: int,
    ) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        inputs: Dict[str, Dict[str, Any]] = {}
        trace: Dict[str, Any] = {}

        sec_payload = layer_payloads.get("SEC", {})
        sec_period = sec_payload.get("periods", {}).get(str(year), {})
        sec_facts = sec_period.get("facts", {})

        market_payload = layer_payloads.get("MARKET", {})
        market_period = market_payload.get("periods", {}).get(str(year), {})
        market_fields = market_period.get("fields", {})

        yahoo_payload = layer_payloads.get("YAHOO", {})
        yahoo_period = yahoo_payload.get("periods", {}).get(str(year), {})
        yahoo_fields = yahoo_period.get("fields", {})

        macro_payload = layer_payloads.get("MACRO", {})
        macro_period = macro_payload.get("periods", {}).get(str(year), {})
        macro_fields = macro_period.get("fields", {})

        yahoo_alias_map = {
            "market:price": "yahoo:price",
            "market:market_cap": "yahoo:market_cap",
            "market:enterprise_value": "yahoo:enterprise_value",
            "market:beta": "yahoo:beta",
            "market:dividend_yield": "yahoo:dividend_yield",
            "market:volume": "yahoo:volume",
            "market:total_return": "yahoo:total_return",
            "us-gaap:EarningsPerShareBasic": "yahoo:eps_ttm",
            "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic": "yahoo:shares_outstanding",
        }

        for field in ratio.required_fields:
            if field in sec_facts:
                inputs[field] = sec_facts[field]
                trace[field] = {
                    "layer": "SEC",
                    "endpoint": sec_payload.get("source_endpoint"),
                    "filing": sec_facts[field].get("accn"),
                }
            elif field in market_fields:
                inputs[field] = market_fields[field]
                trace[field] = {
                    "layer": "MARKET",
                    "endpoint": market_payload.get("source_endpoint"),
                }
            elif yahoo_alias_map.get(field) in yahoo_fields:
                yf_key = yahoo_alias_map[field]
                inputs[field] = yahoo_fields[yf_key]
                trace[field] = {
                    "layer": "YAHOO",
                    "endpoint": yahoo_payload.get("source_endpoint"),
                    "mapped_from": yf_key,
                }
            elif field in macro_fields:
                inputs[field] = macro_fields[field]
                trace[field] = {
                    "layer": "MACRO",
                    "endpoint": macro_payload.get("source_endpoint"),
                }

        return inputs, trace

    def _period_from_inputs(self, inputs: Dict[str, Dict[str, Any]]) -> Optional[str]:
        periods = {v.get("period_end") for v in inputs.values() if v.get("period_end")}
        if len(periods) == 1:
            return next(iter(periods))
        return None

    def _build_definitions(self) -> List[RatioDefinition]:
        return [
            RatioDefinition(
                name="current_ratio",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=[
                    "us-gaap:AssetsCurrent",
                    "us-gaap:LiabilitiesCurrent",
                ],
                denominator_fields=["us-gaap:LiabilitiesCurrent"],
                formula_used="AssetsCurrent / LiabilitiesCurrent",
                calculator=lambda i: i["us-gaap:AssetsCurrent"]["value"]
                / i["us-gaap:LiabilitiesCurrent"]["value"],
            ),
            RatioDefinition(
                name="debt_to_equity",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=[
                    "us-gaap:Liabilities",
                    "us-gaap:StockholdersEquity",
                ],
                denominator_fields=["us-gaap:StockholdersEquity"],
                formula_used="Liabilities / StockholdersEquity",
                calculator=lambda i: i["us-gaap:Liabilities"]["value"]
                / i["us-gaap:StockholdersEquity"]["value"],
            ),
            RatioDefinition(
                name="gross_margin",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=[
                    "us-gaap:GrossProfit",
                    "us-gaap:Revenues",
                ],
                denominator_fields=["us-gaap:Revenues"],
                formula_used="GrossProfit / Revenues",
                calculator=lambda i: i["us-gaap:GrossProfit"]["value"]
                / i["us-gaap:Revenues"]["value"],
            ),
            RatioDefinition(
                name="roa",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=[
                    "us-gaap:NetIncomeLoss",
                    "us-gaap:Assets",
                ],
                denominator_fields=["us-gaap:Assets"],
                formula_used="NetIncomeLoss / Assets",
                calculator=lambda i: i["us-gaap:NetIncomeLoss"]["value"]
                / i["us-gaap:Assets"]["value"],
            ),
            RatioDefinition(
                name="roe",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=[
                    "us-gaap:NetIncomeLoss",
                    "us-gaap:StockholdersEquity",
                ],
                denominator_fields=["us-gaap:StockholdersEquity"],
                formula_used="NetIncomeLoss / StockholdersEquity",
                calculator=lambda i: i["us-gaap:NetIncomeLoss"]["value"]
                / i["us-gaap:StockholdersEquity"]["value"],
            ),
            RatioDefinition(
                name="operating_margin",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=[
                    "us-gaap:OperatingIncomeLoss",
                    "us-gaap:Revenues",
                ],
                denominator_fields=["us-gaap:Revenues"],
                formula_used="OperatingIncomeLoss / Revenues",
                calculator=lambda i: i["us-gaap:OperatingIncomeLoss"]["value"]
                / i["us-gaap:Revenues"]["value"],
            ),
            RatioDefinition(
                name="eps_basic",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=["us-gaap:EarningsPerShareBasic"],
                denominator_fields=[],
                formula_used="EarningsPerShareBasic",
                calculator=lambda i: i["us-gaap:EarningsPerShareBasic"]["value"],
            ),
            RatioDefinition(
                name="pe_ratio",
                ratio_type="MARKET_DEPENDENT",
                required_layers=["SEC", "MARKET"],
                required_fields=[
                    "us-gaap:EarningsPerShareBasic",
                    "market:price",
                ],
                denominator_fields=["us-gaap:EarningsPerShareBasic"],
                formula_used="MarketPrice / EPSBasic",
                calculator=lambda i: i["market:price"]["value"]
                / i["us-gaap:EarningsPerShareBasic"]["value"],
            ),
            RatioDefinition(
                name="pb_ratio",
                ratio_type="MARKET_DEPENDENT",
                required_layers=["SEC", "MARKET"],
                required_fields=[
                    "market:market_cap",
                    "us-gaap:StockholdersEquity",
                ],
                denominator_fields=["us-gaap:StockholdersEquity"],
                formula_used="MarketCap / StockholdersEquity",
                calculator=lambda i: i["market:market_cap"]["value"]
                / i["us-gaap:StockholdersEquity"]["value"],
            ),
            RatioDefinition(
                name="market_cap",
                ratio_type="MARKET_DEPENDENT",
                required_layers=["MARKET"],
                required_fields=["market:market_cap"],
                denominator_fields=[],
                formula_used="market_cap",
                calculator=lambda i: i["market:market_cap"]["value"],
            ),
            RatioDefinition(
                name="book_value_per_share",
                ratio_type="ACCOUNTING_ONLY",
                required_layers=["SEC"],
                required_fields=[
                    "us-gaap:StockholdersEquity",
                    "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
                ],
                denominator_fields=["us-gaap:WeightedAverageNumberOfSharesOutstandingBasic"],
                formula_used="StockholdersEquity / WeightedAverageNumberOfSharesOutstandingBasic",
                calculator=lambda i: i["us-gaap:StockholdersEquity"]["value"]
                / i["us-gaap:WeightedAverageNumberOfSharesOutstandingBasic"]["value"],
            ),
            RatioDefinition(
                name="dividend_yield",
                ratio_type="MARKET_DEPENDENT",
                required_layers=["MARKET"],
                required_fields=["market:dividend_yield"],
                denominator_fields=[],
                formula_used="dividend_yield",
                calculator=lambda i: i["market:dividend_yield"]["value"],
            ),
            RatioDefinition(
                name="fcf_yield",
                ratio_type="MARKET_DEPENDENT",
                required_layers=["SEC", "MARKET"],
                required_fields=[
                    "us-gaap:NetCashProvidedByUsedInOperatingActivities",
                    "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
                    "market:market_cap",
                ],
                denominator_fields=["market:market_cap"],
                formula_used="(OperatingCashFlow - Capex) / MarketCap",
                calculator=lambda i: (
                    i["us-gaap:NetCashProvidedByUsedInOperatingActivities"]["value"]
                    - i["us-gaap:PaymentsToAcquirePropertyPlantAndEquipment"]["value"]
                )
                / i["market:market_cap"]["value"],
            ),
            RatioDefinition(
                name="ev_to_ebitda",
                ratio_type="MARKET_DEPENDENT",
                required_layers=["SEC", "MARKET"],
                required_fields=[
                    "market:enterprise_value",
                    "us-gaap:OperatingIncomeLoss",
                    "us-gaap:DepreciationAndAmortization",
                ],
                denominator_fields=["us-gaap:OperatingIncomeLoss"],
                formula_used="EnterpriseValue / (OperatingIncomeLoss + DepreciationAndAmortization)",
                calculator=lambda i: i["market:enterprise_value"]["value"]
                / (
                    i["us-gaap:OperatingIncomeLoss"]["value"]
                    + i["us-gaap:DepreciationAndAmortization"]["value"]
                ),
            ),
            RatioDefinition(
                name="wacc",
                ratio_type="HYBRID",
                required_layers=["SEC", "MARKET", "MACRO"],
                required_fields=[
                    "market:market_cap",
                    "market:beta",
                    "us-gaap:Liabilities",
                    "macro:risk_free_rate",
                ],
                denominator_fields=[],
                formula_used=(
                    "((E/(E+D))*(Rf+Beta*ERP))+((D/(E+D))*(Rf+DebtSpread)*(1-TaxRate))"
                ),
                calculator=lambda i: (
                    (
                        i["market:market_cap"]["value"]
                        / (
                            i["market:market_cap"]["value"]
                            + i["us-gaap:Liabilities"]["value"]
                        )
                    )
                    * (
                        i["macro:risk_free_rate"]["value"]
                        + (i["market:beta"]["value"] * 0.05)
                    )
                    + (
                        i["us-gaap:Liabilities"]["value"]
                        / (
                            i["market:market_cap"]["value"]
                            + i["us-gaap:Liabilities"]["value"]
                        )
                    )
                    * (i["macro:risk_free_rate"]["value"] + 0.02)
                    * (1 - 0.21)
                ),
            ),
            RatioDefinition(
                name="economic_value_added",
                ratio_type="HYBRID",
                required_layers=["SEC", "MARKET", "MACRO"],
                required_fields=[
                    "us-gaap:OperatingIncomeLoss",
                    "us-gaap:Assets",
                    "market:market_cap",
                    "market:beta",
                    "us-gaap:Liabilities",
                    "macro:risk_free_rate",
                ],
                denominator_fields=[],
                formula_used=(
                    "OperatingIncomeLoss - (Assets * WACC_Proxy)"
                ),
                calculator=lambda i: i["us-gaap:OperatingIncomeLoss"]["value"]
                - (
                    i["us-gaap:Assets"]["value"]
                    * (
                        (
                            (
                                i["market:market_cap"]["value"]
                                / (
                                    i["market:market_cap"]["value"]
                                    + i["us-gaap:Liabilities"]["value"]
                                )
                            )
                            * (
                                i["macro:risk_free_rate"]["value"]
                                + (i["market:beta"]["value"] * 0.05)
                            )
                        )
                        + (
                            (
                                i["us-gaap:Liabilities"]["value"]
                                / (
                                    i["market:market_cap"]["value"]
                                    + i["us-gaap:Liabilities"]["value"]
                                )
                            )
                            * (i["macro:risk_free_rate"]["value"] + 0.02)
                            * (1 - 0.21)
                        )
                    )
                ),
            ),
            RatioDefinition(
                name="real_return",
                ratio_type="HYBRID",
                required_layers=["MARKET", "MACRO"],
                required_fields=[
                    "market:total_return",
                    "macro:inflation_rate",
                ],
                denominator_fields=[],
                formula_used="TotalReturn - InflationRate",
                calculator=lambda i: i["market:total_return"]["value"]
                - i["macro:inflation_rate"]["value"],
            ),
            RatioDefinition(
                name="sharpe_proxy",
                ratio_type="HYBRID",
                required_layers=["MARKET", "MACRO"],
                required_fields=[
                    "market:total_return",
                    "macro:risk_free_rate",
                    "market:beta",
                ],
                denominator_fields=["market:beta"],
                formula_used="(TotalReturn - RiskFreeRate) / Beta",
                calculator=lambda i: (
                    i["market:total_return"]["value"]
                    - i["macro:risk_free_rate"]["value"]
                )
                / i["market:beta"]["value"],
            ),
        ]
