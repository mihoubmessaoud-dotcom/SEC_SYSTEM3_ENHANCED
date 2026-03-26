"""Strategic analysis engine that consumes ratio outputs only."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class StrategicMetricDefinition:
    """Defines dependencies for one strategic metric."""

    name: str
    required_ratios: List[str]
    required_layers: List[str]


class StrategicAnalysisEngine:
    """Runs strategic analytics from computed ratios and layer payload metadata."""

    def __init__(self) -> None:
        self.metrics = [
            StrategicMetricDefinition(
                name="profitability_analysis",
                required_ratios=["gross_margin", "operating_margin", "roe"],
                required_layers=["SEC"],
            ),
            StrategicMetricDefinition(
                name="leverage_risk_assessment",
                required_ratios=["debt_to_equity"],
                required_layers=["SEC"],
            ),
            StrategicMetricDefinition(
                name="liquidity_health_scoring",
                required_ratios=["current_ratio"],
                required_layers=["SEC"],
            ),
            StrategicMetricDefinition(
                name="valuation_classification",
                required_ratios=["pe_ratio", "pb_ratio"],
                required_layers=["MARKET"],
            ),
            StrategicMetricDefinition(
                name="macro_adjusted_performance",
                required_ratios=["real_return", "sharpe_proxy"],
                required_layers=["MACRO", "MARKET"],
            ),
        ]

    def analyze(
        self,
        ratio_results: Dict[str, Dict[str, Any]],
        layer_payloads: Dict[str, Dict[str, Any]],
        year: int,
    ) -> Dict[str, Any]:
        """Compute strategic analytics from ratio outputs and layer availability."""
        output: Dict[str, Any] = {}

        for metric in self.metrics:
            missing_layers = [
                layer
                for layer in metric.required_layers
                if not layer_payloads.get(layer)
                or layer_payloads[layer].get("status") in {"DISABLED", "MISSING_API_KEY"}
            ]
            if missing_layers:
                output[metric.name] = self._not_computable(
                    reason="MISSING_REQUIRED_LAYER",
                    required_ratios=metric.required_ratios,
                    required_layers=metric.required_layers,
                    details={"missing_layers": missing_layers},
                    year=year,
                )
                continue

            missing_ratios = [
                r
                for r in metric.required_ratios
                if ratio_results.get(r, {}).get("status") != "COMPUTED"
            ]
            if missing_ratios:
                output[metric.name] = self._not_computable(
                    reason="MISSING_REQUIRED_RATIO",
                    required_ratios=metric.required_ratios,
                    required_layers=metric.required_layers,
                    details={"missing_ratios": missing_ratios},
                    year=year,
                )
                continue

            output[metric.name] = self._compute_metric(metric.name, ratio_results, year)

        return output

    def _compute_metric(
        self,
        metric_name: str,
        ratio_results: Dict[str, Dict[str, Any]],
        year: int,
    ) -> Dict[str, Any]:
        if metric_name == "profitability_analysis":
            gm = ratio_results["gross_margin"]["value"]
            om = ratio_results["operating_margin"]["value"]
            roe = ratio_results["roe"]["value"]
            score = (gm + om + roe) / 3
            label = "STRONG" if score >= 0.2 else "MODERATE" if score >= 0.1 else "WEAK"
            return self._computed(metric_name, score, label, year)

        if metric_name == "leverage_risk_assessment":
            dte = ratio_results["debt_to_equity"]["value"]
            label = "LOW_RISK" if dte < 1 else "MEDIUM_RISK" if dte < 2 else "HIGH_RISK"
            return self._computed(metric_name, dte, label, year)

        if metric_name == "liquidity_health_scoring":
            cr = ratio_results["current_ratio"]["value"]
            label = "HEALTHY" if cr >= 1.5 else "WATCH" if cr >= 1 else "STRESSED"
            return self._computed(metric_name, cr, label, year)

        if metric_name == "valuation_classification":
            pe = ratio_results["pe_ratio"]["value"]
            pb = ratio_results["pb_ratio"]["value"]
            composite = (pe + pb) / 2
            label = "UNDERVALUED" if composite < 15 else "FAIR" if composite < 25 else "OVERVALUED"
            return self._computed(metric_name, composite, label, year)

        if metric_name == "macro_adjusted_performance":
            rr = ratio_results["real_return"]["value"]
            sp = ratio_results["sharpe_proxy"]["value"]
            score = (rr + sp) / 2
            label = "OUTPERFORM" if score > 0.1 else "INLINE" if score > 0 else "UNDERPERFORM"
            return self._computed(metric_name, score, label, year)

        return self._not_computable(
            reason="DATA_NOT_APPLICABLE",
            required_ratios=[],
            required_layers=[],
            details={"message": "Unknown metric"},
            year=year,
        )

    def _computed(self, name: str, value: float, label: str, year: int) -> Dict[str, Any]:
        return {
            "status": "COMPUTED",
            "value": value,
            "classification": label,
            "period_used": f"{year}-12-31",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"metric": name},
            "data_source_trace": "ratio_engine_outputs",
        }

    def _not_computable(
        self,
        reason: str,
        required_ratios: List[str],
        required_layers: List[str],
        details: Dict[str, Any],
        year: int,
    ) -> Dict[str, Any]:
        return {
            "status": "NOT_COMPUTABLE",
            "reason": reason,
            "required_ratios": required_ratios,
            "required_layers": required_layers,
            "details": details,
            "period_used": f"{year}-12-31",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"required_ratios": required_ratios, "required_layers": required_layers},
            "data_source_trace": "ratio_engine_outputs",
        }
