from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class MetricSpec:
    weight: float
    min_value: float
    target_value: float
    max_value: float
    higher_is_better: bool = True


class ScoringEngine:
    """
    Model-aware scoring engine (0..100, rule-based).

    Key behaviors:
    - weighted scoring per business model
    - strict rejection when required metric value is None
    - deterministic, explainable score composition
    """

    MODEL_SPECS: Dict[str, Dict[str, MetricSpec]] = {
        "commercial_bank": {
            # Banks -> ROE spread + NIM (primary)
            "roe_spread": MetricSpec(weight=0.60, min_value=-0.05, target_value=0.04, max_value=0.10),
            "nim": MetricSpec(weight=0.40, min_value=0.005, target_value=0.025, max_value=0.050),
        },
        "semiconductor_fabless": {
            # Semiconductors -> ROIC + margins
            "roic": MetricSpec(weight=0.45, min_value=0.00, target_value=0.18, max_value=0.40),
            "gross_margin": MetricSpec(weight=0.30, min_value=0.30, target_value=0.55, max_value=0.80),
            "operating_margin": MetricSpec(weight=0.25, min_value=0.00, target_value=0.20, max_value=0.45),
        },
        "semiconductor_idm": {
            "roic": MetricSpec(weight=0.45, min_value=-0.05, target_value=0.10, max_value=0.30),
            "gross_margin": MetricSpec(weight=0.30, min_value=0.20, target_value=0.45, max_value=0.70),
            "operating_margin": MetricSpec(weight=0.25, min_value=-0.05, target_value=0.15, max_value=0.35),
        },
        "consumer_staples": {
            # Consumer -> ROIC + FCF + margins
            "roic": MetricSpec(weight=0.40, min_value=0.00, target_value=0.12, max_value=0.30),
            "fcf_yield": MetricSpec(weight=0.35, min_value=0.00, target_value=0.03, max_value=0.08),
            "gross_margin": MetricSpec(weight=0.25, min_value=0.20, target_value=0.40, max_value=0.65),
        },
        "asset_light": {
            "roic": MetricSpec(weight=0.40, min_value=0.00, target_value=0.15, max_value=0.40),
            "operating_margin": MetricSpec(weight=0.35, min_value=0.00, target_value=0.18, max_value=0.40),
            "fcf_yield": MetricSpec(weight=0.25, min_value=0.00, target_value=0.03, max_value=0.08),
        },
    }

    FALLBACK_MODEL = "asset_light"

    def _normalize_model(self, model: str) -> str:
        raw = str(model or "").strip().lower()
        if raw.startswith("hybrid:"):
            # Score against primary model in hybrid declaration.
            body = raw.replace("hybrid:", "")
            first = body.split("+")[0].strip()
            return first or self.FALLBACK_MODEL
        return raw or self.FALLBACK_MODEL

    @staticmethod
    def _metric_to_score(value: float, spec: MetricSpec) -> float:
        """
        Converts one metric to score in [0..100] using piecewise linear mapping.
        """
        v = float(value)
        if not spec.higher_is_better:
            # Invert metric by mirroring around target.
            v = (spec.max_value + spec.min_value) - v

        if v <= spec.min_value:
            return 0.0
        if v >= spec.max_value:
            return 100.0

        # Ensure target in the interior.
        target = min(max(spec.target_value, spec.min_value), spec.max_value)

        if v <= target:
            # 0..80 zone (below target)
            denom = max(target - spec.min_value, 1e-9)
            return (v - spec.min_value) / denom * 80.0

        # 80..100 zone (above target, diminishing extra reward)
        denom = max(spec.max_value - target, 1e-9)
        return 80.0 + (v - target) / denom * 20.0

    @staticmethod
    def _extract_numeric(metric_payload: object) -> Optional[float]:
        """
        Accepts plain numeric values or RatioEngine-style records:
          {"value": <float|None>, ...}
        """
        if metric_payload is None:
            return None
        if isinstance(metric_payload, (int, float)):
            return float(metric_payload)
        if isinstance(metric_payload, dict):
            v = metric_payload.get("value")
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None
        return None

    def score(self, model: str, metrics: Dict[str, object]) -> Dict[str, object]:
        model_key = self._normalize_model(model)
        spec_map = self.MODEL_SPECS.get(model_key) or self.MODEL_SPECS[self.FALLBACK_MODEL]

        # Ignore missing/None metrics and score only available validated metrics.
        # This keeps scoring operational without fabricating data.
        available: List[Tuple[str, MetricSpec, float]] = []
        missing: List[str] = []
        for metric_name, spec in spec_map.items():
            numeric_value = self._extract_numeric(metrics.get(metric_name))
            if numeric_value is None:
                missing.append(metric_name)
                continue
            available.append((metric_name, spec, numeric_value))

        if not available:
            return {
                "score": None,
                "status": "INVALID",
                "reason": "NO_SOURCE_DATA",
                "missing_metrics": missing,
                "model_used": model_key,
                "details": [],
            }

        # Re-normalize active weights so score remains in [0..100]
        active_weight_sum = sum(spec.weight for _, spec, _ in available)
        if active_weight_sum <= 0:
            return {
                "score": None,
                "status": "INVALID",
                "reason": "INVALID_WEIGHT_CONFIGURATION",
                "missing_metrics": missing,
                "model_used": model_key,
                "details": [],
            }

        weighted_sum = 0.0
        details = []
        for metric_name, spec, metric_value in available:
            metric_score = self._metric_to_score(metric_value, spec)
            normalized_weight = spec.weight / active_weight_sum
            contribution = metric_score * normalized_weight
            weighted_sum += contribution
            details.append(
                {
                    "metric": metric_name,
                    "value": metric_value,
                    "metric_score": round(metric_score, 2),
                    "weight": round(normalized_weight, 6),
                    "contribution": round(contribution, 2),
                }
            )

        score_0_100 = round(max(0.0, min(100.0, weighted_sum)), 2)
        return {
            "score": score_0_100,
            "status": "OK",
            "reason": "",
            "missing_metrics": [],
            "ignored_none_metrics": missing,
            "model_used": model_key,
            "details": details,
        }

    def score_from_ratio_engine(
        self,
        model: str,
        ratio_results: Dict[str, Dict[str, object]],
        extra_metrics: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        """
        Score using RatioEngine outputs only (plus optional precomputed extras).
        This method does not perform any recalculation.
        """
        merged: Dict[str, object] = {}
        for metric_name, payload in (ratio_results or {}).items():
            merged[str(metric_name).strip().lower()] = payload
        for metric_name, payload in (extra_metrics or {}).items():
            merged[str(metric_name).strip().lower()] = payload
        return self.score(model, merged)


__all__ = ["ScoringEngine"]
