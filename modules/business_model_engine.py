from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class ModelRule:
    gross_margin: Tuple[float, float]
    capex_to_revenue: Tuple[float, float]
    rd_to_revenue: Tuple[float, float]


class BusinessModelEngine:
    """
    Rule-based business model detector (no ML, no recalculation).

    Input metrics (structure only):
      - gross_margin
      - capex_to_revenue
      - rd_to_revenue

    Output:
      {
        "model": "semiconductor_fabless" | "hybrid:...+...",
        "confidence": 0.xx,
        "alternatives": [{"model": "...", "confidence": 0.xx}, ...]
      }
    """

    MODEL_RULES: Dict[str, ModelRule] = {
        "semiconductor_fabless": ModelRule(
            gross_margin=(0.50, 0.90),
            capex_to_revenue=(0.00, 0.12),
            rd_to_revenue=(0.10, 0.45),
        ),
        "semiconductor_idm": ModelRule(
            gross_margin=(0.35, 0.70),
            capex_to_revenue=(0.12, 0.50),
            rd_to_revenue=(0.07, 0.30),
        ),
        "commercial_bank": ModelRule(
            gross_margin=(-0.50, 0.45),  # broad proxy for bank-like structures
            capex_to_revenue=(0.00, 0.08),
            rd_to_revenue=(0.00, 0.06),
        ),
        "consumer_staples": ModelRule(
            gross_margin=(0.28, 0.70),
            capex_to_revenue=(0.03, 0.18),
            rd_to_revenue=(0.00, 0.08),
        ),
    }

    METRIC_WEIGHTS: Dict[str, float] = {
        "gross_margin": 0.40,
        "capex_to_revenue": 0.30,
        "rd_to_revenue": 0.30,
    }

    def _metric_score(self, value: Optional[float], lower: float, upper: float) -> float:
        """
        Returns score in [0,1].
        - 1.0 inside target band
        - decreases linearly outside with capped floor 0.0
        """
        if value is None:
            return 0.0
        if lower <= value <= upper:
            return 1.0

        band = max(upper - lower, 1e-9)
        if value < lower:
            dist = (lower - value) / band
        else:
            dist = (value - upper) / band
        return max(0.0, 1.0 - dist)

    def _score_model(self, metrics: Dict[str, Optional[float]], rule: ModelRule) -> float:
        gross_margin = self._metric_score(metrics.get("gross_margin"), *rule.gross_margin)
        capex_to_revenue = self._metric_score(metrics.get("capex_to_revenue"), *rule.capex_to_revenue)
        rd_to_revenue = self._metric_score(metrics.get("rd_to_revenue"), *rule.rd_to_revenue)

        score = (
            gross_margin * self.METRIC_WEIGHTS["gross_margin"]
            + capex_to_revenue * self.METRIC_WEIGHTS["capex_to_revenue"]
            + rd_to_revenue * self.METRIC_WEIGHTS["rd_to_revenue"]
        )
        return round(max(0.0, min(1.0, score)), 4)

    def classify(
        self,
        gross_margin: Optional[float],
        capex_to_revenue: Optional[float],
        rd_to_revenue: Optional[float],
        leverage: Optional[float] = None,  # kept for backward compatibility; intentionally ignored
    ) -> Dict[str, object]:
        metrics = {
            "gross_margin": gross_margin,
            "capex_to_revenue": capex_to_revenue,
            "rd_to_revenue": rd_to_revenue,
        }

        scored: List[Tuple[str, float]] = []
        for model_name, rule in self.MODEL_RULES.items():
            scored.append((model_name, self._score_model(metrics, rule)))
        scored.sort(key=lambda item: item[1], reverse=True)

        best_model, best_score = scored[0]
        second_model, second_score = scored[1]

        # Hybrid when top-2 are both strong and close.
        if best_score >= 0.65 and second_score >= 0.65 and (best_score - second_score) <= 0.04:
            selected_model = f"hybrid:{best_model}+{second_model}"
            selected_score = round((best_score + second_score) / 2.0, 2)
            alternatives = [{"model": m, "confidence": round(s, 2)} for m, s in scored[2:]]
        else:
            selected_model = best_model
            selected_score = round(best_score, 2)
            alternatives = [{"model": m, "confidence": round(s, 2)} for m, s in scored[1:]]

        return {
            "model": selected_model,
            "confidence": selected_score,
            "alternatives": alternatives,
        }

    def classify_from_repository(self, repository: Any, year: int) -> Dict[str, object]:
        """
        Classify using clean_data only from DataRepository.
        No recalculation is performed here.
        """
        y = int(year)

        def _fetch(metric: str) -> Optional[float]:
            key = f"{metric}:{y}"
            got = repository.get_clean(key)
            v = got.get("value")
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        return self.classify(
            gross_margin=_fetch("gross_margin"),
            capex_to_revenue=_fetch("capex_to_revenue"),
            rd_to_revenue=_fetch("rd_to_revenue"),
        )


__all__ = ["BusinessModelEngine"]
