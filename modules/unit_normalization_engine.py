from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class NormalizationRecord:
    original_value: Optional[float]
    normalized_value: Optional[float]
    detected_scale: str
    target_unit: str = "millions"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class UnitNormalizationEngine:
    """
    Normalize financial values to millions BEFORE validation/calculations.

    Notes:
    - Missing values are never filled.
    - Non-monetary/unitless metrics are kept unchanged.
    """

    NON_MONETARY_HINTS = (
        "margin",
        "ratio",
        "yield",
        "growth",
        "days",
        "turnover",
        "leverage",
        "z",
        "beta",
        "tax_rate",
        "nim",
        "spread",
        # Common ratios/metrics that do not include the word "ratio"/"margin" but must
        # never be unit-normalized as money.
        "roic",
        "roe",
        "roa",
        "wacc",
        "eps",
        "bvps",
        "pe",
        "pb",
        "ps",
        "peg",
        "coverage",
    )

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            s = value.strip().lower()
            return s in {"", "none", "null", "nan", "n/a", "na", "--"}
        return False

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if UnitNormalizationEngine._is_missing(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _is_non_monetary_metric(self, metric: str) -> bool:
        m = str(metric or "").strip().lower()
        return any(h in m for h in self.NON_MONETARY_HINTS)

    def normalize_value(self, value: Any, metric: str = "") -> Dict[str, Any]:
        if self._is_missing(value):
            return NormalizationRecord(
                original_value=None,
                normalized_value=None,
                detected_scale="NO_SOURCE_DATA",
            ).to_dict()

        numeric = self._to_float(value)
        if numeric is None:
            return NormalizationRecord(
                original_value=None,
                normalized_value=None,
                detected_scale="NON_NUMERIC",
            ).to_dict()

        if self._is_non_monetary_metric(metric):
            return NormalizationRecord(
                original_value=numeric,
                normalized_value=numeric,
                detected_scale="unitless_or_ratio",
            ).to_dict()

        abs_v = abs(numeric)
        if abs_v > 1e9:
            normalized = numeric / 1_000_000.0
            scale = "billions_or_units_to_millions"
        elif abs_v > 1e6:
            # As required: thousands then millions (equivalent to /1e6).
            normalized = (numeric / 1_000.0) / 1_000.0
            scale = "thousands_to_millions"
        else:
            normalized = numeric / 1_000_000.0
            scale = "units_to_millions"

        return NormalizationRecord(
            original_value=numeric,
            normalized_value=normalized,
            detected_scale=scale,
        ).to_dict()

    def normalize_series(self, metric: str, values_by_year: Dict[int, Any]) -> Dict[int, Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}
        for year, value in sorted((values_by_year or {}).items()):
            out[int(year)] = self.normalize_value(value, metric=metric)
        return out

    def normalize_dataset(self, raw_metrics_by_year: Dict[str, Dict[int, Any]]) -> Dict[str, Any]:
        normalized_metrics: Dict[str, Dict[int, Any]] = {}
        details: Dict[str, Dict[int, Dict[str, Any]]] = {}
        for metric, series in (raw_metrics_by_year or {}).items():
            metric_details = self.normalize_series(metric, series or {})
            details[metric] = metric_details
            normalized_metrics[metric] = {
                int(y): (item.get("normalized_value")) for y, item in metric_details.items()
            }
        return {
            "normalized_metrics": normalized_metrics,
            "normalization_details": details,
            "target_unit": "millions",
        }


__all__ = ["UnitNormalizationEngine", "NormalizationRecord"]
