from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class CorrectionEntry:
    metric: str
    year: int
    original_value: float
    corrected_value: float
    reason: str
    timestamp_utc: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CorrectionLog:
    """Central logging system for deterministic corrections and anomaly flags."""

    def __init__(self) -> None:
        self._corrections: List[CorrectionEntry] = []
        self._flags: List[Dict[str, Any]] = []

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def clear(self) -> None:
        self._corrections = []
        self._flags = []

    def add_correction(
        self,
        metric: str,
        year: int,
        original_value: float,
        corrected_value: float,
        reason: str,
    ) -> None:
        self._corrections.append(
            CorrectionEntry(
                metric=str(metric),
                year=int(year),
                original_value=float(original_value),
                corrected_value=float(corrected_value),
                reason=str(reason),
                timestamp_utc=self._now(),
            )
        )

    def add_flag(self, metric: str, year: int, value: float, reason: str) -> None:
        self._flags.append(
            {
                "timestamp_utc": self._now(),
                "metric": str(metric),
                "year": int(year),
                "value": float(value),
                "reason": str(reason),
            }
        )

    def corrections(self) -> List[Dict[str, Any]]:
        return [entry.to_dict() for entry in self._corrections]

    def flags(self) -> List[Dict[str, Any]]:
        return list(self._flags)

    def summary(self) -> Dict[str, Any]:
        return {
            "total_corrections": len(self._corrections),
            "total_flags": len(self._flags),
        }


class DataCorrectionEngine:
    """
    The ONLY layer allowed to modify numeric source values.

    Scope:
    - Fix deterministic unit-scale errors.
    - Fix deterministic extreme jumps when clearly unit-related.
    - Never fill missing values.
    - Never estimate/guess.
    """

    SCALE_RULES: Dict[str, Dict[str, float]] = {
        # In this project most statement magnitudes are in million USD.
        "revenue": {"max_abs": 5_000_000.0},
        "total_assets": {"max_abs": 20_000_000.0},
        "total_liabilities": {"max_abs": 20_000_000.0},
        "total_equity": {"max_abs": 20_000_000.0},
        "market_cap": {"max_abs": 50_000_000.0},
        "net_income": {"max_abs": 5_000_000.0},
        "free_cash_flow": {"max_abs": 5_000_000.0},
    }

    METRIC_ALIASES: Dict[str, str] = {
        "revenues": "revenue",
        "salesrevenuenet": "revenue",
        "market:market_cap": "market_cap",
        "market:total_debt": "total_debt",
    }

    def __init__(self, jump_ratio_threshold: float = 8.0) -> None:
        self.jump_ratio_threshold = float(max(1.5, jump_ratio_threshold))
        self._log = CorrectionLog()

    @property
    def correction_log(self) -> List[Dict[str, Any]]:
        return self._log.corrections()

    @property
    def flags(self) -> List[Dict[str, Any]]:
        return self._log.flags()

    def get_corrections(self) -> List[Dict[str, Any]]:
        return self._log.corrections()

    def get_flags(self) -> List[Dict[str, Any]]:
        return self._log.flags()

    def get_log_summary(self) -> Dict[str, Any]:
        return self._log.summary()

    def _canonical_metric(self, metric: str) -> str:
        key = str(metric).strip().lower()
        return self.METRIC_ALIASES.get(key, key)

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            s = value.strip().lower()
            return s in {"", "none", "null", "nan", "n/a", "na", "--"}
        return False

    def _to_float(self, value: Any) -> Optional[float]:
        if self._is_missing(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _append_correction(
        self,
        metric: str,
        year: int,
        original_value: float,
        corrected_value: float,
        reason: str,
    ) -> None:
        self._log.add_correction(
            metric=metric,
            year=year,
            original_value=original_value,
            corrected_value=corrected_value,
            reason=reason,
        )

    def _append_flag(self, metric: str, year: int, value: float, reason: str) -> None:
        self._log.add_flag(metric=metric, year=year, value=value, reason=reason)

    def _fix_scale_if_needed(self, metric: str, year: int, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        rule = self.SCALE_RULES.get(metric)
        if not rule:
            return value
        max_abs = float(rule["max_abs"])
        if abs(value) <= max_abs:
            return value

        # Deterministic unit correction only; no estimation.
        # Try dividing by 1,000 then 1,000,000.
        for factor in (1_000.0, 1_000_000.0):
            candidate = value / factor
            if abs(candidate) <= max_abs:
                self._append_correction(
                    metric=metric,
                    year=year,
                    original_value=value,
                    corrected_value=candidate,
                    reason=f"SCALE_UNIT_FIX_DIV_{int(factor)}",
                )
                return candidate

        self._append_flag(metric, year, value, "EXTREME_VALUE_NO_SAFE_SCALE_FIX")
        return value

    def _fix_jump_if_unit_related(
        self,
        metric: str,
        year: int,
        prev_value: Optional[float],
        current_value: Optional[float],
    ) -> Optional[float]:
        # Unit-based jump fixes are only valid for metrics that are configured
        # with explicit scale rules (typically monetary/size metrics).
        if metric not in self.SCALE_RULES:
            if prev_value is not None and current_value is not None and prev_value != 0:
                ratio = abs(current_value / prev_value)
                if ratio > self.jump_ratio_threshold:
                    self._append_flag(metric, year, current_value, "ABNORMAL_JUMP_NO_SAFE_UNIT_FIX")
            return current_value

        if prev_value is None or current_value is None:
            return current_value
        if prev_value == 0:
            return current_value

        ratio = abs(current_value / prev_value)
        if ratio <= self.jump_ratio_threshold:
            return current_value

        # Only deterministic unit correction candidates.
        candidates: List[Tuple[float, str]] = [
            (current_value / 1_000.0, "JUMP_UNIT_FIX_DIV_1000"),
            (current_value * 1_000.0, "JUMP_UNIT_FIX_MUL_1000"),
        ]
        for candidate, reason in candidates:
            if prev_value == 0:
                continue
            new_ratio = abs(candidate / prev_value)
            if new_ratio <= self.jump_ratio_threshold:
                self._append_correction(
                    metric=metric,
                    year=year,
                    original_value=current_value,
                    corrected_value=candidate,
                    reason=reason,
                )
                return candidate

        self._append_flag(metric, year, current_value, "ABNORMAL_JUMP_NO_SAFE_UNIT_FIX")
        return current_value

    def correct_series(self, metric: str, values_by_year: Dict[int, Any]) -> Dict[int, Any]:
        canonical = self._canonical_metric(metric)
        corrected: Dict[int, Any] = {}
        prev_numeric: Optional[float] = None

        for year in sorted(values_by_year.keys()):
            raw_value = values_by_year[year]
            numeric = self._to_float(raw_value)
            if numeric is None:
                # Never fill missing values.
                corrected[int(year)] = None if self._is_missing(raw_value) else raw_value
                prev_numeric = None if self._is_missing(raw_value) else prev_numeric
                continue

            after_scale = self._fix_scale_if_needed(canonical, int(year), numeric)
            after_jump = self._fix_jump_if_unit_related(canonical, int(year), prev_numeric, after_scale)
            corrected[int(year)] = after_jump
            prev_numeric = after_jump

        return corrected

    def correct_dataset(self, raw_metrics_by_year: Dict[str, Dict[int, Any]]) -> Dict[str, Any]:
        self._log.clear()

        corrected: Dict[str, Dict[int, Any]] = {}
        for metric, series in (raw_metrics_by_year or {}).items():
            corrected[metric] = self.correct_series(metric, series or {})

        return {
            "corrected_metrics": corrected,
            "corrections": self.get_corrections(),
            "flags": self.get_flags(),
            "log_summary": self.get_log_summary(),
        }


__all__ = ["DataCorrectionEngine", "CorrectionEntry", "CorrectionLog"]
