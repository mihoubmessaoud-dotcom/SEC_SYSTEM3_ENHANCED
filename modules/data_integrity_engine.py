from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class IntegrityValue:
    value: Optional[float]
    reason: str = ""
    display: str = ""
    flags: Optional[List[str]] = None
    status: str = "OK"

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if not self.flags:
            data.pop("flags", None)
        # Keep stable contract keys for downstream consumers.
        if "reason" not in data:
            data["reason"] = ""
        if "display" not in data:
            data["display"] = ""
        return data


class DataIntegrityEngine:
    """
    Production-grade pre-calculation validator for raw financial data.

    Guarantees:
    - No backfill behavior (no fillna/ffill/bfill).
    - Missing source value is always returned as:
      value=None, reason="NO_SOURCE_DATA"
    - Impossible values are rejected by strict metric bounds.
    - Repeated values (>= 4 consecutive years) are flagged as:
      "SUSPECTED_BACKFILL"
    """

    STRICT_BOUNDS: Dict[str, Tuple[float, float]] = {
        "ap_days": (0.0, 730.0),
        "dso": (0.0, 365.0),
        "inventory_days": (0.0, 1825.0),
        "gross_margin": (-0.5, 1.0),
        "net_margin": (-2.0, 1.0),
    }

    METRIC_ALIASES: Dict[str, str] = {
        "days_sales_outstanding": "dso",
        "accounts_payable_days": "ap_days",
        "days_payable_outstanding": "ap_days",
        "dio": "inventory_days",
    }

    def __init__(self, freeze_threshold_years: int = 4) -> None:
        self.freeze_threshold_years = max(2, int(freeze_threshold_years))

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            normalized = value.strip().lower()
            return normalized in {"", "none", "null", "nan", "n/a", "na", "--"}
        return False

    def _canonical_metric(self, metric: str) -> str:
        key = str(metric).strip().lower()
        return self.METRIC_ALIASES.get(key, key)

    def _to_float(self, value: Any) -> Optional[float]:
        if self._is_missing(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def validate_value(self, metric: str, value: Any) -> IntegrityValue:
        canonical = self._canonical_metric(metric)
        parsed = self._to_float(value)

        if parsed is None:
            return IntegrityValue(
                value=None,
                reason="NO_SOURCE_DATA",
                display="-- (NO_SOURCE_DATA)",
                status="MISSING",
            )

        bounds = self.STRICT_BOUNDS.get(canonical)
        if bounds is not None:
            lo, hi = bounds
            if parsed < lo or parsed > hi:
                return IntegrityValue(
                    value=None,
                    reason="OUT_OF_BOUNDS",
                    display=f"-- (OUT_OF_BOUNDS: {lo}..{hi})",
                    status="REJECTED",
                )

        return IntegrityValue(value=parsed, reason="", display=str(parsed), status="OK")

    def _detect_frozen_years(self, year_values: Dict[int, IntegrityValue]) -> List[int]:
        """
        Returns years where value repeats for >= threshold consecutive years.
        """
        sorted_pairs = sorted(year_values.items(), key=lambda kv: kv[0])
        frozen_years: List[int] = []

        run_years: List[int] = []
        run_value: Optional[float] = None

        for year, result in sorted_pairs:
            current = result.value
            if current is None:
                if len(run_years) >= self.freeze_threshold_years:
                    frozen_years.extend(run_years)
                run_years = []
                run_value = None
                continue

            if run_value is None or current != run_value:
                if len(run_years) >= self.freeze_threshold_years:
                    frozen_years.extend(run_years)
                run_years = [year]
                run_value = current
            else:
                run_years.append(year)

        if len(run_years) >= self.freeze_threshold_years:
            frozen_years.extend(run_years)

        return frozen_years

    def validate_series(self, metric: str, values_by_year: Dict[int, Any]) -> Dict[int, Dict[str, Any]]:
        validated: Dict[int, IntegrityValue] = {}
        for year, raw_value in values_by_year.items():
            validated[int(year)] = self.validate_value(metric, raw_value)

        frozen_years = set(self._detect_frozen_years(validated))
        for year in frozen_years:
            result = validated[year]
            if result.value is not None:
                if not result.flags:
                    result.flags = []
                if "SUSPECTED_BACKFILL" not in result.flags:
                    result.flags.append("SUSPECTED_BACKFILL")
                if result.status == "OK":
                    result.status = "FLAGGED"
                if not result.display:
                    result.display = str(result.value)
                result.display = f"{result.display} [SUSPECTED_BACKFILL]"

        return {year: result.to_dict() for year, result in sorted(validated.items())}

    def validate_raw_dataset(
        self,
        raw_dataset: Dict[str, Dict[int, Any]],
    ) -> Dict[str, Dict[int, Dict[str, Any]]]:
        """
        Input:
          {
            "gross_margin": {2022: 0.42, 2023: None},
            "DSO": {2022: 38, 2023: 9999}
          }
        Output:
          metric -> year -> {value, reason?, flags?, status}
        """
        output: Dict[str, Dict[int, Dict[str, Any]]] = {}
        for metric, values_by_year in raw_dataset.items():
            output[metric] = self.validate_series(metric, values_by_year)
        return output

    def store_validated_series(
        self,
        repository: Any,
        metric: str,
        values_by_year: Dict[int, Any],
        *,
        store_raw: bool = True,
    ) -> Dict[int, Dict[str, Any]]:
        """
        Validate first, then store in DataRepository.

        Storage contract:
        - Raw values always go to raw_data if store_raw=True.
        - Clean values are written ONLY when validation has numeric value.
        - Missing/out-of-bounds are not written to clean_data.
        """
        validated = self.validate_series(metric, values_by_year)

        for year, raw_value in sorted(values_by_year.items()):
            key = f"{str(metric).strip().lower()}:{int(year)}"
            if store_raw:
                repository.set_raw(key, raw_value)

            result = validated[int(year)]
            if result.get("value") is not None:
                reason = "VALIDATED_OK"
                flags = result.get("flags") or []
                if "SUSPECTED_BACKFILL" in flags:
                    reason = "VALIDATED_OK_SUSPECTED_BACKFILL"
                repository.set_clean(key, result["value"], reason=reason)

        return validated


__all__ = ["DataIntegrityEngine", "IntegrityValue"]

