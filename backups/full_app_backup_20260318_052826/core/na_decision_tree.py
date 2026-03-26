"""NA decision tree for ratio computability checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


REASON_CODES = {
    "MISSING_SEC_CONCEPT",
    "MISSING_MARKET_DATA",
    "MISSING_MARKET_LAYER",
    "MISSING_REQUIRED_LAYER",
    "PERIOD_MISMATCH",
    "UNIT_MISMATCH",
    "ZERO_DENOMINATOR",
    "INSUFFICIENT_HISTORY",
    "DATA_NOT_APPLICABLE",
}


@dataclass
class DecisionResult:
    """Container for the NA decision tree result."""

    computable: bool
    reason: Optional[str]
    missing_inputs: List[str]
    decision_path: List[Dict[str, Any]]


class NADecisionTree:
    """Evaluates ratio computability using an ordered branch decision tree."""

    def __init__(self) -> None:
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def evaluate(
        self,
        ratio_name: str,
        ratio_type: str,
        required_layers: List[str],
        required_fields: List[str],
        layer_payloads: Dict[str, Dict[str, Any]],
        inputs: Dict[str, Dict[str, Any]],
        denominator_keys: Optional[List[str]] = None,
    ) -> DecisionResult:
        """Run branch checks in strict order and return decision details."""
        path: List[Dict[str, Any]] = []

        yahoo_ok = bool(layer_payloads.get("YAHOO")) and (
            (layer_payloads.get("YAHOO") or {}).get("status") == "OK"
        )

        for layer_name in required_layers:
            layer_obj = layer_payloads.get(layer_name)
            if not layer_obj:
                if layer_name == "MARKET":
                    if yahoo_ok:
                        path.append(self._pass("market_layer_substituted_by_yahoo"))
                        continue
                    return self._fail(
                        path,
                        branch_name="required_layer_available",
                        reason="MISSING_MARKET_LAYER",
                    )
                return self._fail(
                    path,
                    branch_name="required_layer_available",
                    reason="MISSING_REQUIRED_LAYER",
                )

        if ratio_type in {"MARKET_DEPENDENT", "HYBRID"} and "MARKET" in required_layers:
            market_layer = layer_payloads.get("MARKET")
            if market_layer and market_layer.get("status") in {"DISABLED", "MISSING_API_KEY"}:
                if not yahoo_ok:
                    return self._fail(
                        path,
                        branch_name="market_data_available",
                        reason="MISSING_MARKET_DATA",
                    )
                path.append(self._pass("market_data_substituted_by_yahoo"))
            else:
                path.append(self._pass("market_dependency_check"))
        else:
            path.append(self._pass("market_dependency_check"))

        missing_fields = [field for field in required_fields if field not in inputs]
        if missing_fields:
            return self._fail(
                path,
                branch_name="required_sec_concepts_present",
                reason="MISSING_SEC_CONCEPT",
                missing_inputs=missing_fields,
            )
        path.append(self._pass("required_sec_concepts_present"))

        periods = {inputs[field].get("period_end") for field in required_fields}
        periods.discard(None)
        if len(periods) > 1:
            # Annual cross-source tolerance: SEC fiscal end date can differ from market year-end date.
            # If all period_end values belong to the same year, allow computation.
            period_years = set()
            for p in periods:
                try:
                    period_years.add(str(p)[:4])
                except Exception:
                    pass
            if len(period_years) == 1:
                path.append(self._pass("period_alignment_by_year"))
            else:
                return self._fail(
                    path,
                    branch_name="period_alignment",
                    reason="PERIOD_MISMATCH",
                )
        else:
            path.append(self._pass("period_alignment"))

        units = {inputs[field].get("unit") for field in required_fields}
        units.discard(None)
        if len(units) > 1:
            non_ratio_units = [u for u in units if u not in {"pure", "ratio"}]
            if len(set(non_ratio_units)) > 1:
                return self._fail(
                    path,
                    branch_name="unit_compatibility",
                    reason="UNIT_MISMATCH",
                )
        path.append(self._pass("unit_compatibility"))

        for key in denominator_keys or []:
            entry = inputs.get(key)
            if entry is not None and float(entry.get("value", 0.0)) == 0.0:
                return self._fail(
                    path,
                    branch_name="denominator_non_zero",
                    reason="ZERO_DENOMINATOR",
                )
        path.append(self._pass("denominator_non_zero"))

        if ratio_type == "HYBRID" and len(required_fields) < 2:
            return self._fail(
                path,
                branch_name="history_completeness",
                reason="INSUFFICIENT_HISTORY",
            )
        path.append(self._pass("history_completeness"))

        path.append(self._pass("final_data_availability"))
        return DecisionResult(
            computable=True,
            reason=None,
            missing_inputs=[],
            decision_path=path,
        )

    def build_not_computable(
        self,
        reason: str,
        missing_inputs: Optional[List[str]],
        period_used: Optional[str],
        unit_info: Dict[str, Any],
        audit_trace: Dict[str, Any],
        formula_used: str,
        dependency_map: Dict[str, Any],
        decision_path: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Create a standardized not-computable object."""
        if reason not in REASON_CODES:
            reason = "DATA_NOT_APPLICABLE"
        return {
            "status": "NOT_COMPUTABLE",
            "reason": reason,
            "missing_inputs": missing_inputs or [],
            "period_used": period_used,
            "unit_info": unit_info,
            "audit_trace": audit_trace,
            "formula_used": formula_used,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": dependency_map,
            "decision_tree": decision_path,
        }

    def _pass(self, branch_name: str) -> Dict[str, Any]:
        return {
            "branch": branch_name,
            "branch_passed": True,
            "branch_failed": False,
            "resulting_reason": None,
            "decision_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _fail(
        self,
        path: List[Dict[str, Any]],
        branch_name: str,
        reason: str,
        missing_inputs: Optional[List[str]] = None,
    ) -> DecisionResult:
        path.append(
            {
                "branch": branch_name,
                "branch_passed": False,
                "branch_failed": True,
                "resulting_reason": reason,
                "decision_timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return DecisionResult(
            computable=False,
            reason=reason,
            missing_inputs=missing_inputs or [],
            decision_path=path,
        )
