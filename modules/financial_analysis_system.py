from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

from .business_model_engine import BusinessModelEngine
from .data_correction_engine import DataCorrectionEngine
from .data_integrity_engine import DataIntegrityEngine
from .data_repository import DataRepository
from .financial_signature_engine import FinancialSignatureEngine
from .kpi_engine import KPIEngine
from .ratio_engine_cached import RatioEngine, RatioRegistry
from .scoring_engine import ScoringEngine
from .unit_normalization_engine import UnitNormalizationEngine


@dataclass
class FinancialAnalysisSystem:
    """
    End-to-end orchestrator (strict order):
      RAW DATA
      -> UnitNormalizationEngine
      -> DataIntegrityEngine
      -> DataCorrectionEngine
      -> DataRepository
      -> RatioEngine
      -> BusinessModelEngine
      -> KPIEngine
      -> ScoringEngine
      -> FinancialSignatureEngine
    """

    data_correction_engine: DataCorrectionEngine = DataCorrectionEngine()
    data_integrity_engine: DataIntegrityEngine = DataIntegrityEngine()
    business_model_engine: BusinessModelEngine = BusinessModelEngine()
    kpi_engine: KPIEngine = KPIEngine()
    scoring_engine: ScoringEngine = ScoringEngine()
    financial_signature_engine: FinancialSignatureEngine = FinancialSignatureEngine()
    unit_normalization_engine: UnitNormalizationEngine = UnitNormalizationEngine()

    @staticmethod
    def _latest_valid(validated_metric: Dict[int, Dict[str, Any]]) -> Optional[float]:
        for year in sorted(validated_metric.keys(), reverse=True):
            v = validated_metric[year].get("value")
            if v is not None:
                return float(v)
        return None

    @staticmethod
    def _integrity_summary(integrity_output: Dict[str, Dict[int, Dict[str, Any]]]) -> Dict[str, Any]:
        total = 0
        missing = 0
        rejected = 0
        flagged = 0
        for _, series in integrity_output.items():
            for _, item in series.items():
                total += 1
                status = str(item.get("status", ""))
                if status == "MISSING":
                    missing += 1
                elif status == "REJECTED":
                    rejected += 1
                elif status == "FLAGGED":
                    flagged += 1
        valid = max(total - missing - rejected, 0)
        coverage = round((valid / total) * 100.0, 2) if total else 0.0
        return {
            "total_points": total,
            "valid_points": valid,
            "missing_points": missing,
            "rejected_points": rejected,
            "flagged_points": flagged,
            "coverage_pct": coverage,
        }

    @staticmethod
    def _all_years(raw_metrics_by_year: Dict[str, Dict[int, Any]]) -> List[int]:
        years = set()
        for _, series in (raw_metrics_by_year or {}).items():
            for y in (series or {}).keys():
                try:
                    years.add(int(y))
                except Exception:
                    continue
        return sorted(years)

    @staticmethod
    def _latest_clean_value(repository: DataRepository, metric: str, years: List[int]) -> Optional[float]:
        for year in sorted(years, reverse=True):
            got = repository.get_clean(f"{metric}:{year}")
            value = got.get("value")
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _build_scoring_inputs(
        integrity_post: Dict[str, Dict[int, Dict[str, Any]]],
        ratio_results_by_year: Dict[int, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, Optional[float]]:
        # Start with latest validated metric values from integrity.
        inputs: Dict[str, Optional[float]] = {}
        for metric_name, metric_series in integrity_post.items():
            if not isinstance(metric_series, dict):
                continue
            latest = None
            for y in sorted(metric_series.keys(), reverse=True):
                item = metric_series[y] or {}
                v = item.get("value")
                if v is not None:
                    latest = float(v)
                    break
            inputs[metric_name] = latest

        # Overlay with computed ratios when available (authoritative for these keys).
        for y in sorted(ratio_results_by_year.keys(), reverse=True):
            year_map = ratio_results_by_year[y]
            for rk, rv in year_map.items():
                value = rv.get("value")
                if value is not None and inputs.get(rk) is None:
                    inputs[rk] = float(value)
        return inputs

    @staticmethod
    def _build_signature_trend_data(
        integrity_post: Dict[str, Dict[int, Dict[str, Any]]],
        ratio_results_by_year: Dict[int, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, Dict[int, Optional[float]]]:
        trend_data: Dict[str, Dict[int, Optional[float]]] = {}
        for metric, series in integrity_post.items():
            trend_data[metric] = {int(y): item.get("value") for y, item in series.items()}

        # Ratios are added/overlaid as trend lines.
        for year, yr_map in ratio_results_by_year.items():
            for ratio_name, record in yr_map.items():
                series = trend_data.setdefault(ratio_name, {})
                series[int(year)] = record.get("value")
        return trend_data

    def _store_to_repository(
        self,
        repository: DataRepository,
        corrected_metrics: Dict[str, Dict[int, Any]],
        integrity_post: Dict[str, Dict[int, Dict[str, Any]]],
    ) -> None:
        # Store raw and validated-clean values with strict write-once semantics.
        for metric, series in (corrected_metrics or {}).items():
            for year, raw_value in sorted((series or {}).items()):
                key = f"{str(metric).strip().lower()}:{int(year)}"
                repository.set_raw(key, raw_value)
                post_item = ((integrity_post.get(metric) or {}).get(int(year)) or {})
                clean_value = post_item.get("value")
                if clean_value is not None:
                    reason = "VALIDATED_OK"
                    flags = post_item.get("flags") or []
                    if "SUSPECTED_BACKFILL" in flags:
                        reason = "VALIDATED_OK_SUSPECTED_BACKFILL"
                    repository.set_clean(key, clean_value, reason=reason)

    def analyze(
        self,
        ticker: str,
        raw_metrics_by_year: Dict[str, Dict[int, Any]],
        forced_model: Optional[str] = None,
    ) -> Dict[str, Any]:
        pipeline_trace: List[str] = []

        # 1) UnitNormalizationEngine (normalize to millions BEFORE validation/calculations)
        normalization_output = self.unit_normalization_engine.normalize_dataset(raw_metrics_by_year)
        normalized_metrics = normalization_output.get("normalized_metrics", {}) or {}
        pipeline_trace.append("UnitNormalizationEngine")

        # 2) DataIntegrityEngine on normalized raw data (pre-correction snapshot)
        integrity_pre = self.data_integrity_engine.validate_raw_dataset(normalized_metrics)
        pipeline_trace.append("DataIntegrityEngine:pre")

        # 3) DataCorrectionEngine (only allowed mutation layer)
        correction_output = self.data_correction_engine.correct_dataset(normalized_metrics)
        corrected_metrics = correction_output.get("corrected_metrics", {}) or {}
        pipeline_trace.append("DataCorrectionEngine")

        # 4) DataRepository population (raw + clean validated values)
        integrity_post = self.data_integrity_engine.validate_raw_dataset(corrected_metrics)
        integrity_summary = self._integrity_summary(integrity_post)
        repository = DataRepository()
        self._store_to_repository(repository, corrected_metrics, integrity_post)
        pipeline_trace.append("DataIntegrityEngine:post")
        pipeline_trace.append("DataRepository")

        years = self._all_years(corrected_metrics)

        # 5) RatioEngine (single-pass with RatioRegistry cache; no duplicate calculations)
        ratio_engine = RatioEngine(repository=repository, registry=RatioRegistry())
        ratio_results_by_year: Dict[int, Dict[str, Dict[str, Any]]] = {}
        for year in years:
            ratio_results_by_year[int(year)] = {
                "ebitda": ratio_engine.calculate("ebitda", int(year)),
                "roic": ratio_engine.calculate("roic", int(year)),
                "roe": ratio_engine.calculate("roe", int(year)),
                "peg": ratio_engine.calculate("peg", int(year)),
                "gross_margin": ratio_engine.calculate("gross_margin", int(year)),
            }
        pipeline_trace.append("RatioEngine")

        # 6) BusinessModelEngine (clean data only, no recalculation)
        model_inputs = {
            "gross_margin": self._latest_clean_value(repository, "gross_margin", years),
            "capex_to_revenue": self._latest_clean_value(repository, "capex_to_revenue", years),
            "rd_to_revenue": self._latest_clean_value(repository, "rd_to_revenue", years),
        }
        model_result = self.business_model_engine.classify(**model_inputs)
        if forced_model:
            fm = str(forced_model).strip().lower()
            # Fail-closed: only allow forcing to known model names.
            if fm in self.business_model_engine.MODEL_RULES:
                model_result = {
                    "model": fm,
                    "confidence": 0.99,
                    "alternatives": [],
                }
        pipeline_trace.append("BusinessModelEngine")

        # 7) KPIEngine
        kpi_result = self.kpi_engine.assign_dynamic(model_result)
        pipeline_trace.append("KPIEngine")

        # 8) ScoringEngine
        score_inputs = self._build_scoring_inputs(integrity_post, ratio_results_by_year)
        score_result = self.scoring_engine.score(model_result["model"], score_inputs)
        pipeline_trace.append("ScoringEngine")

        # 9) FinancialSignatureEngine
        trend_data = self._build_signature_trend_data(integrity_post, ratio_results_by_year)
        signature_result = self.financial_signature_engine.generate_signature(model_result["model"], trend_data)
        pipeline_trace.append("FinancialSignatureEngine")

        return {
            "ticker": str(ticker).upper(),
            "model": model_result,
            "kpis": kpi_result,
            "ratios": ratio_results_by_year,
            "score": score_result,
            "signature": signature_result,
            # backward-compatibility alias
            "financial_signature": signature_result,
            "data_integrity": {
                "summary": integrity_summary,
                "validated_metrics": integrity_post,
                "pre_validation": integrity_pre,
                "corrections": correction_output.get("corrections", []),
                "correction_flags": correction_output.get("flags", []),
                "normalization": normalization_output,
                "pipeline_trace": pipeline_trace,
                "mutation_policy": "ONLY_DataCorrectionEngine_CAN_MODIFY_VALUES",
                "repository_meta": {
                    "raw_count": len(repository.raw_data),
                    "clean_count": len(repository.clean_data),
                },
            },
        }


__all__ = ["FinancialAnalysisSystem"]
