from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Dict, Optional

from .ratio_engine import RatioEngine
from .ratio_formats import canonicalize_ratio_value, get_ratio_metadata


ALIAS_TO_CANONICAL = {
    'roe': 'roe',
    'return_on_equity': 'roe',
    'dso': 'days_sales_outstanding',
    'dso_days': 'days_sales_outstanding',
    'ar_days': 'days_sales_outstanding',
    'days_sales_outstanding': 'days_sales_outstanding',
    'ccc': 'ccc_days',
    'ccc_days': 'ccc_days',
    'pb': 'pb_ratio',
    'p_b_ratio': 'pb_ratio',
    'pb_ratio': 'pb_ratio',
    'sgr': 'sgr_internal',
    'sgr_internal': 'sgr_internal',
}


class _GuardedYearRow(Mapping):
    def __init__(self, row: Dict):
        self._row = row or {}

    def __iter__(self):
        return iter(self._row)

    def __len__(self):
        return len(self._row)

    def __getitem__(self, key):
        k = str(key or '').strip()
        if k and k != k.lower():
            raise RuntimeError(f"URS debug guard blocked non-normalized ratio key access: {k}")
        return self._row[key]


class _GuardedRatiosByYear(Mapping):
    def __init__(self, payload: Dict):
        self._payload = payload or {}

    def __iter__(self):
        return iter(self._payload)

    def __len__(self):
        return len(self._payload)

    def __getitem__(self, key):
        row = self._payload[key]
        if isinstance(row, dict):
            return _GuardedYearRow(row)
        return row

    def get(self, key, default=None):
        if key not in self._payload:
            return default
        return self.__getitem__(key)


def maybe_guard_ratios_by_year(ratios_by_year: Dict):
    if str(os.getenv('SEC_DEBUG_RATIO_GUARD', '')).strip() == '1':
        return _GuardedRatiosByYear(ratios_by_year or {})
    return ratios_by_year or {}


class UnifiedRatioSource:
    def __init__(self):
        self._engine = RatioEngine()
        self._contracts_by_year: Dict[int, Dict[str, Dict]] = {}
        self._raw_ratios_by_year: Dict[int, Dict] = {}
        self._ticker: Optional[str] = None
        self._sector_profile: Optional[str] = None

    @staticmethod
    def _canonical_sector_profile(profile: Optional[str]) -> Optional[str]:
        p = str(profile or "").strip().lower()
        if not p:
            return None
        if p == "bank" or p.endswith("_bank") or p in {"commercial_bank", "investment_bank", "universal_bank"}:
            return "bank"
        if p == "insurance" or p.startswith("insurance_"):
            return "insurance"
        if p in {"broker_dealer", "broker-dealer", "broker"}:
            return "broker_dealer"
        if p in {"industrial", "technology", "unknown"}:
            return p
        return p

    @staticmethod
    def normalize_ratio_id(ratio_id: str) -> str:
        rid = str(ratio_id or '').strip().lower()
        return ALIAS_TO_CANONICAL.get(rid, rid)

    def load(self, ticker: str, data_by_year: Dict, ratios_by_year: Dict, sector_profile: Optional[str] = None) -> None:
        self._ticker = ticker
        self._raw_ratios_by_year = ratios_by_year or {}
        self._sector_profile = self._canonical_sector_profile(sector_profile)
        built = self._engine.build(data_by_year or {}, ratios_by_year or {})
        self._contracts_by_year = built.get('ratios', {}) or {}

    def get_ratio_contract(self, ticker: str, year: int, ratio_id: str) -> Dict:
        canonical = self.normalize_ratio_id(ratio_id)
        row = (self._contracts_by_year.get(year, {}) or {})
        contract = row.get(canonical)
        if contract is None:
            contract = self._fallback_from_raw(year, canonical)
        else:
            # If engine contract is NOT_COMPUTABLE but raw legacy map has a numeric value,
            # prefer the numeric raw value to avoid hiding available ratios in the UI.
            cval = contract.get('value') if isinstance(contract, dict) else None
            if not isinstance(cval, (int, float)):
                reason = str((contract or {}).get('reason') or '').upper()
                # Strict guard: do NOT bypass engine rejections caused by quality gates
                # (unit mismatch, plausibility, period mismatch, zero denominator, etc.).
                fallback_allowed_reasons = {
                    'MISSING_SEC_CONCEPT',
                    'MISSING_MARKET_DATA',
                    'INSUFFICIENT_HISTORY',
                    'DATA_NOT_APPLICABLE',
                    '',
                }
                if reason in fallback_allowed_reasons:
                    raw_fallback = self._fallback_from_raw(year, canonical)
                    rval = raw_fallback.get('value') if isinstance(raw_fallback, dict) else None
                    if isinstance(rval, (int, float)):
                        contract = raw_fallback
        contract = dict(contract)
        contract['ratio_id'] = canonical
        contract.setdefault('source', 'ratio_engine')
        contract.setdefault('inputs', {})
        contract.setdefault('raw_values_used', contract.get('inputs', {}))
        contract.setdefault('input_concepts', contract.get('input_tags', []))
        contract.setdefault('formula_used', canonical)
        contract.setdefault('period', None)
        contract.setdefault('computation_timestamp', None)
        if isinstance(contract.get('value'), (int, float)):
            contract.setdefault('status', 'COMPUTED')
            contract.setdefault('reason', None)
            contract.setdefault('missing_inputs', [])
        else:
            contract.setdefault('status', 'NOT_COMPUTABLE')
            contract.setdefault('reason', 'MISSING_SEC_CONCEPT')
            contract.setdefault('missing_inputs', list(contract.get('input_concepts') or []))
        contract.setdefault('bounds_result', {'status': 'unknown', 'ratio_id': canonical})
        # Coherence fix: keep PB and BVPS internally consistent when market PB is available
        # but equity anchors are missing / stale. Prefer aligning BVPS to PB (price / pb),
        # rather than emitting contradictory PB/BVPS across sheets.
        try:
            import math

            def _finite(v):
                try:
                    return isinstance(v, (int, float)) and math.isfinite(float(v))
                except Exception:
                    return False

            if canonical == 'book_value_per_share' and self._sector_profile in {'bank', 'insurance', 'broker_dealer'}:
                bvps_val = contract.get('value')
                if _finite(bvps_val) and abs(float(bvps_val)) > 1e-12:
                    pb_contract = (self._contracts_by_year.get(year, {}) or {}).get('pb_ratio') or {}
                    pb_val = pb_contract.get('value') if isinstance(pb_contract, dict) else None
                    pb_inputs = pb_contract.get('inputs') if isinstance(pb_contract, dict) else {}
                    price = None
                    try:
                        if isinstance(pb_inputs, dict):
                            price = pb_inputs.get('price')
                    except Exception:
                        price = None
                    if _finite(price) and _finite(pb_val) and pb_val not in (0, None):
                        implied_pb = float(price) / float(bvps_val)
                        gap = max(abs(implied_pb), abs(float(pb_val))) / max(min(abs(implied_pb), abs(float(pb_val))), 1e-12)
                        # Only realign when the mismatch is material (likely unit/basis mix).
                        if gap >= 1.35 and abs(float(pb_val)) >= 0.10:
                            new_bvps = float(price) / float(pb_val)
                            # Guard against pathological results.
                            if _finite(new_bvps) and 0.01 <= abs(new_bvps) <= 5_000.0:
                                contract['value'] = new_bvps
                                contract['status'] = 'COMPUTED'
                                contract['reason'] = 'BVPS_REALIGNED_TO_PB'
                                try:
                                    contract['reliability'] = min(float(contract.get('reliability') or 80.0), 70.0)
                                except Exception:
                                    contract['reliability'] = 70.0
                                contract['source'] = contract.get('source') or 'ratio_engine'
                                inputs = dict(contract.get('inputs') or {})
                                inputs.update({'price': float(price), 'pb_ratio': float(pb_val), 'bvps_original': float(bvps_val)})
                                contract['inputs'] = inputs
        except Exception:
            pass
        # Institutional Data Contract v1 (backward-compatible enrichments)
        contract['data_contract_version'] = 'v1'
        contract['metric_id'] = canonical
        contract['year'] = year
        contract['ticker'] = ticker or self._ticker
        contract['reason_code'] = contract.get('reason')
        if isinstance(contract.get('reliability'), (int, float)):
            rel = float(contract.get('reliability') or 0.0)
            contract['confidence_score'] = max(0.0, min(100.0, rel))
            contract['confidence'] = round(contract['confidence_score'] / 100.0, 4)
        else:
            contract.setdefault('confidence_score', 0.0)
            contract.setdefault('confidence', 0.0)
        contract.setdefault('unit', contract.get('ratio_unit'))
        contract.setdefault('formula', contract.get('formula_used'))
        contract.setdefault('sources', self._build_sources(contract))
        contract['provenance_complete'] = self._is_provenance_complete(contract)
        contract = self._apply_sector_sanity_bounds(contract)
        return contract

    def _apply_sector_sanity_bounds(self, contract: Dict) -> Dict:
        """
        Fail-closed on economically impossible values for sector-critical ratios.
        This prevents outlier raw values from leaking into UI/export/comparison/smart analysis.
        """
        sector = str(self._sector_profile or '').strip().lower()
        if not sector:
            return contract
        ratio_id = str(contract.get('ratio_id') or contract.get('metric_id') or '').strip().lower()
        value = contract.get('value')
        if not isinstance(value, (int, float)):
            return contract

        strict_bounds = {}
        if sector == 'bank':
            strict_bounds = {
                'loan_to_deposit_ratio': (0.20, 2.50),
                'capital_ratio_proxy': (0.03, 0.25),
                'net_interest_margin': (-0.03, 0.08),
                'bank_efficiency_ratio': (0.30, 0.90),
            }
        if ratio_id not in strict_bounds:
            return contract

        lo, hi = strict_bounds[ratio_id]
        try:
            fv = float(value)
        except Exception:
            fv = None
        if fv is None:
            return contract
        if fv < lo or fv > hi:
            # Convert to NOT_COMPUTABLE to force all visible layers to suppress the value.
            new = dict(contract)
            new['value'] = None
            new['status'] = 'NOT_COMPUTABLE'
            new['reason'] = 'OUT_OF_RANGE_UNTRUSTED'
            new['reason_code'] = new['reason']
            new['missing_inputs'] = list(new.get('missing_inputs') or [])
            new['reliability'] = 0
            new['confidence_score'] = 0.0
            new['confidence'] = 0.0
            br = dict(new.get('bounds_result') or {})
            br['status'] = 'blocked'
            br['ratio_id'] = ratio_id
            br['details'] = f"value={fv:.6g} outside [{lo}, {hi}] for sector={sector}"
            new['bounds_result'] = br
            return new
        return contract

    @staticmethod
    def _build_sources(contract: Dict) -> list:
        src = str(contract.get('source') or 'ratio_engine')
        concepts = list(contract.get('input_concepts') or contract.get('input_tags') or [])
        values = dict(contract.get('raw_values_used') or contract.get('inputs') or {})
        out = []
        if concepts:
            for c in concepts:
                out.append({'source': src, 'concept': str(c), 'value': values.get(c)})
        elif values:
            for k, v in values.items():
                out.append({'source': src, 'concept': str(k), 'value': v})
        else:
            out.append({'source': src, 'concept': None, 'value': None})
        return out

    @staticmethod
    def _is_provenance_complete(contract: Dict) -> bool:
        formula = str(contract.get('formula_used') or '').strip()
        source = str(contract.get('source') or '').strip()
        trace = contract.get('data_source_trace') or contract.get('decision_tree')
        concepts = list(contract.get('input_concepts') or contract.get('input_tags') or [])
        values = dict(contract.get('raw_values_used') or contract.get('inputs') or {})
        has_source_signal = bool(source or trace)
        # For computed ratios, require formula + source + at least one input indicator.
        if str(contract.get('status') or '').upper() == 'COMPUTED':
            return bool(formula and has_source_signal and (concepts or values))
        # For NOT_COMPUTABLE keep relaxed provenance requirement.
        return has_source_signal

    def _fallback_from_raw(self, year: int, canonical_ratio_id: str) -> Dict:
        raw_row = (self._raw_ratios_by_year.get(year, {}) or {})
        raw = raw_row.get(canonical_ratio_id)
        row_reasons = dict(raw_row.get('_ratio_reasons') or {})
        meta = get_ratio_metadata(canonical_ratio_id)
        forced_reason = row_reasons.get(canonical_ratio_id)
        # If a stale reason exists but a numeric raw value is available,
        # prefer the numeric value (reason can be legacy residue before quality fixes).
        if forced_reason and raw is None:
            return {
                'status': 'NOT_COMPUTABLE',
                'reason': forced_reason,
                'missing_inputs': [],
                'reliability': 0,
                'source': 'ratio_engine',
                'inputs': {},
                'raw_values_used': {},
                'input_concepts': [],
                'formula_used': canonical_ratio_id,
                'period': None,
                'computation_timestamp': None,
                'bounds_result': {'status': 'no_value', 'ratio_id': canonical_ratio_id},
                **meta,
            }
        if raw is None:
            return {
                'status': 'NOT_COMPUTABLE',
                'reason': 'MISSING_SEC_CONCEPT',
                'missing_inputs': [],
                'reliability': 0,
                'source': 'ratio_engine',
                'inputs': {},
                'raw_values_used': {},
                'input_concepts': [],
                'formula_used': canonical_ratio_id,
                'period': None,
                'computation_timestamp': None,
                'bounds_result': {'status': 'no_value', 'ratio_id': canonical_ratio_id},
                **meta,
            }
        val = canonicalize_ratio_value(canonical_ratio_id, raw)
        if val is None:
            return {
                'status': 'NOT_COMPUTABLE',
                'reason': 'DATA_NOT_APPLICABLE',
                'missing_inputs': [],
                'reliability': 0,
                'source': 'ratio_engine',
                'inputs': {'raw_value': raw},
                'raw_values_used': {'raw_value': raw},
                'input_concepts': [],
                'formula_used': canonical_ratio_id,
                'period': None,
                'computation_timestamp': None,
                'bounds_result': {'status': 'rejected', 'ratio_id': canonical_ratio_id},
                **meta,
            }
        out = {
            'status': 'COMPUTED',
            'value': float(val),
            'reliability': 80,
            'reason': None,
            'missing_inputs': [],
            'source': 'ratio_engine',
            'inputs': {'raw_value': raw},
            'raw_values_used': {'raw_value': raw},
            'input_concepts': [],
            'formula_used': canonical_ratio_id,
            'period': None,
            'computation_timestamp': None,
            'bounds_result': {'status': 'ok', 'ratio_id': canonical_ratio_id},
            **meta,
        }
        if meta.get('ratio_format') == 'percent' and abs(float(val)) > 2.0:
            out.pop('value', None)
            out['status'] = 'NOT_COMPUTABLE'
            out['reliability'] = 0
            out['reason'] = 'DATA_NOT_APPLICABLE'
            out['bounds_result'] = {'status': 'rejected', 'ratio_id': canonical_ratio_id}
        return out


_GLOBAL_RATIO_SOURCE = UnifiedRatioSource()


def load_ratio_context(ticker: str, data_by_year: Dict, ratios_by_year: Dict) -> None:
    _GLOBAL_RATIO_SOURCE.load(ticker, data_by_year, ratios_by_year)


def get_ratio_contract(ticker: str, year: int, ratio_id: str) -> Dict:
    return _GLOBAL_RATIO_SOURCE.get_ratio_contract(ticker, year, ratio_id)
