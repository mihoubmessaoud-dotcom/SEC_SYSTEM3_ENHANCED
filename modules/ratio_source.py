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

    @staticmethod
    def normalize_ratio_id(ratio_id: str) -> str:
        rid = str(ratio_id or '').strip().lower()
        return ALIAS_TO_CANONICAL.get(rid, rid)

    def load(self, ticker: str, data_by_year: Dict, ratios_by_year: Dict) -> None:
        self._ticker = ticker
        self._raw_ratios_by_year = ratios_by_year or {}
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
        return contract

    def _fallback_from_raw(self, year: int, canonical_ratio_id: str) -> Dict:
        raw_row = (self._raw_ratios_by_year.get(year, {}) or {})
        raw = raw_row.get(canonical_ratio_id)
        row_reasons = dict(raw_row.get('_ratio_reasons') or {})
        meta = get_ratio_metadata(canonical_ratio_id)
        forced_reason = row_reasons.get(canonical_ratio_id)
        if forced_reason:
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
