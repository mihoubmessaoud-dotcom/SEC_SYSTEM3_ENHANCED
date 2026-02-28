from __future__ import annotations

import os
from typing import Dict, Optional


# Canonical rule:
# - percent-like ratios are stored as FRACTION (e.g., 0.5544166 for 55.44166%)
# - conversion to human-readable percent happens in formatter only.

PERCENT_RATIO_IDS = {
    # SEC fetcher / classic UI
    'gross_margin',
    'operating_margin',
    'net_margin',
    'ebitda_margin',
    'roa',
    'roe',
    'roic',
    'ocf_margin',
    'retention_ratio',
    'sgr_internal',
    'dividend_yield',
    'fcf_yield',
    'cost_of_debt',
    'wacc',
    'economic_spread',
    # institutional engine
    'fcf_margin',
    'net_interest_margin',
    'net_income_to_assets',
    'equity_ratio',
    'returns_on_assets_proxy',
    'bank_efficiency_ratio',
}

_DEBUG_TRACE_BUFFER = []


def is_ratio_format_debug_enabled() -> bool:
    return str(os.getenv('SEC_DEBUG_RATIO_FORMAT', '')).strip() == '1'


def reset_ratio_format_trace() -> None:
    _DEBUG_TRACE_BUFFER.clear()


def get_ratio_format_trace():
    return list(_DEBUG_TRACE_BUFFER)


def get_ratio_metadata(ratio_id: str) -> Dict[str, object]:
    is_percent = ratio_id in PERCENT_RATIO_IDS
    return {
        'ratio_format': 'percent' if is_percent else 'number',
        'ratio_unit': 'fraction' if is_percent else 'scalar',
        'ratio_display_multiplier': 100 if is_percent else 1,
    }


def canonicalize_ratio_value(ratio_id: str, value: Optional[float]) -> Optional[float]:
    """
    Normalize ratio value to canonical internal representation.
    For percent-like ratios:
      - expected canonical range roughly [-2, 2]
      - if legacy percent format is detected (e.g., 55.4), convert to fraction.
    """
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return None
    meta = get_ratio_metadata(ratio_id)
    if meta['ratio_format'] == 'percent':
        if abs(v) > 2.0 and abs(v) <= 200.0:
            return v / 100.0
    return v


def format_ratio_value(ratio_id: str, value: Optional[float]) -> Dict[str, object]:
    meta = get_ratio_metadata(ratio_id)
    canonical = canonicalize_ratio_value(ratio_id, value)
    if canonical is None:
        return {
            'canonical_value': None,
            'display_value': None,
            'display_text': 'N/A',
            'display_suffix': '',
            'formatter_path': 'format_ratio_value:none',
            'format_rejection_reason': None,
            **meta,
        }

    if meta['ratio_format'] == 'percent' and abs(canonical) > 2.0:
        out = {
            'canonical_value': canonical,
            'display_value': None,
            'display_text': 'N/A',
            'display_suffix': '',
            'formatter_path': 'format_ratio_value:rejected_percent_out_of_range',
            'format_rejection_reason': 'percent_out_of_range',
            **meta,
        }
        if is_ratio_format_debug_enabled():
            log_line = (
                f"[RATIO_FMT] {ratio_id} "
                f"canonical={out.get('canonical_value')} "
                f"display={out.get('display_value')} "
                f"suffix={out.get('display_suffix', '')} "
                f"formatter={out.get('formatter_path')}"
            )
            print(log_line)
            _DEBUG_TRACE_BUFFER.append({
                'ratio_name': ratio_id,
                'canonical_value': out.get('canonical_value'),
                'display_value': out.get('display_value'),
                'display_suffix': out.get('display_suffix', ''),
                'formatter_path': out.get('formatter_path'),
                'ratio_format': out.get('ratio_format'),
                'ratio_unit': out.get('ratio_unit'),
                'ratio_display_multiplier': out.get('ratio_display_multiplier'),
                'format_rejection_reason': out.get('format_rejection_reason'),
            })
        return out

    if meta['ratio_format'] == 'percent':
        display_value = canonical * 100.0
        out = {
            'canonical_value': canonical,
            'display_value': display_value,
            'display_text': f'{display_value:.2f}%',
            'display_suffix': '%',
            'formatter_path': 'format_ratio_value:percent*100_once',
            'format_rejection_reason': None,
            **meta,
        }
    else:
        out = {
            'canonical_value': canonical,
            'display_value': canonical,
            'display_text': f'{canonical:.2f}',
            'display_suffix': '',
            'formatter_path': 'format_ratio_value:number_direct',
            'format_rejection_reason': None,
            **meta,
        }

    if is_ratio_format_debug_enabled():
        log_line = (
            f"[RATIO_FMT] {ratio_id} "
            f"canonical={out.get('canonical_value')} "
            f"display={out.get('display_value')} "
            f"suffix={out.get('display_suffix', '')} "
            f"formatter={out.get('formatter_path')}"
        )
        print(log_line)
        _DEBUG_TRACE_BUFFER.append({
            'ratio_name': ratio_id,
            'canonical_value': out.get('canonical_value'),
            'display_value': out.get('display_value'),
            'display_suffix': out.get('display_suffix', ''),
            'formatter_path': out.get('formatter_path'),
            'ratio_format': out.get('ratio_format'),
            'ratio_unit': out.get('ratio_unit'),
            'ratio_display_multiplier': out.get('ratio_display_multiplier'),
        })

    return out
