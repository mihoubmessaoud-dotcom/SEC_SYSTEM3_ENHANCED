from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple


CONCEPT_PRIORITY = {
    'annual_revenue': [
        'Revenue_Hierarchy',
        'NetRevenue_Hierarchy',
        'NetRevenue',
        'Revenues',
        'SalesRevenueNet',
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'Revenue',
    ],
    'annual_cogs': [
        'CostOfRevenue',
        'CostOfGoodsAndServicesSold',
        'COGS',
    ],
    'total_equity': [
        'StockholdersEquity',
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    ],
    'accounts_receivable': [
        'AccountsReceivableNetCurrent_Hierarchy',
        'AccountsReceivableNetCurrent',
        'AccountsReceivable',
        'ReceivablesNetCurrent',
    ],
    'accounts_payable': [
        'AccountsPayableCurrent_Hierarchy',
        'AccountsPayableCurrent',
        'AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent',
        'AccountsPayable',
    ],
    'inventory': [
        'InventoryNet_Hierarchy',
        'InventoryNet',
        'Inventory',
    ],
}


def _infer_period_type(tag: str) -> str:
    t = (tag or '').lower()
    if any(k in t for k in ('qtd', 'quarter', 'qtr')):
        return 'Q'
    if 'ytd' in t:
        return 'YTD'
    return 'FY'


def _is_consolidated(candidate: Dict) -> bool:
    return not bool(candidate.get('has_dimensions'))


def _parse_date(v) -> Optional[datetime]:
    if not isinstance(v, str) or not v.strip():
        return None
    txt = v.strip()
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y%m%d'):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            continue
    return None


def _as_iso_date(v) -> Optional[str]:
    d = _parse_date(v)
    if d is None:
        return None
    return d.strftime('%Y-%m-%d')


def _parse_unit_to_scale(unit: Optional[str], scale_applied) -> Tuple[str, str, float]:
    u = str(unit or 'USD').strip()
    u_low = u.lower()
    currency = 'USD'
    if 'eur' in u_low:
        currency = 'EUR'
    if 'gbp' in u_low:
        currency = 'GBP'

    if isinstance(scale_applied, (int, float)) and scale_applied not in (0,):
        scale = float(scale_applied)
    else:
        scale = 1.0

    if any(k in u_low for k in ('million', 'usdm', 'x1e6', '1e6')):
        scale *= 1_000_000.0
    elif any(k in u_low for k in ('billion', 'usdb', 'x1e9', '1e9')):
        scale *= 1_000_000_000.0
    elif any(k in u_low for k in ('thousand', 'usdk', 'x1e3', '1e3')):
        scale *= 1_000.0

    return u, currency, scale


def _normalize_candidate(raw: Dict, concept_id: str, rank: int) -> Dict:
    tag = raw.get('tag')
    orig_unit, currency, scale = _parse_unit_to_scale(raw.get('unit'), raw.get('scale_applied', 1))
    raw_value = raw.get('value')
    value = float(raw_value) * scale if isinstance(raw_value, (int, float)) else None
    period_end = raw.get('period_end')
    return {
        'tag': tag,
        'value': value,
        'raw_value': raw_value,
        'context_id': raw.get('context_id'),
        'period_type': raw.get('period_type') or _infer_period_type(tag or ''),
        'period_start': raw.get('period_start'),
        'period_end': period_end,
        'period_end_dt': _as_iso_date(period_end),
        'unit': 'USD' if currency == 'USD' else orig_unit,
        'original_unit': orig_unit,
        'currency': currency,
        'decimals': raw.get('decimals'),
        'scale_applied': scale,
        'has_dimensions': bool(raw.get('has_dimensions')),
        'priority_rank': rank,
        'score': 0.0,
        'concept_id': concept_id,
        'reason_rejected': None,
        'selection_reason_hint': raw.get('selection_reason'),
        'parent_child_mismatch_pct': raw.get('parent_child_mismatch_pct'),
    }


def _reject(c: Dict, reason: str) -> None:
    c['reason_rejected'] = reason


def resolve_item(
    year: int,
    concept_id: str,
    candidates: List[Dict],
    *,
    require_fy: bool = False,
    allow_negative: bool = True,
) -> Dict:
    ordered_tags = CONCEPT_PRIORITY.get(concept_id, [])
    rank_map = {tag: idx for idx, tag in enumerate(ordered_tags)}
    normalized = []
    for i, c in enumerate(candidates or []):
        tag = c.get('tag')
        rank = rank_map.get(tag, 10_000 + i)
        normalized.append(_normalize_candidate(c, concept_id, rank))

    has_any_fy = any(n.get('period_type') == 'FY' and isinstance(n.get('value'), (int, float)) for n in normalized)
    for c in normalized:
        v = c.get('value')
        if not isinstance(v, (int, float)):
            _reject(c, 'missing_or_non_numeric')
            continue
        if require_fy and c.get('period_type') != 'FY' and has_any_fy:
            _reject(c, 'not_fy_duration')
            continue
        if not allow_negative and v < 0:
            _reject(c, 'negative_value_rejected')
            continue
        if c.get('currency') != 'USD':
            _reject(c, 'unit_mismatch_non_usd')
            continue

    accepted = [c for c in normalized if c.get('reason_rejected') is None]
    if not accepted:
        return {
            'value': None,
            'tag': None,
            'context_id': None,
            'period_type': 'FY' if require_fy else 'INSTANT',
            'period_start': None,
            'period_end': None,
            'unit': None,
            'original_unit': None,
            'currency': None,
            'decimals': None,
            'scale_applied': None,
            'confidence': 0,
            'selection_reason': f'{concept_id}_missing_or_invalid',
            'candidates': normalized[:3],
        }

    accepted.sort(
        key=lambda c: (
            c.get('priority_rank', 10_000),
            0 if _is_consolidated(c) else 1,
            0 if c.get('period_type') == 'FY' else 1,
        )
    )
    picked = accepted[0]
    confidence = 100
    if picked.get('priority_rank', 10_000) >= 10_000:
        confidence -= 40
    if not _is_consolidated(picked):
        confidence -= 10
    if require_fy and picked.get('period_type') != 'FY':
        confidence -= 20
    selection_reason = picked.get('selection_reason_hint') or 'top_priority_concept'
    if picked.get('priority_rank', 10_000) >= 10_000:
        selection_reason = 'concept_fallback_used'

    return {
        'value': float(picked.get('value')),
        'tag': picked.get('tag'),
        'context_id': picked.get('context_id'),
        'period_type': picked.get('period_type'),
        'period_start': picked.get('period_start'),
        'period_end': picked.get('period_end'),
        'period_end_dt': picked.get('period_end_dt'),
        'unit': picked.get('unit'),
        'original_unit': picked.get('original_unit'),
        'currency': picked.get('currency'),
        'decimals': picked.get('decimals'),
        'scale_applied': picked.get('scale_applied', 1),
        'confidence': max(0, confidence),
        'selection_reason': selection_reason,
        'priority_rank': picked.get('priority_rank', 0),
        'parent_child_mismatch_pct': picked.get('parent_child_mismatch_pct'),
        'candidates': normalized[:3],
    }
