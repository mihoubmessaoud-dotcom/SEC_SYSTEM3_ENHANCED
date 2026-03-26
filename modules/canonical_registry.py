from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional


_DEFAULT_PRIORITY = {
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

_REGISTRY_CACHE: Optional[Dict] = None


def _normalize_sector(s: Optional[str]) -> str:
    txt = str(s or '').strip().lower()
    if not txt:
        return 'unknown'
    alias = {
        'tech': 'technology',
        'it': 'technology',
        'software': 'technology',
        'internet': 'technology',
        'semiconductor': 'technology',
        'semiconductors': 'technology',
        'consumer discretionary': 'consumer',
        'consumer staples': 'consumer',
        'retail': 'consumer',
        'industrials': 'industrial',
        'manufacturing': 'industrial',
        'capital goods': 'industrial',
        'health': 'healthcare',
        'health care': 'healthcare',
        'medical': 'healthcare',
        'pharma': 'healthcare',
        'biotech': 'healthcare',
        'energy services': 'energy',
        'oil & gas': 'energy',
        'utilities': 'utility',
        'telecommunications': 'telecom',
        'communication services': 'telecom',
        'banking': 'bank',
        'banks': 'bank',
        'financial': 'bank',
        'financials': 'bank',
        'financial_services': 'bank',
        'brokerage': 'broker_dealer',
        'asset management': 'broker_dealer',
        'insurer': 'insurance',
        'insurance services': 'insurance',
        'real estate': 'reit',
        'real_estate': 'reit',
        'materials': 'materials',
        'chemicals': 'materials',
        'transportation': 'transport',
        'airlines': 'transport',
        'aerospace': 'aerospace_defense',
        'defense': 'aerospace_defense',
    }
    return alias.get(txt, txt)


def load_registry() -> Dict:
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is not None:
        return _REGISTRY_CACHE
    root = Path(__file__).resolve().parents[1]
    path = root / 'config' / 'canonical_registry.json'
    if not path.exists():
        _REGISTRY_CACHE = {
            'version': 'fallback',
            'global': dict(_DEFAULT_PRIORITY),
            'sector_overrides': {},
            'not_applicable': {},
        }
        return _REGISTRY_CACHE
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        payload = {}
    _REGISTRY_CACHE = {
        'version': payload.get('version', 'v1'),
        'global': payload.get('global') or dict(_DEFAULT_PRIORITY),
        'sector_overrides': payload.get('sector_overrides') or {},
        'not_applicable': payload.get('not_applicable') or {},
    }
    return _REGISTRY_CACHE


def get_priority_list(concept_id: str, sector_profile: Optional[str]) -> List[str]:
    reg = load_registry()
    sector = _normalize_sector(sector_profile)
    merged: List[str] = []
    for src in (
        ((reg.get('sector_overrides') or {}).get(sector, {}) or {}).get(concept_id, []),
        (reg.get('global') or {}).get(concept_id, []),
        _DEFAULT_PRIORITY.get(concept_id, []),
    ):
        for tag in src or []:
            if isinstance(tag, str) and tag and tag not in merged:
                merged.append(tag)
    return merged


def is_not_applicable(concept_id: str, sector_profile: Optional[str]) -> bool:
    reg = load_registry()
    sector = _normalize_sector(sector_profile)
    blocked = ((reg.get('not_applicable') or {}).get(sector) or [])
    return concept_id in set(blocked)
