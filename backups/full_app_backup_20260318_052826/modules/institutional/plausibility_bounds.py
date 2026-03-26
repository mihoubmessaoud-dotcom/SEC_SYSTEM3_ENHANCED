from __future__ import annotations

from typing import Dict, Optional, Tuple


SECTOR_BOUNDS: Dict[str, Dict[str, Tuple[float, float]]] = {
    'bank': {
        'net_interest_margin': (-0.03, 0.08),
        'loan_to_deposit_ratio': (0.20, 2.50),
        'capital_ratio_proxy': (0.03, 0.25),
        'roa': (-0.05, 0.05),
        'roe': (-0.50, 0.50),
        'asset_turnover': (0.0, 0.30),
        'current_ratio': (0.0, 5.0),
        'quick_ratio': (0.0, 5.0),
        'debt_to_equity': (0.0, 30.0),
        'debt_to_assets': (0.0, 0.98),
    },
    'insurance': {
        'roa': (-0.20, 0.20),
        'roe': (-1.00, 2.00),
        'net_margin': (-1.00, 1.00),
        'operating_margin': (-1.00, 1.00),
        'debt_to_equity': (0.0, 10.0),
        'debt_to_assets': (0.0, 0.95),
        'current_ratio': (0.0, 10.0),
        'quick_ratio': (0.0, 10.0),
        'asset_turnover': (0.0, 1.0),
    },
    'industrial': {
        'gross_margin': (-0.20, 0.90),
        'operating_margin': (-0.50, 0.50),
        'net_margin': (-0.50, 0.50),
        'roa': (-0.30, 0.30),
        'roe': (-1.00, 2.00),
        'current_ratio': (0.20, 15.0),
        'quick_ratio': (0.10, 15.0),
        'debt_to_equity': (0.0, 10.0),
        'debt_to_assets': (0.0, 0.95),
        'asset_turnover': (0.0, 5.0),
        'inventory_turnover': (0.0, 40.0),
    },
    'broker_dealer': {
        'roa': (-0.10, 0.10),
        'roe': (-1.00, 2.00),
        'net_margin': (-1.00, 1.00),
        'debt_to_equity': (0.0, 50.0),
        'debt_to_assets': (0.0, 0.99),
        'asset_turnover': (0.0, 1.0),
    },
}


GENERIC_BOUNDS: Dict[str, Tuple[float, float]] = {
    'gross_margin': (-1.0, 1.0),
    'operating_margin': (-1.0, 1.0),
    'net_margin': (-1.0, 1.0),
    'roa': (-0.50, 0.50),
    'roe': (-2.0, 3.0),
    'current_ratio': (0.0, 20.0),
    'quick_ratio': (0.0, 20.0),
    'debt_to_equity': (0.0, 10.0),
    'debt_to_assets': (0.0, 1.0),
    'asset_turnover': (0.0, 10.0),
    'inventory_turnover': (0.0, 50.0),
    'net_interest_margin': (-0.05, 0.10),
    'loan_to_deposit_ratio': (0.0, 3.0),
    'capital_ratio_proxy': (0.0, 0.50),
    'fcf_margin': (-1.0, 1.0),
    'net_income_to_assets': (-0.50, 0.50),
    'equity_ratio': (0.0, 1.0),
    'combined_proxy': (0.0, 5.0),
    'capital_adequacy_proxy': (0.0, 5.0),
    'returns_on_assets_proxy': (-0.50, 0.50),
    'capital_buffer_proxy': (0.0, 5.0),
}


def get_bounds(profile: str, ratio_id: str) -> Optional[Tuple[float, float]]:
    sector_map = SECTOR_BOUNDS.get(profile, {})
    return sector_map.get(ratio_id) or GENERIC_BOUNDS.get(ratio_id)
