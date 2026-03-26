from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, List

from .canonical_resolver import resolve_item
from .ratio_formats import canonicalize_ratio_value, get_ratio_metadata


class RatioEngine:
    """
    Canonical ratio engine used as single source of truth.
    - Returns ratio contracts only from this layer.
    - Enforces canonical revenue/COGS selection and strict sanity gates.
    """

    REVENUE_TAGS = [
        'Revenue_Hierarchy',
        'NetRevenue_Hierarchy',
        'NetRevenue',
        'Revenues',
        'SalesRevenueNet',
        'RevenuesQTD',
        'SalesRevenueNetQTD',
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'Revenue',
    ]
    COGS_TAGS = [
        'CostOfRevenue',
        'CostOfGoodsAndServicesSold',
        'COGS',
        'CostOfSales',
        'CostOfProductsSold',
    ]
    AR_TAGS = [
        'AccountsReceivableNetCurrent_Hierarchy',
        'AccountsReceivableNetCurrent',
        'AccountsReceivable',
        'ReceivablesNetCurrent',
        'FinancingReceivableExcludingAccruedInterestBeforeAllowanceForCreditLoss',
        'FinancingReceivableAccruedInterestBeforeAllowanceForCreditLoss',
    ]
    AP_TAGS = [
        'AccountsPayableCurrent_Hierarchy',
        'AccountsPayableCurrent',
        'AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent',
        'AccountsPayable',
    ]
    INVENTORY_TAGS = ['InventoryNet_Hierarchy', 'InventoryNet', 'Inventory']
    NI_TAGS = ['NetIncomeLoss', 'ProfitLoss']
    OP_INCOME_TAGS = ['OperatingIncomeLoss', 'IncomeLossFromOperations']
    ASSETS_TAGS = ['Assets', 'Assets_Hierarchy', 'TotalAssets']
    CURRENT_ASSETS_TAGS = ['AssetsCurrent_Hierarchy', 'AssetsCurrent', 'CurrentAssets']
    CURRENT_LIABILITIES_TAGS = ['LiabilitiesCurrent_Hierarchy', 'LiabilitiesCurrent', 'CurrentLiabilities']
    EQUITY_TAGS = ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest']
    GROSS_PROFIT_TAGS = ['GrossProfit', 'GrossProfit_Hierarchy']

    BASE_RATIO_IDS = [
        'roe',
        'roa',
        'gross_margin',
        'operating_margin',
        'net_margin',
        'days_sales_outstanding',
        'ap_days',
        'inventory_days',
        'pe_ratio',
        'pb_ratio',
        'dividend_yield',
        'sgr_internal',
        'retention_ratio',
        'roic',
        'ccc_days',
        'altman_z_score',
        'accruals_ratio',
        'net_debt_ebitda',
        'interest_coverage',
        'investment_score',
        'economic_spread',
        'wacc',
        'book_value_per_share',
        'eps_basic',
        'fcf_yield',
        'free_cash_flow',
        'fcf_per_share',
        'dividends_paid',
    ]

    IMPLAUSIBLE_BOUNDS = {
        'gross_margin': (-0.5, 0.95),
        'roe': (-2.0, 2.0),
        'pe_ratio': (-500.0, 500.0),
        'pb_ratio': (-200.0, 200.0),
        'net_debt_ebitda': (-20.0, 20.0),
        'interest_coverage': (-500.0, 500.0),
        'book_value_per_share': (-2_000.0, 2_000.0),
        'eps_basic': (-2_000.0, 2_000.0),
        'days_sales_outstanding': (0.0, 3650.0),
        'ap_days': (0.0, 3650.0),
        'inventory_days': (0.0, 3650.0),
        'ccc_days': (-3650.0, 3650.0),
    }
    RELIABILITY_PENALTIES = {
        'concept_fallback_used': 40,
        'fallback_companyfacts_no_statement_anchor': 80,
        'fy_mismatch': 60,
        'unit_mismatch': 50,
        'plausibility_violation': 30,
        'multi_year_consistency_fail': 25,
        'html_xbrl_divergence': 20,
        'exception_policy': 30,
        'parent_child_mismatch': 40,
    }
    RELIABILITY_BOOSTS = {
        'parent_child_validated_le_1pct': 25,
        'parent_child_validated_le_2pct': 10,
    }

    REASON_CODES = {
        'MISSING_SEC_CONCEPT',
        'MISSING_MARKET_DATA',
        'PERIOD_MISMATCH',
        'UNIT_MISMATCH',
        'ZERO_DENOMINATOR',
        'INSUFFICIENT_HISTORY',
        'DATA_NOT_APPLICABLE',
    }

    RATIO_TYPE = {
        'current_ratio': 'ACCOUNTING_ONLY',
        'quick_ratio': 'ACCOUNTING_ONLY',
        'debt_to_equity': 'ACCOUNTING_ONLY',
        'debt_ratio': 'ACCOUNTING_ONLY',
        'roa': 'ACCOUNTING_ONLY',
        'roe': 'ACCOUNTING_ONLY',
        'gross_margin': 'ACCOUNTING_ONLY',
        'net_margin': 'ACCOUNTING_ONLY',
        'asset_turnover': 'ACCOUNTING_ONLY',
        'inventory_turnover': 'ACCOUNTING_ONLY',
        'interest_coverage': 'ACCOUNTING_ONLY',
        'days_sales_outstanding': 'ACCOUNTING_ONLY',
        'ap_days': 'ACCOUNTING_ONLY',
        'inventory_days': 'ACCOUNTING_ONLY',
        'ccc_days': 'ACCOUNTING_ONLY',
        'altman_z_score': 'ACCOUNTING_ONLY',
        'accruals_ratio': 'ACCOUNTING_ONLY',
        'roic': 'ACCOUNTING_ONLY',
        'free_cash_flow': 'ACCOUNTING_ONLY',
        'fcf_per_share': 'ACCOUNTING_ONLY',
        'book_value_per_share': 'ACCOUNTING_ONLY',
        'sgr_internal': 'ACCOUNTING_ONLY',
        'retention_ratio': 'ACCOUNTING_ONLY',
        'dividends_paid': 'ACCOUNTING_ONLY',
        'pb_ratio': 'MARKET_DEPENDENT',
        'pe_ratio': 'MARKET_DEPENDENT',
        'dividend_yield': 'MARKET_DEPENDENT',
        'fcf_yield': 'MARKET_DEPENDENT',
        'market_cap': 'MARKET_DEPENDENT',
        'wacc': 'HYBRID',
        'economic_spread': 'HYBRID',
        'investment_score': 'HYBRID',
    }

    FORMULA_MAP = {
        'gross_margin': '(Revenue - COGS) / Revenue',
        'operating_margin': 'OperatingIncome / Revenue',
        'net_margin': 'NetIncome / Revenue',
        'roe': 'NetIncome / AvgEquity',
        'roa': 'NetIncome / AvgAssets',
        'current_ratio': 'CurrentAssets / CurrentLiabilities',
        'days_sales_outstanding': '365 * AvgAR / Revenue',
        'ap_days': '365 * AvgAP / COGS',
        'inventory_days': '365 * AvgInventory / COGS',
        'ccc_days': 'DSO + DIH - DPO',
        'pb_ratio': 'MarketCap / Equity',
    }

    def __init__(self) -> None:
        self.computation_timestamp = datetime.now(timezone.utc).isoformat()

    def build(self, data_by_year: Dict, ratios_by_year: Dict) -> Dict:
        years = sorted(
            {y for y in (data_by_year or {}).keys() if isinstance(y, int)}
            | {y for y in (ratios_by_year or {}).keys() if isinstance(y, int)}
        )
        ratio_contracts_by_year: Dict[int, Dict[str, Dict]] = {}
        items_by_year: Dict[int, Dict] = {}
        lockdown_rows: List[Dict] = []
        ratio_reliability_rows: List[Dict] = []
        integrity_rows_by_year: Dict[str, Dict] = {}
        canonical_diag_by_year: Dict[str, Dict] = {}

        for year in years:
            row = (ratios_by_year or {}).get(year, {}) or {}
            raw = (data_by_year or {}).get(year, {}) or {}
            prev_raw = (data_by_year or {}).get(year - 1, {}) or {}
            contracts = {}

            rev = self.get_canonical_annual_revenue(year, raw)
            cogs = self.get_canonical_cogs(year, raw)
            equity = self.get_canonical_equity(year, raw)
            ar = self.get_canonical_accounts_receivable(year, raw)
            ap = self.get_canonical_accounts_payable(year, raw)
            inv = self.get_canonical_inventory(year, raw)
            assets = self.get_canonical_assets(year, raw)
            current_assets = self.get_canonical_current_assets(year, raw)
            current_liabilities = self.get_canonical_current_liabilities(year, raw)
            net_income = self.get_canonical_net_income(year, raw)
            operating_income = self.get_canonical_operating_income(year, raw)

            # fail-closed for currency / scale mismatch
            if rev.get('value') is not None and cogs.get('value') is not None:
                if rev.get('currency') and cogs.get('currency') and rev.get('currency') != cogs.get('currency'):
                    cogs['value'] = None
                    cogs['selection_reason'] = 'unit_mismatch'
                if rev.get('scale_applied') and cogs.get('scale_applied'):
                    if float(rev.get('scale_applied')) != float(cogs.get('scale_applied')):
                        cogs['value'] = None
                        cogs['selection_reason'] = 'unit_mismatch'
            # try next COGS candidate if gross margin is implausibly high
            if rev.get('value') and cogs.get('value'):
                gm_probe = (float(rev.get('value')) - float(cogs.get('value'))) / float(rev.get('value'))
                if gm_probe > 0.95:
                    for cand in cogs.get('candidates', []):
                        if cand.get('reason_rejected') is None and cand.get('tag') != cogs.get('tag'):
                            cv = self._num(cand.get('value'))
                            if cv is None:
                                continue
                            trial = (float(rev.get('value')) - float(cv)) / float(rev.get('value'))
                            if -0.5 <= trial <= 0.95:
                                cogs['tag'] = cand.get('tag')
                                cogs['value'] = cv
                                cogs['selection_reason'] = 'fallback_candidate_after_gross_margin_gate'
                                break

            item_ctx = {
                'canonical_revenue': rev,
                'canonical_cogs': cogs,
                'canonical_equity': equity,
                'canonical_ar': ar,
                'canonical_ap': ap,
                'canonical_inventory': inv,
                'canonical_assets': assets,
                'canonical_current_assets': current_assets,
                'canonical_current_liabilities': current_liabilities,
                'canonical_net_income': net_income,
                'canonical_operating_income': operating_income,
            }

            canonical_diag_by_year[str(year)] = {
                'year': year,
                'fiscal_end_date_selected': rev.get('period_end') or cogs.get('period_end'),
                'canonical_revenue_tag': rev.get('tag'),
                'canonical_cogs_tag': cogs.get('tag'),
                'canonical_equity_tag': equity.get('tag'),
                'canonical_ar_tag': ar.get('tag'),
                'canonical_ap_tag': ap.get('tag'),
                'canonical_inventory_tag': inv.get('tag'),
                'canonical_assets_tag': assets.get('tag'),
                'canonical_current_assets_tag': current_assets.get('tag'),
                'canonical_current_liabilities_tag': current_liabilities.get('tag'),
                'canonical_net_income_tag': net_income.get('tag'),
                'canonical_operating_income_tag': operating_income.get('tag'),
                'context_type': {
                    'revenue': rev.get('period_type'),
                    'cogs': cogs.get('period_type'),
                    'equity': equity.get('period_type'),
                    'accounts_receivable': ar.get('period_type'),
                    'accounts_payable': ap.get('period_type'),
                    'inventory': inv.get('period_type'),
                    'assets': assets.get('period_type'),
                    'current_assets': current_assets.get('period_type'),
                    'current_liabilities': current_liabilities.get('period_type'),
                    'net_income': net_income.get('period_type'),
                    'operating_income': operating_income.get('period_type'),
                },
                'units_scale': {
                    'revenue': {'unit': rev.get('unit'), 'scale': rev.get('scale_applied')},
                    'cogs': {'unit': cogs.get('unit'), 'scale': cogs.get('scale_applied')},
                    'equity': {'unit': equity.get('unit'), 'scale': equity.get('scale_applied')},
                    'accounts_receivable': {'unit': ar.get('unit'), 'scale': ar.get('scale_applied')},
                    'accounts_payable': {'unit': ap.get('unit'), 'scale': ap.get('scale_applied')},
                    'inventory': {'unit': inv.get('unit'), 'scale': inv.get('scale_applied')},
                    'assets': {'unit': assets.get('unit'), 'scale': assets.get('scale_applied')},
                    'current_assets': {'unit': current_assets.get('unit'), 'scale': current_assets.get('scale_applied')},
                    'current_liabilities': {'unit': current_liabilities.get('unit'), 'scale': current_liabilities.get('scale_applied')},
                    'net_income': {'unit': net_income.get('unit'), 'scale': net_income.get('scale_applied')},
                    'operating_income': {'unit': operating_income.get('unit'), 'scale': operating_income.get('scale_applied')},
                },
                'selection_confidence': {
                    'revenue': rev.get('confidence', 0),
                    'cogs': cogs.get('confidence', 0),
                    'equity': equity.get('confidence', 0),
                    'accounts_receivable': ar.get('confidence', 0),
                    'accounts_payable': ap.get('confidence', 0),
                    'inventory': inv.get('confidence', 0),
                    'assets': assets.get('confidence', 0),
                    'current_assets': current_assets.get('confidence', 0),
                    'current_liabilities': current_liabilities.get('confidence', 0),
                    'net_income': net_income.get('confidence', 0),
                    'operating_income': operating_income.get('confidence', 0),
                },
                'top_candidates': {
                    'revenue': rev.get('candidates', []),
                    'cogs': cogs.get('candidates', []),
                    'equity': equity.get('candidates', []),
                    'accounts_receivable': ar.get('candidates', []),
                    'accounts_payable': ap.get('candidates', []),
                    'inventory': inv.get('candidates', []),
                    'assets': assets.get('candidates', []),
                    'current_assets': current_assets.get('candidates', []),
                    'current_liabilities': current_liabilities.get('candidates', []),
                    'net_income': net_income.get('candidates', []),
                    'operating_income': operating_income.get('candidates', []),
                },
            }
            integrity_rows_by_year[str(year)] = self._build_integrity_row(year, rev, cogs, equity, ar, ap, inv)

            passthrough_skip = {
                'roe',
                'gross_margin',
                'days_sales_outstanding',
                'ap_days',
                'inventory_days',
                'ccc_days',
                'pb_ratio',
                'operating_margin',
                'net_margin',
                'roa',
                'current_ratio',
            }
            for rid in [r for r in self.BASE_RATIO_IDS if r not in passthrough_skip]:
                contracts[rid] = self._build_base_contract(rid, row.get(rid))
                lockdown_rows.append(self._lockdown_row(year, rid, contracts[rid], {}, rev, cogs))

            contracts['gross_margin'] = self._compute_gross_margin(rev, cogs)
            self._apply_reliability_penalties(
                contracts['gross_margin'],
                rev,
                cogs,
                extra_items=[rev, cogs],
                ratio_id='gross_margin',
                year=year,
                raw=raw,
            )
            lockdown_rows.append(
                self._lockdown_row(
                    year,
                    'gross_margin',
                    contracts['gross_margin'],
                    {'revenue': rev.get('value'), 'cogs': cogs.get('value')},
                    rev,
                    cogs,
                    input_tags=[rev.get('tag'), cogs.get('tag')],
                )
            )

            contracts['roe'] = self._compute_roe(raw, prev_raw, equity)
            self._apply_reliability_penalties(
                contracts['roe'],
                rev,
                cogs,
                extra_items=[equity],
                ratio_id='roe',
                year=year,
                raw=raw,
            )
            lockdown_rows.append(
                self._lockdown_row(
                    year,
                    'roe',
                    contracts['roe'],
                    contracts['roe'].get('inputs', {}),
                    rev,
                    cogs,
                    input_tags=contracts['roe'].get('input_tags', []),
                )
            )

            contracts['days_sales_outstanding'] = self._compute_days_ratio('dso', raw, prev_raw, rev, cogs, ar, ap, inv)
            contracts['ap_days'] = self._compute_days_ratio('dpo', raw, prev_raw, rev, cogs, ar, ap, inv)
            contracts['inventory_days'] = self._compute_days_ratio('dih', raw, prev_raw, rev, cogs, ar, ap, inv)
            contracts['operating_margin'] = self._compute_operating_margin(rev, operating_income)
            contracts['net_margin'] = self._compute_net_margin(rev, net_income)
            contracts['roa'] = self._compute_roa(raw, prev_raw, assets, net_income)
            contracts['current_ratio'] = self._compute_current_ratio(current_assets, current_liabilities)
            contracts['ccc_days'] = self._compute_ccc(
                contracts['days_sales_outstanding'],
                contracts['inventory_days'],
                contracts['ap_days'],
            )
            self._apply_reliability_penalties(
                contracts['days_sales_outstanding'],
                rev,
                cogs,
                extra_items=[ar, rev],
                ratio_id='days_sales_outstanding',
                year=year,
                raw=raw,
            )
            self._apply_reliability_penalties(
                contracts['ap_days'],
                rev,
                cogs,
                extra_items=[ap, cogs],
                ratio_id='ap_days',
                year=year,
                raw=raw,
            )
            self._apply_reliability_penalties(
                contracts['inventory_days'],
                rev,
                cogs,
                extra_items=[inv, cogs],
                ratio_id='inventory_days',
                year=year,
                raw=raw,
            )
            self._apply_reliability_penalties(
                contracts['operating_margin'],
                rev,
                cogs,
                extra_items=[operating_income, rev],
                ratio_id='operating_margin',
                year=year,
                raw=raw,
            )
            self._apply_reliability_penalties(
                contracts['net_margin'],
                rev,
                cogs,
                extra_items=[net_income, rev],
                ratio_id='net_margin',
                year=year,
                raw=raw,
            )
            self._apply_reliability_penalties(
                contracts['roa'],
                rev,
                cogs,
                extra_items=[assets, net_income],
                ratio_id='roa',
                year=year,
                raw=raw,
            )
            self._apply_reliability_penalties(
                contracts['current_ratio'],
                rev,
                cogs,
                extra_items=[current_assets, current_liabilities],
                ratio_id='current_ratio',
                year=year,
                raw=raw,
            )
            self._apply_reliability_penalties(
                contracts['ccc_days'],
                rev,
                cogs,
                extra_items=[ar, ap, inv, rev, cogs],
                ratio_id='ccc_days',
                year=year,
                raw=raw,
            )
            for rid in ['days_sales_outstanding', 'ap_days', 'inventory_days', 'ccc_days']:
                lockdown_rows.append(
                    self._lockdown_row(
                        year,
                        rid,
                        contracts[rid],
                        contracts[rid].get('inputs', {}),
                        rev,
                        cogs,
                        input_tags=contracts[rid].get('input_tags', []),
                    )
                )
            for rid in ['operating_margin', 'net_margin', 'roa', 'current_ratio']:
                lockdown_rows.append(
                    self._lockdown_row(
                        year,
                        rid,
                        contracts[rid],
                        contracts[rid].get('inputs', {}),
                        rev,
                        cogs,
                        input_tags=contracts[rid].get('input_tags', []),
                    )
                )

            contracts['pb_ratio'] = self._compute_pb(row, raw, prev_raw, equity)
            self._apply_reliability_penalties(
                contracts['pb_ratio'],
                rev,
                cogs,
                extra_items=[equity],
                ratio_id='pb_ratio',
                year=year,
                raw=raw,
            )
            lockdown_rows.append(
                self._lockdown_row(
                    year,
                    'pb_ratio',
                    contracts['pb_ratio'],
                    contracts['pb_ratio'].get('inputs', {}),
                    rev,
                    cogs,
                    input_tags=contracts['pb_ratio'].get('input_tags', []),
                )
            )

            for rid, c in list(contracts.items()):
                contracts[rid] = self._finalize_contract(rid, c)

            ratio_contracts_by_year[year] = contracts
            for rid, c in sorted(contracts.items()):
                ratio_reliability_rows.append(
                    {
                        'year': year,
                        'ratio_id': rid,
                        'value': c.get('value'),
                        'reliability': c.get('reliability', 0),
                        'reason': c.get('reason'),
                        'source': c.get('source'),
                        'bounds_result': c.get('bounds_result'),
                    }
                )
            items_by_year[year] = item_ctx

        self._write_canonical_diagnostics(canonical_diag_by_year)
        self._write_data_integrity_diagnostics(integrity_rows_by_year)
        self._write_ratio_reliability_report(ratio_reliability_rows)
        self._write_ratio_results_with_explanations(ratio_contracts_by_year)
        return {
            'ratios': ratio_contracts_by_year,
            'items': items_by_year,
            'contracts_by_year': ratio_contracts_by_year,
            'diagnostics': {'errors': []},
            'lockdown_report': lockdown_rows,
            'canonical_item_selection_diagnostics': canonical_diag_by_year,
            'data_integrity_diagnostics': integrity_rows_by_year,
            'ratio_reliability_report': ratio_reliability_rows,
        }

    def get_canonical_annual_revenue(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('annual_revenue') or [])
        if strict_tree:
            candidates = list(by_meta)
        else:
            candidates = list(by_meta) if by_meta else [{'tag': t, 'value': raw.get(t)} for t in self.REVENUE_TAGS]
        for tag in ('Revenue_Hierarchy', 'NetRevenue_Hierarchy'):
            v = self._num((raw or {}).get(tag))
            if not strict_tree and v is not None and all((c.get('tag') != tag) for c in candidates):
                candidates.insert(0, {'tag': tag, 'value': v, 'period_type': 'FY'})
        return resolve_item(
            year,
            'annual_revenue',
            candidates,
            require_fy=True,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_cogs(self, year: int, raw: Dict, allow_negative_exception: bool = False) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('annual_cogs') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t)} for t in self.COGS_TAGS]
        resolved = resolve_item(
            year,
            'annual_cogs',
            candidates,
            require_fy=True,
            allow_negative=allow_negative_exception,
            sector_profile=sector_profile,
        )
        if self._num(resolved.get('value')) is None:
            rev = self.get_canonical_annual_revenue(year, raw)
            rv = self._num(rev.get('value'))
            gp = None
            for t in self.GROSS_PROFIT_TAGS:
                v = self._num((raw or {}).get(t))
                if v is not None:
                    gp = v
                    break
            if rv is not None and gp is not None:
                cogs_derived = rv - gp
                if cogs_derived >= 0:
                    resolved['value'] = float(cogs_derived)
                    resolved['tag'] = 'derived:RevenueMinusGrossProfit'
                    resolved['selection_reason'] = 'derived_from_revenue_minus_gross_profit'
                    resolved['confidence'] = min(int(resolved.get('confidence') or 100), 85)
        return resolved

    def get_canonical_equity(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('total_equity') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'INSTANT'} for t in self.EQUITY_TAGS]
        return resolve_item(
            year,
            'total_equity',
            candidates,
            require_fy=False,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_accounts_receivable(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('accounts_receivable') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'INSTANT'} for t in self.AR_TAGS]
        v = self._num((raw or {}).get('AccountsReceivableNetCurrent_Hierarchy'))
        if (not strict_tree) and v is not None and all((c.get('tag') != 'AccountsReceivableNetCurrent_Hierarchy') for c in candidates):
            candidates.insert(0, {'tag': 'AccountsReceivableNetCurrent_Hierarchy', 'value': v, 'period_type': 'INSTANT'})
        return resolve_item(
            year,
            'accounts_receivable',
            candidates,
            require_fy=False,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_accounts_payable(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('accounts_payable') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'INSTANT'} for t in self.AP_TAGS]
        v = self._num((raw or {}).get('AccountsPayableCurrent_Hierarchy'))
        if (not strict_tree) and v is not None and all((c.get('tag') != 'AccountsPayableCurrent_Hierarchy') for c in candidates):
            candidates.insert(0, {'tag': 'AccountsPayableCurrent_Hierarchy', 'value': v, 'period_type': 'INSTANT'})
        return resolve_item(
            year,
            'accounts_payable',
            candidates,
            require_fy=False,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_inventory(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('inventory') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'INSTANT'} for t in self.INVENTORY_TAGS]
        v = self._num((raw or {}).get('InventoryNet_Hierarchy'))
        if (not strict_tree) and v is not None and all((c.get('tag') != 'InventoryNet_Hierarchy') for c in candidates):
            candidates.insert(0, {'tag': 'InventoryNet_Hierarchy', 'value': v, 'period_type': 'INSTANT'})
        return resolve_item(
            year,
            'inventory',
            candidates,
            require_fy=False,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_assets(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('total_assets') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'INSTANT'} for t in self.ASSETS_TAGS]
        return resolve_item(
            year,
            'total_assets',
            candidates,
            require_fy=False,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_current_assets(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('current_assets') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'INSTANT'} for t in self.CURRENT_ASSETS_TAGS]
        return resolve_item(
            year,
            'current_assets',
            candidates,
            require_fy=False,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_current_liabilities(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('current_liabilities') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'INSTANT'} for t in self.CURRENT_LIABILITIES_TAGS]
        return resolve_item(
            year,
            'current_liabilities',
            candidates,
            require_fy=False,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_net_income(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('net_income') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'FY'} for t in self.NI_TAGS]
        return resolve_item(
            year,
            'net_income',
            candidates,
            require_fy=True,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def get_canonical_operating_income(self, year: int, raw: Dict) -> Dict:
        sector_profile = self._resolve_sector_profile_from_row(raw)
        strict_tree = bool((raw or {}).get('__statement_tree_required__'))
        by_meta = (((raw or {}).get('__canonical_fact_candidates__') or {}).get('operating_income') or [])
        candidates = list(by_meta) if (strict_tree or by_meta) else [{'tag': t, 'value': raw.get(t), 'period_type': 'FY'} for t in self.OP_INCOME_TAGS]
        return resolve_item(
            year,
            'operating_income',
            candidates,
            require_fy=True,
            allow_negative=True,
            sector_profile=sector_profile,
        )

    def _compute_gross_margin(self, rev: Dict, cogs: Dict) -> Dict:
        out = self._contract(None, 0, 'gross_margin_inputs_missing', 'gross_margin', get_ratio_metadata('gross_margin'))
        if 'not_applicable_for_sector' in str(cogs.get('selection_reason') or ''):
            out['reason'] = 'ratio_not_applicable_sector'
            return out
        rv = self._num(rev.get('value'))
        cg = self._num(cogs.get('value'))
        out['inputs'] = {'revenue': rv, 'cogs': cg}
        out['input_tags'] = [rev.get('tag'), cogs.get('tag')]
        out['period_end'] = rev.get('period_end') or cogs.get('period_end')
        out['formula_used'] = self.FORMULA_MAP.get('gross_margin')
        if rv == 0:
            out['reason'] = 'zero_denominator'
            return out
        if rv is None or rv == 0 or cg is None:
            return out
        gm = (rv - cg) / rv
        out['value'] = float(gm)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, 'gross_margin')
        return out

    def _compute_roe(self, raw: Dict, prev_raw: Dict, equity_ctx: Dict) -> Dict:
        out = self._contract(None, 0, 'roe_inputs_missing_or_near_zero', 'roe', get_ratio_metadata('roe'))
        ni = self._pick(raw, self.NI_TAGS)
        eq = self._num(equity_ctx.get('value'))
        eq_prev = self._pick(prev_raw, self.EQUITY_TAGS)
        avg_eq = None
        if eq is not None and eq_prev is not None:
            avg_eq = (eq + eq_prev) / 2.0
        elif eq is not None:
            avg_eq = eq
        out['input_tags'] = [self._pick_tag(raw, self.NI_TAGS), equity_ctx.get('tag'), self._pick_tag(prev_raw, self.EQUITY_TAGS)]
        out['inputs'] = {'net_income': ni, 'equity': eq, 'prev_equity': eq_prev, 'avg_equity': avg_eq}
        out['formula_used'] = self.FORMULA_MAP.get('roe')
        out['period_end'] = equity_ctx.get('period_end')
        if avg_eq is not None and abs(avg_eq) < 1e-12:
            out['reason'] = 'zero_denominator'
            return out
        if ni is None or avg_eq is None or abs(avg_eq) < 1e-12:
            return out
        roe = ni / avg_eq
        out['value'] = float(roe)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, 'roe')
        return out

    def _compute_days_ratio(self, kind: str, raw: Dict, prev_raw: Dict, rev: Dict, cogs: Dict, ar_ctx: Dict, ap_ctx: Dict, inv_ctx: Dict) -> Dict:
        rid = {'dso': 'days_sales_outstanding', 'dpo': 'ap_days', 'dih': 'inventory_days'}[kind]
        out = self._contract(None, 0, 'day_ratio_inputs_missing', rid, get_ratio_metadata(rid))
        if kind in ('dpo', 'dih') and 'not_applicable_for_sector' in str(cogs.get('selection_reason') or ''):
            out['reason'] = 'ratio_not_applicable_sector'
            return out
        if kind == 'dih' and 'not_applicable_for_sector' in str(inv_ctx.get('selection_reason') or ''):
            out['reason'] = 'ratio_not_applicable_sector'
            return out
        if kind == 'dso' and rev.get('period_type') != 'FY':
            # If only QTD-style revenue exists, expose a precise failure reason.
            if rev.get('value') is None:
                qtd_tags = ('RevenuesQTD', 'SalesRevenueNetQTD')
                if any(self._num((raw or {}).get(t)) is not None for t in qtd_tags):
                    out['reason'] = 'revenue_not_fy'
                    return out
            out['reason'] = 'revenue_not_fy'
            return out
        if kind == 'dso':
            cur = self._aggregate_receivables(raw, ar_ctx.get('value'))
            prev = self._aggregate_receivables(prev_raw, prev_raw.get('AccountsReceivableNetCurrent_Hierarchy'))
            den = self._num(rev.get('value'))
            den_name = 'revenue'
            input_tags = [ar_ctx.get('tag'), 'AccountsReceivableNetCurrent_Hierarchy', rev.get('tag')]
        elif kind == 'dpo':
            cur = self._num(ap_ctx.get('value'))
            prev = self._num(prev_raw.get('AccountsPayableCurrent_Hierarchy'))
            den = self._num(cogs.get('value'))
            den_name = 'cogs'
            input_tags = [ap_ctx.get('tag'), 'AccountsPayableCurrent_Hierarchy', cogs.get('tag')]
        else:
            cur = self._num(inv_ctx.get('value'))
            prev = self._num(prev_raw.get('InventoryNet_Hierarchy'))
            den = self._num(cogs.get('value'))
            den_name = 'cogs'
            input_tags = [inv_ctx.get('tag'), 'InventoryNet_Hierarchy', cogs.get('tag')]
        avg_bal = None
        if cur is not None and prev is not None:
            avg_bal = (cur + prev) / 2.0
        elif cur is not None:
            avg_bal = cur
        out['input_tags'] = input_tags
        out['inputs'] = {'current_balance': cur, 'prev_balance': prev, 'avg_balance': avg_bal, den_name: den}
        out['formula_used'] = self.FORMULA_MAP.get(rid)
        out['period_end'] = rev.get('period_end') or cogs.get('period_end')
        if den == 0:
            out['reason'] = 'zero_denominator'
            return out
        if avg_bal is None or den is None or den == 0:
            return out
        out['value'] = 365.0 * float(avg_bal) / float(den)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, rid)
        return out

    def _aggregate_receivables(self, raw: Dict, fallback: Optional[float]) -> Optional[float]:
        direct = self._num((raw or {}).get('AccountsReceivableNetCurrent_Hierarchy'))
        if direct is not None:
            return direct
        return self._num(fallback)

    def _compute_ccc(self, dso: Dict, dih: Dict, dpo: Dict) -> Dict:
        out = self._contract(None, 0, 'missing_ccc_components', 'ccc_days', get_ratio_metadata('ccc_days'))
        if not (self._is_value(dso) or self._is_value(dih) or self._is_value(dpo)):
            return out
        ccc = self._num_or_zero(dso) + self._num_or_zero(dih) - self._num_or_zero(dpo)
        out['value'] = float(ccc)
        out['reliability'] = min(
            int(dso.get('reliability', 100)),
            int(dih.get('reliability', 100)),
            int(dpo.get('reliability', 100)),
        )
        out['reason'] = None
        out['input_tags'] = list((dso.get('input_tags') or []) + (dih.get('input_tags') or []) + (dpo.get('input_tags') or []))
        out['inputs'] = {'dso': dso.get('value'), 'dih': dih.get('value'), 'dpo': dpo.get('value')}
        out['formula_used'] = self.FORMULA_MAP.get('ccc_days')
        out['period_end'] = dso.get('period_end') or dih.get('period_end') or dpo.get('period_end')
        self._apply_bounds_or_reject(out, 'ccc_days')
        return out

    def _compute_operating_margin(self, rev: Dict, op_income: Dict) -> Dict:
        out = self._contract(None, 0, 'operating_margin_inputs_missing', 'operating_margin', get_ratio_metadata('operating_margin'))
        rv = self._num(rev.get('value'))
        oi = self._num(op_income.get('value'))
        out['inputs'] = {'revenue': rv, 'operating_income': oi}
        out['input_tags'] = [rev.get('tag'), op_income.get('tag')]
        out['period_end'] = rev.get('period_end') or op_income.get('period_end')
        out['formula_used'] = self.FORMULA_MAP.get('operating_margin')
        if rv == 0:
            out['reason'] = 'zero_denominator'
            return out
        if rv is None or oi is None:
            return out
        out['value'] = float(oi / rv)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, 'operating_margin')
        return out

    def _compute_net_margin(self, rev: Dict, net_income: Dict) -> Dict:
        out = self._contract(None, 0, 'net_margin_inputs_missing', 'net_margin', get_ratio_metadata('net_margin'))
        rv = self._num(rev.get('value'))
        ni = self._num(net_income.get('value'))
        out['inputs'] = {'revenue': rv, 'net_income': ni}
        out['input_tags'] = [rev.get('tag'), net_income.get('tag')]
        out['period_end'] = rev.get('period_end') or net_income.get('period_end')
        out['formula_used'] = self.FORMULA_MAP.get('net_margin')
        if rv == 0:
            out['reason'] = 'zero_denominator'
            return out
        if rv is None or ni is None:
            return out
        out['value'] = float(ni / rv)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, 'net_margin')
        return out

    def _compute_roa(self, raw: Dict, prev_raw: Dict, assets_ctx: Dict, net_income_ctx: Dict) -> Dict:
        out = self._contract(None, 0, 'roa_inputs_missing', 'roa', get_ratio_metadata('roa'))
        ni = self._num(net_income_ctx.get('value'))
        assets = self._num(assets_ctx.get('value'))
        prev_assets = self._pick(prev_raw, self.ASSETS_TAGS)
        avg_assets = None
        if assets is not None and prev_assets is not None:
            avg_assets = (assets + prev_assets) / 2.0
        elif assets is not None:
            avg_assets = assets
        out['input_tags'] = [net_income_ctx.get('tag'), assets_ctx.get('tag'), self._pick_tag(prev_raw, self.ASSETS_TAGS)]
        out['inputs'] = {'net_income': ni, 'assets': assets, 'prev_assets': prev_assets, 'avg_assets': avg_assets}
        out['period_end'] = assets_ctx.get('period_end') or net_income_ctx.get('period_end')
        out['formula_used'] = self.FORMULA_MAP.get('roa')
        if avg_assets is not None and abs(avg_assets) < 1e-12:
            out['reason'] = 'zero_denominator'
            return out
        if ni is None or avg_assets is None:
            return out
        out['value'] = float(ni / avg_assets)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, 'roa')
        return out

    def _compute_current_ratio(self, current_assets_ctx: Dict, current_liabilities_ctx: Dict) -> Dict:
        out = self._contract(None, 0, 'current_ratio_inputs_missing', 'current_ratio', get_ratio_metadata('current_ratio'))
        ca = self._num(current_assets_ctx.get('value'))
        cl = self._num(current_liabilities_ctx.get('value'))
        out['inputs'] = {'current_assets': ca, 'current_liabilities': cl}
        out['input_tags'] = [current_assets_ctx.get('tag'), current_liabilities_ctx.get('tag')]
        out['period_end'] = current_assets_ctx.get('period_end') or current_liabilities_ctx.get('period_end')
        out['formula_used'] = self.FORMULA_MAP.get('current_ratio')
        if cl == 0:
            out['reason'] = 'zero_denominator'
            return out
        if ca is None or cl is None:
            return out
        out['value'] = float(ca / cl)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, 'current_ratio')
        return out

    def _compute_pb(self, ratio_row: Dict, raw: Dict, prev_raw: Dict, equity_ctx: Dict) -> Dict:
        out = self._contract(None, 0, 'pb_missing_required_inputs', 'pb_ratio', get_ratio_metadata('pb_ratio'))
        market_cap = self._num(ratio_row.get('market_cap'))
        price = self._num(ratio_row.get('price'))
        shares = self._num(ratio_row.get('shares_outstanding'))
        equity = self._num(equity_ctx.get('value'))
        if equity is None:
            equity = self._pick(prev_raw, self.EQUITY_TAGS)
        if market_cap is None and price is not None and shares is not None:
            market_cap = price * shares
        out['input_tags'] = ['market_cap', 'price', 'shares_outstanding', equity_ctx.get('tag')]
        out['inputs'] = {'market_cap': market_cap, 'price': price, 'shares': shares, 'equity': equity}
        out['formula_used'] = self.FORMULA_MAP.get('pb_ratio')
        out['period_end'] = equity_ctx.get('period_end')
        if equity is not None and abs(equity) < 1e-12:
            out['reason'] = 'zero_denominator'
            return out
        if market_cap is None or equity is None or abs(equity) < 1e-12:
            return out
        out['value'] = float(market_cap / equity)
        out['reliability'] = 100
        out['reason'] = None
        self._apply_bounds_or_reject(out, 'pb_ratio')
        return out

    def _build_base_contract(self, ratio_id: str, raw_value) -> Dict:
        meta = get_ratio_metadata(ratio_id)
        if raw_value is None:
            if self.RATIO_TYPE.get(ratio_id) == 'MARKET_DEPENDENT':
                return self._contract(None, 0, 'missing_market_data', ratio_id, meta)
            return self._contract(None, 0, 'missing_ratio_from_engine', ratio_id, meta)
        try:
            v = canonicalize_ratio_value(ratio_id, float(raw_value))
        except Exception:
            return self._contract(None, 0, 'non_numeric_ratio_value', ratio_id, meta)
        if v is None or not self._is_finite(v):
            return self._contract(None, 0, 'canonicalization_failed', ratio_id, meta)
        out = self._contract(float(v), 100, None, ratio_id, meta)
        self._apply_bounds_or_reject(out, ratio_id)
        return out

    def _apply_bounds_or_reject(self, contract: Dict, ratio_id: str) -> None:
        low_high = self.IMPLAUSIBLE_BOUNDS.get(ratio_id)
        if not low_high:
            contract['bounds_result'] = {'status': 'not_configured', 'ratio_id': ratio_id}
            return
        value = contract.get('value')
        lo, hi = low_high
        if not isinstance(value, (int, float)):
            contract['bounds_result'] = {'status': 'no_value', 'ratio_id': ratio_id, 'lower': lo, 'upper': hi}
            return
        if value < lo or value > hi:
            self._reject(contract, 'plausibility_violation')
            contract['bounds_result'] = {'status': 'rejected', 'ratio_id': ratio_id, 'lower': lo, 'upper': hi, 'observed': value}
            return
        contract['bounds_result'] = {'status': 'ok', 'ratio_id': ratio_id, 'lower': lo, 'upper': hi, 'observed': value}

    def _apply_reliability_penalties(
        self,
        contract: Dict,
        rev: Dict,
        cogs: Dict,
        *,
        extra_items: List[Dict],
        ratio_id: str,
        year: int,
        raw: Dict,
    ) -> None:
        base = int(contract.get('reliability') or 0)
        if base <= 0:
            return
        penalties = []
        boosts = []

        all_items = [i for i in (extra_items or []) if isinstance(i, dict)]
        if any((i.get('selection_reason') == 'concept_fallback_used') for i in all_items):
            penalties.append('concept_fallback_used')
        if any((i.get('selection_reason') == 'fallback_companyfacts_no_statement_anchor') for i in all_items):
            penalties.append('fallback_companyfacts_no_statement_anchor')
        mismatch_values = []
        for i in all_items:
            mv = i.get('parent_child_mismatch_pct')
            if isinstance(mv, (int, float)):
                mismatch_values.append(float(mv))
                if mv <= 0.01:
                    boosts.append('parent_child_validated_le_1pct')
                elif mv <= 0.02:
                    boosts.append('parent_child_validated_le_2pct')
                elif mv > 0.03:
                    penalties.append('parent_child_mismatch')

        fy_ok = self._fy_alignment_ok(all_items)
        if not fy_ok:
            penalties.append('fy_mismatch')

        if not self._units_compatible(all_items):
            penalties.append('unit_mismatch')

        if contract.get('bounds_result', {}).get('status') == 'rejected' or contract.get('reason') == 'plausibility_violation':
            penalties.append('plausibility_violation')

        if not self._multi_year_consistency_ok(ratio_id, year, contract.get('value')):
            penalties.append('multi_year_consistency_fail')

        html_penalty = self._html_xbrl_divergence_penalty(raw, ratio_id, contract.get('value'))
        if html_penalty:
            penalties.append('html_xbrl_divergence')

        if any(k in raw for k in ('BankruptcyIndicator', 'NegativeEquityIndicator', 'HyperinflationIndicator', 'EarlyStageBiotechIndicator')):
            penalties.append('exception_policy')
            contract['exception_policy'] = {
                'exception_type': self._resolve_exception_type(raw),
                'evidence': self._resolve_exception_evidence(raw),
            }

        score = 100
        for b in boosts:
            score += int(self.RELIABILITY_BOOSTS.get(b, 0))
        for p in penalties:
            score -= int(self.RELIABILITY_PENALTIES.get(p, 0))
        score = max(0, min(100, score))
        if 'fallback_companyfacts_no_statement_anchor' in penalties:
            score = min(score, 20)
        if mismatch_values and max(mismatch_values) > 0.05:
            self._reject(contract, 'parent_child_mismatch')
            contract['reliability_penalties'] = penalties
            contract['reliability_boosts'] = boosts
            return
        if 'fy_mismatch' in penalties:
            self._reject(contract, 'fy_mismatch')
            contract['reliability_penalties'] = penalties
            return
        if 'unit_mismatch' in penalties:
            self._reject(contract, 'unit_mismatch')
            contract['reliability_penalties'] = penalties
            return
        contract['reliability'] = score
        if score == 0 and contract.get('value') is not None:
            self._reject(contract, contract.get('reason') or penalties[0] if penalties else 'rejected')
        if not contract.get('reason') and penalties:
            contract['reason'] = penalties[0]
        contract['reliability_penalties'] = penalties
        contract['reliability_boosts'] = boosts

    def _fy_alignment_ok(self, items: List[Dict]) -> bool:
        dates = []
        for i in items:
            dt = self._parse_date(i.get('period_end'))
            if dt is not None:
                dates.append(dt)
        if len(dates) <= 1:
            return True
        low = min(dates)
        high = max(dates)
        return abs((high - low).days) <= 7

    def _units_compatible(self, items: List[Dict]) -> bool:
        currencies = set()
        for i in items:
            cur = i.get('currency')
            if isinstance(cur, str) and cur.strip():
                currencies.add(cur.strip().upper())
        return len(currencies) <= 1

    def _multi_year_consistency_ok(self, ratio_id: str, year: int, value) -> bool:
        if not isinstance(value, (int, float)):
            return True
        _ = ratio_id
        _ = year
        return True

    def _html_xbrl_divergence_penalty(self, raw: Dict, ratio_id: str, value) -> bool:
        html = raw.get('__html_table_values__')
        if not isinstance(html, dict):
            return False
        hv = html.get(ratio_id)
        if not isinstance(hv, (int, float)) or not isinstance(value, (int, float)):
            return False
        denom = max(abs(float(hv)), 1e-9)
        mismatch = abs(float(value) - float(hv)) / denom
        if mismatch > 0.03:
            return True
        return False

    def _resolve_exception_type(self, raw: Dict) -> str:
        if raw.get('BankruptcyIndicator'):
            return 'bankruptcy_distress'
        if raw.get('NegativeEquityIndicator'):
            return 'negative_equity'
        if raw.get('HyperinflationIndicator'):
            return 'hyperinflation'
        if raw.get('EarlyStageBiotechIndicator'):
            return 'early_stage_biotech'
        return 'unknown_exception'

    def _resolve_exception_evidence(self, raw: Dict) -> Dict:
        keys = ['BankruptcyIndicator', 'NegativeEquityIndicator', 'HyperinflationIndicator', 'EarlyStageBiotechIndicator']
        return {k: raw.get(k) for k in keys if k in raw}

    def _lockdown_row(
        self,
        year: int,
        ratio_name: str,
        contract: Dict,
        input_values: Dict,
        canonical_revenue: Dict,
        canonical_cogs: Dict,
        input_tags=None,
    ) -> Dict:
        input_tags = input_tags or []
        return {
            'ratio_name': ratio_name,
            'year': year,
            'input_tags': [t for t in input_tags if t],
            'input_values': input_values or {},
            'canonical_revenue_tag_selected': canonical_revenue.get('tag'),
            'canonical_cogs_tag_selected': canonical_cogs.get('tag'),
            'computed_ratio': contract.get('value'),
            'rejected': contract.get('value') is None,
            'reason': contract.get('reason'),
            'bounds_result': contract.get('bounds_result'),
            'source_layer': 'ratio_engine',
        }

    def _standard_reason_code(self, ratio_id: str, reason: Optional[str], value, inputs: Dict) -> Optional[str]:
        if isinstance(value, (int, float)) and self._is_finite(float(value)):
            return None
        r = str(reason or '').strip().lower()
        ratio_type = self.RATIO_TYPE.get(ratio_id, 'ACCOUNTING_ONLY')
        # Backward-compatible explicit reasons used by regression tests and UI diagnostics.
        if r in {'plausibility_violation', 'revenue_not_fy', 'fy_mismatch', 'parent_child_mismatch', 'missing_ccc_components'}:
            return r
        if ratio_type == 'MARKET_DEPENDENT':
            market_inputs = {'market_cap', 'price', 'shares', 'shares_outstanding'}
            if any((inputs or {}).get(k) is None for k in market_inputs if k in (inputs or {})):
                return 'MISSING_MARKET_DATA'
            if not inputs:
                return 'MISSING_MARKET_DATA'
        if 'zero' in r:
            return 'ZERO_DENOMINATOR'
        if 'unit_mismatch' in r:
            return 'UNIT_MISMATCH'
        if 'fy_mismatch' in r or 'period' in r:
            return 'PERIOD_MISMATCH'
        if 'insufficient' in r or 'history' in r:
            return 'INSUFFICIENT_HISTORY'
        if 'missing' in r:
            return 'MISSING_SEC_CONCEPT' if ratio_type != 'MARKET_DEPENDENT' else 'MISSING_MARKET_DATA'
        if 'not_applicable' in r:
            return 'DATA_NOT_APPLICABLE'
        if 'canonicalization' in r or 'non_numeric' in r or 'plausibility' in r or 'rejected' in r:
            return 'DATA_NOT_APPLICABLE'
        if 'fallback_companyfacts_no_statement_anchor' in r:
            return 'DATA_NOT_APPLICABLE'
        return 'MISSING_SEC_CONCEPT' if ratio_type != 'MARKET_DEPENDENT' else 'MISSING_MARKET_DATA'

    def _derive_missing_inputs(self, inputs: Dict, input_tags: List[str]) -> List[str]:
        missing = []
        for k, v in (inputs or {}).items():
            if v is None:
                missing.append(str(k))
        for t in (input_tags or []):
            if t and t not in missing:
                missing.append(str(t))
        return missing

    def _extract_period(self, contract: Dict) -> Optional[str]:
        p = contract.get('period_end') or contract.get('period')
        if isinstance(p, str) and p.strip():
            return p
        return None

    def _finalize_contract(self, ratio_id: str, contract: Dict) -> Dict:
        c = dict(contract or {})
        ratio_type = self.RATIO_TYPE.get(ratio_id, 'ACCOUNTING_ONLY')
        inputs = c.get('inputs', {}) if isinstance(c.get('inputs'), dict) else {}
        input_tags = list(c.get('input_tags') or [])
        value = c.get('value')
        status = 'COMPUTED' if isinstance(value, (int, float)) and self._is_finite(float(value)) else 'NOT_COMPUTABLE'
        reason_code = self._standard_reason_code(ratio_id, c.get('reason'), value, inputs)
        missing_inputs = self._derive_missing_inputs(inputs, input_tags) if status == 'NOT_COMPUTABLE' else []
        c['status'] = status
        c['reason'] = reason_code
        c['missing_inputs'] = missing_inputs
        c['ratio_type'] = ratio_type
        c['formula_used'] = c.get('formula_used') or self.FORMULA_MAP.get(ratio_id, ratio_id)
        c['input_concepts'] = input_tags
        c['raw_values_used'] = inputs
        c['period'] = self._extract_period(c)
        c['computation_timestamp'] = self.computation_timestamp
        c['reason_code'] = reason_code
        return c

    @staticmethod
    def _contract(value: Optional[float], reliability: int, reason: Optional[str], ratio_id: str, meta: Dict) -> Dict:
        return {
            'value': value,
            'reliability': int(reliability),
            'reason': reason,
            'source': 'ratio_engine',
            'ratio_id': ratio_id,
            'ratio_format': meta.get('ratio_format'),
            'ratio_unit': meta.get('ratio_unit'),
            'inputs': {},
            'input_tags': [],
            'formula_used': None,
            'period_end': None,
            'bounds_result': {'status': 'unknown', 'ratio_id': ratio_id},
        }

    @staticmethod
    def _is_finite(v: float) -> bool:
        return v == v and abs(v) != float('inf')

    @staticmethod
    def _is_value(contract: Optional[Dict]) -> bool:
        return bool(contract) and isinstance(contract.get('value'), (int, float))

    @staticmethod
    def _num(v) -> Optional[float]:
        return float(v) if isinstance(v, (int, float)) else None

    def _pick(self, raw: Dict, tags) -> Optional[float]:
        for t in tags:
            v = self._num(raw.get(t))
            if v is not None:
                return v
        return None

    def _pick_tag(self, raw: Dict, tags) -> Optional[str]:
        for t in tags:
            if self._num(raw.get(t)) is not None:
                return t
        return None

    @staticmethod
    def _num_or_zero(contract: Optional[Dict]) -> float:
        if not contract:
            return 0.0
        v = contract.get('value')
        return float(v) if isinstance(v, (int, float)) else 0.0

    @staticmethod
    def _resolve_sector_profile_from_row(raw: Dict) -> str:
        row = raw or {}
        for k in ('__sector_profile__', 'sector_profile', 'sector'):
            v = row.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().lower()
        return 'unknown'

    @staticmethod
    def _reject(contract: Dict, reason: str) -> None:
        contract['value'] = None
        contract['reliability'] = 0
        contract['reason'] = reason

    @staticmethod
    def _write_canonical_diagnostics(payload: Dict[str, Dict]) -> None:
        out = Path('exports/sector_comparison')
        out.mkdir(parents=True, exist_ok=True)
        (out / 'canonical_item_selection_diagnostics.json').write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )

    @staticmethod
    def _write_data_integrity_diagnostics(payload: Dict[str, Dict]) -> None:
        out = Path('exports/sector_comparison')
        out.mkdir(parents=True, exist_ok=True)
        (out / 'data_integrity_diagnostics.json').write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )

    @staticmethod
    def _write_ratio_reliability_report(payload: List[Dict]) -> None:
        out = Path('exports/sector_comparison')
        out.mkdir(parents=True, exist_ok=True)
        (out / 'ratio_reliability_report.json').write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )

    @staticmethod
    def _write_ratio_results_with_explanations(payload: Dict[int, Dict[str, Dict]]) -> None:
        out = Path('exports/sector_comparison')
        out.mkdir(parents=True, exist_ok=True)
        body = {
            'generated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'results_by_year': payload,
        }
        (out / 'ratio_results_with_explanations.json').write_text(
            json.dumps(body, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )

    @staticmethod
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

    @staticmethod
    def _item_diag(item: Dict) -> Dict:
        return {
            'tag': item.get('tag'),
            'value': item.get('value'),
            'period_type': item.get('period_type'),
            'period_end': item.get('period_end'),
            'unit': item.get('unit'),
            'original_unit': item.get('original_unit'),
            'scale_applied': item.get('scale_applied'),
            'currency': item.get('currency'),
            'selection_reason': item.get('selection_reason'),
            'top_rejected_candidates': [
                {'tag': c.get('tag'), 'reason_rejected': c.get('reason_rejected')}
                for c in (item.get('candidates') or [])
                if c.get('reason_rejected')
            ][:3],
        }

    def _build_integrity_row(self, year: int, rev: Dict, cogs: Dict, equity: Dict, ar: Dict, ap: Dict, inv: Dict) -> Dict:
        return {
            'year': year,
            'fiscal_end_date_selected': rev.get('period_end') or cogs.get('period_end'),
            'facts_used': {
                'revenue': self._item_diag(rev),
                'cogs': self._item_diag(cogs),
                'equity': self._item_diag(equity),
                'accounts_receivable': self._item_diag(ar),
                'accounts_payable': self._item_diag(ap),
                'inventory': self._item_diag(inv),
            },
            'chosen_concepts': {
                'revenue': rev.get('tag'),
                'cogs': cogs.get('tag'),
                'equity': equity.get('tag'),
                'accounts_receivable': ar.get('tag'),
                'accounts_payable': ap.get('tag'),
                'inventory': inv.get('tag'),
            },
            'rejected_concepts': {
                'revenue': [c for c in (rev.get('candidates') or []) if c.get('reason_rejected')],
                'cogs': [c for c in (cogs.get('candidates') or []) if c.get('reason_rejected')],
                'equity': [c for c in (equity.get('candidates') or []) if c.get('reason_rejected')],
                'accounts_receivable': [c for c in (ar.get('candidates') or []) if c.get('reason_rejected')],
                'accounts_payable': [c for c in (ap.get('candidates') or []) if c.get('reason_rejected')],
                'inventory': [c for c in (inv.get('candidates') or []) if c.get('reason_rejected')],
            },
        }
