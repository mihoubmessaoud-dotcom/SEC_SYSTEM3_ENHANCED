# -*- coding: utf-8 -*-
"""
modules/sec_fetcher.py
SEC API Module - Ø¬Ø§Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª SEC Ù…Ø­Ø³Ù‘Ù†
- ÙŠØ¬Ù„Ø¨ companyfacts Ù…Ù† SEC
- ÙŠÙƒØªØ´Ù ØªÙ„Ù‚Ø§Ø¦ÙŠØ§Ù‹ (dynamic mapping) Ù…ÙØ§Ù‡ÙŠÙ… XBRL Ø§Ù„Ø´Ø§Ø¦Ø¹Ø© ÙˆÙŠØ·Ø§Ø¨Ù‚Ù‡Ø§ Ø¨Ø§Ù„Ù…Ù‚Ø§ÙŠÙŠØ³ Ø§Ù„Ù…Ø·Ù„ÙˆØ¨Ø©
- ÙŠØ­Ø³Ø¨ Ø§Ù„Ù†Ø³Ø¨ Ø§Ù„Ù…ÙˆØ³Ø¹Ø© (Ø¨Ù…Ø§ ÙÙŠ Ø°Ù„Ùƒ retention_ratio Ùˆ sgr_internal Ùˆ altman)
- ÙŠØ¯Ø¹Ù… Ø¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ Ø¹Ø¨Ø± yfinance (Ø§Ø®ØªÙŠØ§Ø±ÙŠ)
"""
import requests
import traceback
import copy
from .ratio_formats import canonicalize_ratio_value, format_ratio_value, get_ratio_metadata
import time
import re
import math
import csv
import os
from datetime import datetime
import sys
import json
import xml.etree.ElementTree as ET
from pathlib import Path
import io
import zipfile
from core.audit_pack import build_institutional_audit_pack, write_audit_pack_to_outputs
from core.classification_debug import write_classification_debug
from .canonical_classification import (
    build_canonical_classification,
    canonical_sector_gating_from_classification,
)
from .xbrl_statement_tree import (
    parse_presentation_linkbase,
    parse_calculation_linkbase,
    parse_instance_xbrl,
    pick_primary_role,
    select_income_statement_role,
    find_statement_anchor_context,
)
from .direct_extraction_engine import DirectExtractionEngine
try:
    from config.layer_registry import build_layer_registry
except Exception:
    build_layer_registry = None
try:
    from core.ratio_engine import RatioEngine as CoreRatioEngine
    from core.strategy_engine import StrategicAnalysisEngine as CoreStrategyEngine
except Exception:
    CoreRatioEngine = None
    CoreStrategyEngine = None

# yfinance Ø§Ø®ØªÙŠØ§Ø±ÙŠØ› Ø¥Ø°Ø§ Ù„Ù… ÙŠÙƒÙ† Ù…Ø«Ø¨ØªÙ‹Ø§ Ù†ØªØ¬Ø§Ù‡Ù„ Ø¬Ù„Ø¨ Ø§Ù„Ø³ÙˆÙ‚ Ø§Ù„Ø¢Ù„ÙŠ
try:
    import yfinance as yf
except Exception:
    yf = None


from .sec_auto_learner import SECAutoLearner
from .institutional import InstitutionalFinancialIntelligenceEngine, EngineConfig
try:
    from .financial_analysis_system import FinancialAnalysisSystem
except Exception:
    FinancialAnalysisSystem = None

class SECDataFetcher:
    # Canonical label priorities requested by user:
    # prefer these labels first and avoid semantic drift from lower-quality alternatives.
    CANONICAL_LABELS = {
        'Revenue': ['Revenues', 'Revenue', 'Net sales'],
        'COGS': ['CostOfRevenue', 'Cost of Revenue', 'COGS'],
        'NetIncome': ['NetIncomeLoss', 'NetIncome', 'Net income'],
        'Equity': ['StockholdersEquity', 'Total Equity'],
        'Assets': ['Assets', 'TotalAssets'],
        'InterestExpense': ['InterestExpense', 'Interest Expense', 'InterestExpense_Hierarchy'],
        'CapEx': ['PaymentsToAcquirePropertyPlantAndEquipment'],
    }

    # Controlled fallbacks only when canonical labels are absent.
    # We keep these explicit so the pipeline stays auditable.
    CANONICAL_LABEL_FALLBACKS = {
        'Revenue': ['SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax'],
        'COGS': ['CostOfGoodsAndServicesSold', 'CostOfSales', 'CostsAndExpenses'],
        'NetIncome': ['ProfitLoss'],
        'Equity': ['StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
        'Assets': ['AssetsNet', 'Total Assets'],
        'InterestExpense': ['InterestCostsIncurred', 'InterestPaidNet'],
        'CapEx': ['CapitalExpenditures'],
    }

    CONTROL_TOTALS_NOT_EQUITY = {
        'LiabilitiesAndStockholdersEquity',
        'Total liabilities and stockholders equity',
        'Total liabilities and shareholders equity',
    }

    VALIDATION_RULES = {
        'roe': {'min': -0.5, 'max': 5.0, 'action': 'HALT'},
        'gross_margin': {'min': -0.5, 'max': 1.0, 'action': 'HALT'},
        'net_margin': {'min': -0.5, 'max': 1.0, 'action': 'HALT'},
        'asset_turnover': {'min': 0.01, 'max': 10.0, 'action': 'HALT'},
        'debt_to_equity': {'min': -50.0, 'max': 100.0, 'action': 'FLAG'},
        'debt_to_assets': {'min': -5.0, 'max': 5.0, 'action': 'HALT'},
        'interest_coverage': {'min': -100.0, 'max': 500.0, 'action': 'FLAG'},
        'net_debt_ebitda': {'min': -100.0, 'max': 500.0, 'action': 'FLAG'},
    }

    MARKET_MILLION_KEYS = {
        'market_cap',
        'enterprise_value',
        'total_debt',
    }

    SECTOR_RATIO_BLOCKLIST = {
        # Banking statements are structurally different; these ratios are not
        # reliable under generic industrial formulas.
        'bank': {
            # Working-capital and industrial production metrics are not primary
            # decision metrics for banks.
            'inventory_turnover', 'inventory_days', 'days_sales_outstanding', 'payables_turnover', 'ap_days',
            'current_ratio', 'quick_ratio', 'cash_ratio',
            'gross_margin', 'operating_margin', 'ebitda_margin', 'asset_turnover',
            'ocf_margin', 'operating_cash_flow_margin',
            'free_cash_flow', 'fcf_per_share', 'fcf_yield',
            'interest_coverage',
            'altman_z_score', 'ev_to_ebitda', 'ev_ebitda',
        },
        'insurance': {
            'inventory_turnover', 'inventory_days', 'days_sales_outstanding', 'payables_turnover', 'ap_days',
            'current_ratio', 'quick_ratio', 'cash_ratio', 'interest_coverage', 'operating_margin',
            'gross_margin', 'ebitda_margin', 'asset_turnover', 'ocf_margin', 'operating_cash_flow_margin',
            'altman_z_score', 'bank_total_revenue', 'bank_efficiency_ratio',
        },
        'industrial': {
            # Hide financial-institution-specific proxy ratios for industrial issuers.
            'net_interest_margin', 'loan_to_deposit_ratio', 'capital_ratio_proxy',
            'bank_total_revenue',
            'bank_efficiency_ratio',
            'combined_proxy', 'capital_adequacy_proxy', 'net_income_to_assets', 'equity_ratio',
        },
        'technology': {
            # Technology uses the same ratio policy pack as industrial corporates.
            'net_interest_margin', 'loan_to_deposit_ratio', 'capital_ratio_proxy',
            'bank_total_revenue',
            'bank_efficiency_ratio',
            'combined_proxy', 'capital_adequacy_proxy', 'net_income_to_assets', 'equity_ratio',
        },
        'unknown': set(),
    }

    SUB_SECTOR_TO_PRIMARY = {
        'software_saas': 'technology',
        'hardware_platform': 'technology',
        'semiconductor_fabless': 'technology',
        'commercial_bank': 'bank',
        'investment_bank': 'bank',
        'insurance_life': 'insurance',
        'insurance_pc': 'insurance',
        'insurance_broker': 'insurance',
        'integrated_oil': 'industrial',
        'consumer_staples': 'industrial',
        'ev_automaker': 'industrial',
    }

    SUB_SECTOR_RATIO_BLOCKLIST = {
        'commercial_bank': set(SECTOR_RATIO_BLOCKLIST['bank']),
        'investment_bank': set(SECTOR_RATIO_BLOCKLIST['bank']) | {
            'loan_to_deposit_ratio',
            'net_interest_margin',
        },
        'insurance_life': (set(SECTOR_RATIO_BLOCKLIST['insurance']) - {'fcf_yield'}) | {
            'combined_proxy',
        },
        'insurance_pc': set(SECTOR_RATIO_BLOCKLIST['insurance']) - {'fcf_yield'},
        'insurance_broker': set(SECTOR_RATIO_BLOCKLIST['industrial']) | {
            'combined_proxy', 'capital_adequacy_proxy', 'net_income_to_assets', 'equity_ratio',
            'net_interest_margin', 'loan_to_deposit_ratio', 'capital_ratio_proxy',
        },
        'software_saas': set(SECTOR_RATIO_BLOCKLIST['technology']),
        'hardware_platform': set(SECTOR_RATIO_BLOCKLIST['technology']),
        'semiconductor_fabless': set(SECTOR_RATIO_BLOCKLIST['technology']),
        'integrated_oil': set(SECTOR_RATIO_BLOCKLIST['industrial']),
        'consumer_staples': set(SECTOR_RATIO_BLOCKLIST['industrial']),
        'ev_automaker': set(SECTOR_RATIO_BLOCKLIST['industrial']),
    }

    SECTOR_STRATEGIC_BLOCKLIST = {
        'bank': {
            'CCC_Days', 'Inventory_Days', 'AR_Days', 'AP_Days', 'Op_Leverage',
            'Gross_Margin', 'Operating_Margin',
            'Current_Ratio', 'Quick_Ratio', 'Cash_Ratio', 'OCF_Margin',
            'FCF', 'FCF_Yield', 'FCF_per_Share',
            'Altman_Z_Score', 'Accruals_Ratio', 'Accruals_Change',
            'Combined_Ratio_Proxy', 'Capital_Adequacy_Proxy',
            'ROIC', 'Economic_Spread',
        },
        'insurance': {
            'CCC_Days', 'Inventory_Days', 'AR_Days', 'AP_Days',
            'Gross_Margin', 'Operating_Margin',
            'Current_Ratio', 'Quick_Ratio', 'Cash_Ratio', 'OCF_Margin',
            'FCF', 'FCF_Yield', 'FCF_per_Share',
            'Altman_Z_Score', 'Accruals_Ratio', 'Accruals_Change',
            'Net_Interest_Margin', 'Loan_to_Deposit_Ratio', 'Capital_Ratio_Proxy',
            'ROIC', 'Economic_Spread',
        },
        'industrial': {
            # Hide financial-sector-only strategic fields for corporates.
            'Net_Interest_Margin', 'Loan_to_Deposit_Ratio', 'Capital_Ratio_Proxy',
            'Bank_Total_Revenue',
            'Bank_Efficiency_Ratio',
            'Combined_Ratio_Proxy', 'Capital_Adequacy_Proxy',
            'Net_Income_to_Assets', 'Equity_Ratio',
        },
        'technology': {
            # Technology uses the same strategic policy pack as industrial corporates.
            'Net_Interest_Margin', 'Loan_to_Deposit_Ratio', 'Capital_Ratio_Proxy',
            'Bank_Total_Revenue',
            'Bank_Efficiency_Ratio',
            'Combined_Ratio_Proxy', 'Capital_Adequacy_Proxy',
            'Net_Income_to_Assets', 'Equity_Ratio',
        },
        'unknown': set(),
    }

    SUB_SECTOR_STRATEGIC_BLOCKLIST = {
        'commercial_bank': set(SECTOR_STRATEGIC_BLOCKLIST['bank']),
        'investment_bank': set(SECTOR_STRATEGIC_BLOCKLIST['bank']) | {
            'Loan_to_Deposit_Ratio',
            'Net_Interest_Margin',
        },
        'insurance_life': (set(SECTOR_STRATEGIC_BLOCKLIST['insurance']) - {'FCF', 'FCF_Yield', 'FCF_per_Share'}) | {
            'Combined_Ratio_Proxy',
        },
        'insurance_pc': set(SECTOR_STRATEGIC_BLOCKLIST['insurance']) - {'FCF', 'FCF_Yield', 'FCF_per_Share'},
        'insurance_broker': set(SECTOR_STRATEGIC_BLOCKLIST['industrial']) | {
            'Combined_Ratio_Proxy', 'Capital_Adequacy_Proxy', 'Net_Income_to_Assets', 'Equity_Ratio',
            'Net_Interest_Margin', 'Loan_to_Deposit_Ratio', 'Capital_Ratio_Proxy',
        },
        'software_saas': set(SECTOR_STRATEGIC_BLOCKLIST['technology']),
        'hardware_platform': set(SECTOR_STRATEGIC_BLOCKLIST['technology']),
        'semiconductor_fabless': set(SECTOR_STRATEGIC_BLOCKLIST['technology']),
        'integrated_oil': set(SECTOR_STRATEGIC_BLOCKLIST['industrial']),
        'consumer_staples': set(SECTOR_STRATEGIC_BLOCKLIST['industrial']),
        'ev_automaker': set(SECTOR_STRATEGIC_BLOCKLIST['industrial']),
    }

    MODERN_METRIC_ALIASES = {
        'CostOfRevenue': 'COGS',
        'GrossProfit': 'GrossProfit',
        'OperatingIncomeLoss': 'OperatingIncome',
        'NetIncomeLoss': 'NetIncome',
        'Assets': 'TotalAssets',
        'Liabilities': 'TotalLiabilities',
        'StockholdersEquity': 'TotalEquity',
        'AssetsCurrent': 'CurrentAssets',
        'LiabilitiesCurrent': 'CurrentLiabilities',
        'CashAndCashEquivalentsAtCarryingValue': 'CashAndCashEquivalents',
        'AccountsReceivableNetCurrent': 'AccountsReceivable',
        'InventoryNet': 'Inventory',
        'AccountsPayableCurrent': 'AccountsPayable',
        'NetCashProvidedByUsedInOperatingActivities': 'OperatingCashFlow',
        'NetCashProvidedByUsedInInvestingActivities': 'InvestingCashFlow',
        'NetCashProvidedByUsedInFinancingActivities': 'FinancingCashFlow',
        'PaymentsToAcquirePropertyPlantAndEquipment': 'CapitalExpenditures',
        'RetainedEarningsAccumulatedDeficit': 'RetainedEarnings',
        'WeightedAverageNumberOfSharesOutstandingBasic': 'SharesBasic',
        'DepreciationDepletionAndAmortization': 'DepreciationAmortization',
    }

    GOLDEN_TAG_CLASSIFICATION = {
        'Assets': ('Total Assets', 'Balance Sheet'),
        'AssetsNet': ('Total Assets', 'Balance Sheet'),
        'AssetsCurrent': ('Current Assets', 'Balance Sheet'),
        'AssetsNoncurrent': ('Non-current Assets', 'Balance Sheet'),
        'Liabilities': ('Total Liabilities', 'Balance Sheet'),
        'LiabilitiesCurrent': ('Current Liabilities', 'Balance Sheet'),
        'LiabilitiesNoncurrent': ('Non-current Liabilities', 'Balance Sheet'),
        'StockholdersEquity': ('Total Equity', 'Balance Sheet'),
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest': ('Total Equity', 'Balance Sheet'),
        'LiabilitiesAndStockholdersEquity': ('Total Assets', 'Balance Sheet'),
        'Revenues': ('Revenues', 'Income Statement'),
        'RevenueFromContractWithCustomerExcludingAssessedTax': ('Revenues', 'Income Statement'),
        'SalesRevenueNet': ('Revenues', 'Income Statement'),
        'CostOfRevenue': ('Cost of Revenue', 'Income Statement'),
        'GrossProfit': ('Gross Profit', 'Income Statement'),
        'OperatingIncomeLoss': ('Operating Income', 'Income Statement'),
        'NetIncomeLoss': ('Net Income', 'Income Statement'),
        'NetCashProvidedByUsedInOperatingActivities': ('Operating Cash Flow', 'Cash Flow'),
        'NetCashProvidedByUsedInInvestingActivities': ('Investing Cash Flow', 'Cash Flow'),
        'NetCashProvidedByUsedInFinancingActivities': ('Financing Cash Flow', 'Cash Flow'),
        'CashAndCashEquivalentsAtCarryingValue': ('Cash and Cash Equivalents', 'Balance Sheet'),
        'AccountsReceivableNetCurrent': ('Accounts Receivable', 'Balance Sheet'),
        'InventoryNet': ('Inventory', 'Balance Sheet'),
        'AccountsPayableCurrent': ('Accounts Payable', 'Balance Sheet'),
    }

    @staticmethod
    def _truthy_env(name: str) -> bool:
        v = str(os.environ.get(name, '')).strip().lower()
        return v in ('1', 'true', 'yes', 'on')

    def _sanitize_proxy_environment(self):
        """
        Protect SEC connectivity from broken system proxy settings.
        If proxy points to loopback discard/port (e.g. 127.0.0.1:9), force direct mode.
        """
        if self._truthy_env('SEC_ALLOW_SYSTEM_PROXY'):
            return

        proxy_keys = ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy')
        bad_markers = ('127.0.0.1:9', 'localhost:9')
        current = {k: str(os.environ.get(k, '')).strip() for k in proxy_keys}
        has_broken_proxy = any(v and any(m in v.lower() for m in bad_markers) for v in current.values())
        if not has_broken_proxy:
            return

        disabled = {}
        for k in proxy_keys:
            if os.environ.get(k):
                disabled[k] = os.environ.get(k)
                os.environ.pop(k, None)

        no_proxy_tokens = set(
            t.strip() for t in str(os.environ.get('NO_PROXY', '')).split(',') if str(t).strip()
        )
        no_proxy_tokens.update({
            'localhost',
            '127.0.0.1',
            '::1',
            '.sec.gov',
            'sec.gov',
            'www.sec.gov',
            'data.sec.gov',
        })
        os.environ['NO_PROXY'] = ','.join(sorted(no_proxy_tokens))
        os.environ['no_proxy'] = os.environ['NO_PROXY']
        self._disabled_proxy_env = disabled
        print("🌐 Proxy safeguard: disabled broken loopback proxy for direct SEC access.")

    def __init__(self, user_agent_email='mihoubmessaoud@yahoo.fr'):
        try:
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(encoding='utf-8', errors='replace')
            if hasattr(sys.stderr, 'reconfigure'):
                sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass

        self.base_url = "https://data.sec.gov"
        self.headers = {
            'User-Agent': f'Financial-Analysis-System/1.0 ({user_agent_email})',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': 'application/json'
        }
        self.companies_cache = {}
        self.latest_dynamic_map = {}  # updated per-fetch
        self._companyconcept_cache = {}
        self._companyconcept_entries_cache = {}
        self._active_cik_padded = None
        self._active_ticker = None
        self._active_start_year = None
        self._active_end_year = None
        self._fetch_request_cache = {}
        self._fetch_request_cache_path = Path('outputs') / 'fetch_request_cache.json'
        self._submissions_cache = {}
        self._submissions_cache_path = Path('outputs') / 'submissions_cache.json'
        self._disabled_proxy_env = {}
        self.institutional_engine = None
        self.direct_engine = None
        self._sector_memory_path = Path('exports') / 'sector_profile_memory.json'
        self._sector_profile_memory = {}
        self._sanitize_proxy_environment()
        self._init_yfinance_cache()
        
        # âœ… NEW: Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¹Ù„Ù… Ø§Ù„ØªÙ„Ù‚Ø§Ø¦ÙŠ
        try:
            self.auto_learner = SECAutoLearner()
            print("ðŸ¤– Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¹Ù„Ù… Ø§Ù„ØªÙ„Ù‚Ø§Ø¦ÙŠ Ø¬Ø§Ù‡Ø²")
            stats = self.auto_learner.get_statistics()
            if stats['total_sec_mappings'] > 0:
                print(f"   ðŸ“š ØªÙ… ØªØ­Ù…ÙŠÙ„ {stats['total_sec_mappings']} ØªØ¹ÙŠÙŠÙ† Ù…ØªØ¹Ù„Ù…")
        except Exception as e:
            print(f"âš ï¸ ØªØ¹Ø°Ø± ØªÙ‡ÙŠØ¦Ø© Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¹Ù„Ù…: {e}")
            self.auto_learner = None

        # Institutional-grade intelligence engine
        try:
            self.institutional_engine = InstitutionalFinancialIntelligenceEngine(
                EngineConfig(tolerance=0.05, output_dir='exports/institutional')
            )
            print("âœ… Institutional Financial Intelligence Engine initialized")
        except Exception as e:
            print(f"âš ï¸ ØªØ¹Ø°Ø± ØªÙ‡ÙŠØ¦Ø© Ø§Ù„Ù…Ø­Ø±Ùƒ Ø§Ù„Ù…Ø¤Ø³Ø³ÙŠ: {e}")
            self.institutional_engine = None

        # Direct extraction engine (single source: SEC consolidated statements)
        try:
            self.direct_engine = DirectExtractionEngine(
                user_agent=f"Financial-Analysis-System/1.0 ({user_agent_email})"
            )
        except Exception:
            self.direct_engine = None
        try:
            self.financial_analysis_system = FinancialAnalysisSystem() if FinancialAnalysisSystem is not None else None
        except Exception:
            self.financial_analysis_system = None
        self._load_sector_profile_memory()
        self._load_fetch_request_cache()
        self._load_submissions_cache()
        
        self._load_companies()

    def _make_fetch_cache_key(self, ticker: str, start_year: int, end_year: int, filing_type: str) -> str:
        t = str(ticker or '').upper().strip()
        return f"{t}|{int(start_year)}|{int(end_year)}|{str(filing_type or '').upper().strip()}"

    def _load_fetch_request_cache(self):
        try:
            p = self._fetch_request_cache_path
            if p.exists():
                payload = json.loads(p.read_text(encoding='utf-8'))
                if isinstance(payload, dict):
                    self._fetch_request_cache = payload
        except Exception:
            self._fetch_request_cache = {}

    def _save_fetch_request_cache(self):
        try:
            p = self._fetch_request_cache_path
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(
                json.dumps(self._fetch_request_cache, ensure_ascii=False),
                encoding='utf-8'
            )
        except Exception:
            pass

    def _load_submissions_cache(self):
        try:
            p = self._submissions_cache_path
            if p.exists():
                payload = json.loads(p.read_text(encoding='utf-8'))
                if isinstance(payload, dict):
                    self._submissions_cache = payload
        except Exception:
            self._submissions_cache = {}

    def _save_submissions_cache(self):
        try:
            p = self._submissions_cache_path
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(
                json.dumps(self._submissions_cache, ensure_ascii=False),
                encoding='utf-8'
            )
        except Exception:
            pass

    @staticmethod
    def _normalize_year_keyed_dict(payload):
        if not isinstance(payload, dict):
            return payload
        out = {}
        for k, v in payload.items():
            nk = k
            try:
                nk = int(k)
            except Exception:
                nk = k
            out[nk] = v
        return out

    def _normalize_cached_fetch_result(self, result):
        if not isinstance(result, dict):
            return result
        normalized = copy.deepcopy(result)
        for key in ('data_by_year', 'financial_ratios', 'strategic_analysis', 'core_ratio_results', 'core_strategy_results'):
            normalized[key] = self._normalize_year_keyed_dict(normalized.get(key) or {})

        dl = normalized.get('data_layers') or {}
        if isinstance(dl, dict):
            for lk in ('layer1_by_year', 'layer2_by_year', 'layer3_by_year', 'layer4_by_year'):
                dl[lk] = self._normalize_year_keyed_dict(dl.get(lk) or {})
            # Shares unit harmonization must apply on cache hits too (Layer2/Layer4 are frequently cached).
            try:
                l2_fixed, l2_diag = self._harmonize_layer_shares_units_to_million(
                    dl.get('layer2_by_year') or {},
                    layer_name="LAYER2_MARKET_CACHE",
                )
                dl['layer2_by_year'] = l2_fixed
                if l2_diag:
                    dl.setdefault('layer2_diagnostics', {})['shares_unit_harmonization'] = l2_diag
            except Exception:
                pass
            try:
                l4_fixed, l4_diag = self._harmonize_layer_shares_units_to_million(
                    dl.get('layer4_by_year') or {},
                    layer_name="LAYER4_YAHOO_CACHE",
                )
                dl['layer4_by_year'] = l4_fixed
                if l4_diag:
                    dl.setdefault('layer4_diagnostics', {})['shares_unit_harmonization'] = l4_diag
            except Exception:
                pass
            # Cross-source reconciliation must apply on cache hits too to prevent extreme
            # Polygon/Yahoo divergences from leaking into visible Layer2_Market outputs.
            try:
                l2_rec, rec_diag = self._reconcile_market_layer_against_yahoo(
                    dl.get('layer2_by_year') or {},
                    dl.get('layer4_by_year') or {},
                )
                dl['layer2_by_year'] = l2_rec
                if rec_diag:
                    dl.setdefault('layer2_diagnostics', {})['market_vs_yahoo_reconciliation'] = rec_diag
            except Exception:
                pass

            # Money and balance-sheet reconciliation must also apply on cache hits; otherwise
            # old cached runs can keep 1000x-scale balance anchors (e.g., bank Assets) and
            # leak absurd ROA/ROE values into visible outputs.
            l1 = dl.get('layer1_by_year') or {}
            l1_changed = False
            try:
                l1_fixed, money_diag = self._harmonize_layer1_money_units_to_million(l1 or {})
                if money_diag:
                    dl.setdefault('unit_harmonization', {})['money_usd_million_cache'] = money_diag
                if l1_fixed is not None:
                    l1 = l1_fixed
            except Exception:
                pass
            try:
                l1_fixed, bs_diag = self._reconcile_balance_sheet_totals(l1 or {})
                if bs_diag:
                    dl.setdefault('unit_harmonization', {})['balance_sheet_totals_cache'] = bs_diag
                if l1_fixed is not None:
                    l1 = l1_fixed
            except Exception:
                bs_diag = {}
            try:
                l1_fixed, series_diag = self._harmonize_layer1_series_scale_to_million(l1 or {})
                if series_diag:
                    dl.setdefault('unit_harmonization', {})['series_scale_to_million_cache'] = series_diag
                if l1_fixed is not None:
                    l1 = l1_fixed
            except Exception:
                series_diag = {}
            # Detect changes by presence of diagnostics (cheap, deterministic).
            if (
                (dl.get('unit_harmonization') or {}).get('money_usd_million_cache')
                or (dl.get('unit_harmonization') or {}).get('balance_sheet_totals_cache')
                or (dl.get('unit_harmonization') or {}).get('series_scale_to_million_cache')
            ):
                l1_changed = True
            if l1_changed:
                dl['layer1_by_year'] = l1
                normalized['data_by_year'] = l1
                # Recompute visible ratios/strategic on cache hits if anchors were corrected.
                try:
                    normalized['financial_ratios'] = self._calculate_financial_ratios(l1 or {})
                except Exception:
                    pass
                try:
                    normalized['strategic_analysis'] = self.generate_strategic_analysis(
                        l1 or {},
                        normalized.get('financial_ratios') or {},
                    )
                except Exception:
                    pass

            extra = dl.get('extra_layers_by_year') or {}
            if isinstance(extra, dict):
                fixed_extra = {}
                for exk, exv in extra.items():
                    fixed_extra[exk] = self._normalize_year_keyed_dict(exv or {})
                dl['extra_layers_by_year'] = fixed_extra
            normalized['data_layers'] = dl

        # Ensure phase-1 audit pack is available even for cache hits.
        if not normalized.get('audit_pack_path'):
            try:
                audit_pack = build_institutional_audit_pack(
                    ticker=(normalized.get('company_info') or {}).get('ticker') or normalized.get('ticker') or 'UNKNOWN',
                    period=str(normalized.get('period') or ''),
                    data_by_year=normalized.get('data_by_year') or {},
                    financial_ratios=normalized.get('financial_ratios') or {},
                    canonical_money_unit="usd_million",
                    canonical_shares_unit="shares_million",
                )
                audit_pack_path = write_audit_pack_to_outputs(audit_pack=audit_pack, outputs_dir="outputs")
                normalized['audit_pack'] = audit_pack
                normalized['audit_pack_path'] = audit_pack_path
            except Exception:
                pass

        # Canonical classification SSOT for cache hits (avoid mixed/legacy sector labels).
        try:
            if not isinstance(normalized.get('canonical_classification'), dict) or not normalized.get('canonical_classification'):
                ci = normalized.get('company_info', {}) or {}
                sg = normalized.get('sector_gating', {}) or {}
                canonical_cls = build_canonical_classification(
                    ticker=ci.get('ticker') or normalized.get('ticker') or 'UNKNOWN',
                    company_name=ci.get('name') or '',
                    sic=ci.get('sic') or '',
                    naics='',
                    sic_description=ci.get('sic_description') or '',
                    sector_profile_hint=sg.get('profile') or ci.get('sector') or '',
                    sub_sector_profile_hint=sg.get('sub_profile') or '',
                    institutional_primary_profile='',
                    institutional_diag={},
                ).to_dict()
                canonical_sg = canonical_sector_gating_from_classification(canonical_cls)
                normalized['canonical_classification'] = canonical_cls
                normalized['sector_gating'] = {
                    **(normalized.get('sector_gating', {}) or {}),
                    **canonical_sg,
                }
                dbg_path = write_classification_debug(
                    canonical_classification=canonical_cls,
                    outputs_dir="outputs",
                    ticker=ci.get('ticker') or normalized.get('ticker') or '',
                )
                if dbg_path and not normalized.get('canonical_classification_debug_path'):
                    normalized['canonical_classification_debug_path'] = dbg_path
        except Exception:
            pass
        return normalized

    @staticmethod
    def _fas_to_num(value):
        try:
            if value is None:
                return None
            if isinstance(value, str):
                vv = value.strip().replace(',', '')
                if vv == '' or vv.lower() in {'nan', 'na', 'n/a', 'none', '--'}:
                    return None
                return float(vv)
            return float(value)
        except Exception:
            return None

    def _fas_pick(self, row: dict, keys: list):
        if not isinstance(row, dict):
            return None
        for k in keys:
            if k in row:
                v = self._fas_to_num(row.get(k))
                if v is not None:
                    return v
        return None

    def _harmonize_layer_shares_units_to_million(self, layer_by_year: dict, *, layer_name: str = "LAYER") -> tuple[dict, dict]:
        """
        Harmonize shares-outstanding style fields to canonical shares_million units.

        This fixes mixed-unit market/Yahoo payloads where:
        - Some years store raw shares (e.g., 2.43e10)
        - Other years store shares_million (e.g., 24661)

        Uses anchors when available:
          market_cap (usd_million) ≈ price (usd) * shares (shares_million)
        """
        if not isinstance(layer_by_year, dict) or not layer_by_year:
            return layer_by_year or {}, {}

        out = {}
        diag = {}

        def _num(x):
            try:
                if x is None:
                    return None
                if isinstance(x, str):
                    s = x.strip().replace(",", "")
                    if not s or s.lower() in {"nan", "none", "null", "n/a", "na", "--"}:
                        return None
                    return float(s)
                return float(x)
            except Exception:
                return None

        shares_keys = ("market:shares_outstanding", "yahoo:shares_outstanding")
        price_keys = ("market:price", "yahoo:price")
        mcap_keys = ("market:market_cap", "yahoo:market_cap")

        for y, row in (layer_by_year or {}).items():
            try:
                yy = int(y)
            except Exception:
                yy = y
            r = dict(row or {}) if isinstance(row, dict) else {}
            yr_diag = {"scaled_keys": {}, "rule": None}

            px = None
            for k in price_keys:
                px = _num(r.get(k))
                if px not in (None, 0):
                    break
            mc = None
            for k in mcap_keys:
                mc = _num(r.get(k))
                if mc not in (None, 0):
                    break

            for sk in shares_keys:
                sh = _num(r.get(sk))
                if sh in (None, 0):
                    continue

                # Candidate A: assume already shares_million.
                sh_m = float(sh)
                # Candidate B: assume raw shares, convert to shares_million.
                sh_from_raw_m = float(sh) / 1_000_000.0

                scale = 1.0
                rule = None
                if mc not in (None, 0) and px not in (None, 0):
                    implied_a = sh_m * float(px)  # million USD
                    implied_b = sh_from_raw_m * float(px)  # million USD
                    err_a = abs(implied_a - float(mc)) / max(abs(float(mc)), 1.0)
                    err_b = abs(implied_b - float(mc)) / max(abs(float(mc)), 1.0)
                    if err_b + 1e-12 < err_a:
                        scale = 1.0 / 1_000_000.0
                        rule = "ANCHOR_MATCH_MC_PRICE"
                else:
                    # Heuristic: huge values are almost certainly raw shares.
                    if abs(float(sh)) >= 50_000_000.0:
                        scale = 1.0 / 1_000_000.0
                        rule = "HEURISTIC_BIG_RAW_SHARES"

                if scale != 1.0:
                    r[sk] = float(sh) * scale
                    yr_diag["scaled_keys"][sk] = scale
                    yr_diag["rule"] = rule or yr_diag["rule"]
                else:
                    # Ensure type is numeric float for Excel.
                    r[sk] = float(sh)

            out[yy] = r
            if yr_diag["scaled_keys"]:
                diag[str(yy)] = {"layer": layer_name, **yr_diag}

        return out, diag

    def _reconcile_market_layer_against_yahoo(self, market_by_year: dict, yahoo_by_year: dict):
        """
        Cross-source sanity check: reconcile obvious Polygon/Yahoo divergences for price/market_cap.

        This prevents nonsense like price=1463 when Yahoo has price=20 for the same year.
        Policy: only override when divergence is extreme (>=10x) and Yahoo value is present.
        """
        if not isinstance(market_by_year, dict) or not isinstance(yahoo_by_year, dict):
            return market_by_year or {}, {}

        def _num(x):
            try:
                if x is None:
                    return None
                if isinstance(x, str):
                    s = x.strip().replace(",", "")
                    if not s or s.lower() in {"nan", "none", "null", "na", "n/a", "--"}:
                        return None
                    return float(s)
                v = float(x)
                # Normalize NaN/Inf to None so downstream logic can heal gaps.
                if v != v or math.isinf(v):
                    return None
                return v
            except Exception:
                return None

        out = {}
        diag = {}
        for y, mrow in (market_by_year or {}).items():
            try:
                yy = int(y)
            except Exception:
                yy = y
            m = dict(mrow or {}) if isinstance(mrow, dict) else {}
            yrow = (yahoo_by_year or {}).get(yy) or (yahoo_by_year or {}).get(str(yy)) or {}
            yrow = dict(yrow or {}) if isinstance(yrow, dict) else {}

            px_m = _num(m.get("market:price") or m.get("yahoo:price"))
            px_y = _num(yrow.get("yahoo:price") or yrow.get("market:price"))
            mc_m = _num(m.get("market:market_cap") or m.get("yahoo:market_cap"))
            mc_y = _num(yrow.get("yahoo:market_cap") or yrow.get("market:market_cap"))
            sh_m = _num(m.get("market:shares_outstanding") or m.get("yahoo:shares_outstanding"))
            sh_y = _num(yrow.get("yahoo:shares_outstanding") or yrow.get("market:shares_outstanding"))

            def _ratio(a, b):
                if a in (None, 0) or b in (None, 0):
                    return None
                mx = max(abs(float(a)), abs(float(b)))
                mn = max(min(abs(float(a)), abs(float(b))), 1e-12)
                return mx / mn

            yr_diag = {}
            r_px = _ratio(px_m, px_y)
            r_mc = _ratio(mc_m, mc_y)
            if r_px is not None and r_px >= 10.0 and px_y not in (None, 0):
                # Override market price with Yahoo when divergence is extreme.
                yr_diag["price_override"] = {"from": px_m, "to": px_y, "ratio": r_px, "source": "YAHOO"}
                m["market:price"] = float(px_y)
                px_m = float(px_y)
            if r_mc is not None and r_mc >= 10.0 and mc_y not in (None, 0):
                yr_diag["market_cap_override"] = {"from": mc_m, "to": mc_y, "ratio": r_mc, "source": "YAHOO"}
                m["market:market_cap"] = float(mc_y)
                mc_m = float(mc_y)

            # Fill missing shares_outstanding when market cap + price exist.
            # Canonical units in this repo:
            # - market_cap: million USD
            # - price: USD
            # => shares_outstanding: million shares
            if sh_m in (None, 0):
                base_mc = mc_m if mc_m not in (None, 0) else mc_y
                base_px = px_m if px_m not in (None, 0) else px_y
                if base_mc not in (None, 0) and base_px not in (None, 0):
                    derived = float(base_mc) / float(base_px)
                    # Plausibility guardrail (million shares).
                    if 1e-6 < abs(derived) <= 10_000_000.0:
                        m["market:shares_outstanding"] = derived
                        yr_diag["shares_derived"] = {"value": derived, "source": "MCAP_OVER_PRICE"}
                        sh_m = derived
                elif sh_y not in (None, 0):
                    # Secondary fallback: adopt Yahoo shares if present.
                    m["market:shares_outstanding"] = float(sh_y)
                    yr_diag["shares_fallback"] = {"value": float(sh_y), "source": "YAHOO"}

            out[yy] = m
            if yr_diag:
                diag[str(yy)] = yr_diag

        return out, diag

    def _reconcile_revenue_concepts(self, layer1_by_year: dict):
        """
        Resolve conflicting revenue concepts (common in SEC facts):
        - Prefer SalesRevenueNet / RevenueFromContractWithCustomerExcludingAssessedTax when present.
        - If 'Revenues' exists but differs materially, override it so ratio engine doesn't pick a wrong sub-line.
        """
        if not isinstance(layer1_by_year, dict) or not layer1_by_year:
            return layer1_by_year or {}, {}

        def _sf(x):
            return self._safe_float(x)

        preferred_keys = (
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "Revenue",
        )
        reconcile_targets = ("Revenues", "Revenue")

        out = {}
        diag = {}
        for y, row in (layer1_by_year or {}).items():
            if not isinstance(row, dict):
                out[y] = row
                continue
            r = dict(row)
            pref = None
            for pk in preferred_keys:
                v = _sf(r.get(pk))
                if v is not None and v > 0:
                    pref = float(v)
                    pref_key = pk
                    break
            if pref is None:
                out[y] = r
                continue

            yr_diag = {}
            for tk in reconcile_targets:
                cur = _sf(r.get(tk))
                if cur is None or cur <= 0:
                    continue
                rel = abs(float(cur) - float(pref)) / max(abs(float(pref)), 1.0)
                # Only reconcile when mismatch is material.
                if rel >= 0.25:
                    yr_diag[tk] = {"from": float(cur), "to": float(pref), "preferred_key": pref_key, "rel_diff": rel}
                    r[tk] = float(pref)
            out[y] = r
            if yr_diag:
                diag[str(y)] = yr_diag

        return out, diag

    def _build_fas_raw_metrics(self, data_by_year: dict, financial_ratios: dict) -> dict:
        years = sorted(
            {
                int(y) for y in (list((data_by_year or {}).keys()) + list((financial_ratios or {}).keys()))
                if str(y).lstrip('-').isdigit()
            }
        )
        out = {}
        ratio_growth_rev = {}
        ratio_growth_ni = {}
        prev_rev = None
        prev_ni = None

        def _set(metric_name: str, year: int, value):
            out.setdefault(metric_name, {})[int(year)] = value

        for y in years:
            drow = (data_by_year or {}).get(y, {}) or {}
            rrow = (financial_ratios or {}).get(y, {}) or {}

            rev = self._fas_pick(drow, ['Revenues', 'SalesRevenueNet', 'Revenue', 'TotalRevenue'])
            ni = self._fas_pick(drow, ['NetIncomeLoss', 'NetIncome', 'ProfitLoss'])
            capex_raw = self._fas_pick(drow, ['PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpenditures', 'CapEx'])
            rd_raw = self._fas_pick(drow, ['ResearchAndDevelopmentExpense', 'ResearchDevelopmentExpense', 'RAndDExpense'])

            capex_to_rev = None
            if rev not in (None, 0) and capex_raw is not None:
                capex_to_rev = abs(capex_raw) / abs(rev)
            rd_to_rev = None
            if rev not in (None, 0) and rd_raw is not None:
                rd_to_rev = rd_raw / rev

            if prev_rev not in (None, 0) and rev is not None:
                ratio_growth_rev[y] = (rev - prev_rev) / abs(prev_rev)
            if prev_ni not in (None, 0) and ni is not None:
                ratio_growth_ni[y] = (ni - prev_ni) / abs(prev_ni)
            if rev is not None:
                prev_rev = rev
            if ni is not None:
                prev_ni = ni

            roe = self._fas_pick(rrow, ['roe'])
            wacc = self._fas_pick(rrow, ['wacc'])
            roe_spread = (roe - wacc) if (roe is not None and wacc is not None) else None

            _set('gross_margin', y, self._fas_pick(rrow, ['gross_margin']))
            _set('net_margin', y, self._fas_pick(rrow, ['net_margin']))
            _set('operating_margin', y, self._fas_pick(rrow, ['operating_margin']))
            _set('roic', y, self._fas_pick(rrow, ['roic']))
            _set('fcf_yield', y, self._fas_pick(rrow, ['fcf_yield']))
            _set('asset_turnover', y, self._fas_pick(rrow, ['asset_turnover']))
            _set('interest_coverage', y, self._fas_pick(rrow, ['interest_coverage']))
            _set('altman_z', y, self._fas_pick(rrow, ['altman_z_score', 'altman_z']))
            _set('leverage', y, self._fas_pick(rrow, ['debt_to_equity', 'debt_to_assets', 'net_debt_ebitda']))
            _set('nim', y, self._fas_pick(rrow, ['net_interest_margin', 'nim']))
            _set('roe_spread', y, roe_spread)
            _set('capex_to_revenue', y, capex_to_rev)
            _set('rd_to_revenue', y, rd_to_rev)

            # strict integrity metrics
            _set('ap_days', y, self._fas_pick(rrow, ['ap_days', 'days_payable_outstanding']))
            _set('dso', y, self._fas_pick(rrow, ['days_sales_outstanding', 'dso']))
            _set('inventory_days', y, self._fas_pick(rrow, ['inventory_days', 'days_inventory_outstanding']))

        out['revenue_growth'] = ratio_growth_rev
        out['net_income_growth'] = ratio_growth_ni
        return out

    def _run_financial_analysis_system(
        self,
        ticker: str,
        data_by_year: dict,
        financial_ratios: dict,
        canonical_classification: dict | None = None,
    ) -> dict:
        if self.financial_analysis_system is None:
            return {'status': 'DISABLED', 'reason': 'FINANCIAL_ANALYSIS_SYSTEM_NOT_AVAILABLE'}
        try:
            raw_metrics = self._build_fas_raw_metrics(data_by_year or {}, financial_ratios or {})
            forced_model = None
            try:
                cc = canonical_classification or {}
                entity_type = str(cc.get("entity_type") or "").strip().lower()
                sub = str(cc.get("operating_sub_sector") or "").strip().lower()
                if entity_type == "bank":
                    forced_model = "commercial_bank"
                elif sub.startswith("semiconductor"):
                    # FAS currently supports fabless/idm; treat diversified as fabless for KPIs.
                    forced_model = "semiconductor_idm" if sub.endswith("idm") else "semiconductor_fabless"
            except Exception:
                forced_model = None
            return self.financial_analysis_system.analyze(
                ticker=str(ticker or '').upper(),
                raw_metrics_by_year=raw_metrics,
                forced_model=forced_model,
            )
        except Exception as exc:
            return {'status': 'ERROR', 'reason': f'FINANCIAL_ANALYSIS_SYSTEM_FAILED: {exc}'}

    def _init_yfinance_cache(self):
        if yf is None:
            return
        try:
            cache_dir = Path('outputs') / 'yf_cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            if hasattr(yf, 'set_tz_cache_location'):
                yf.set_tz_cache_location(str(cache_dir.resolve()))
        except Exception:
            pass

    def _load_sector_profile_memory(self):
        try:
            p = self._sector_memory_path
            if p.exists():
                self._sector_profile_memory = json.loads(p.read_text(encoding='utf-8'))
            else:
                self._sector_profile_memory = {}
        except Exception:
            self._sector_profile_memory = {}

    def _save_sector_profile_memory(self):
        try:
            p = self._sector_memory_path
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(
                json.dumps(self._sector_profile_memory, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
        except Exception:
            pass

    def _resolve_sector_profile(
        self,
        company_name: str,
        ticker: str,
        data_by_year: dict = None,
        sic_description: str = '',
    ) -> str:
        t = str(ticker or '').upper().strip()
        sic_key = str(sic_description or '').strip().lower()
        memory = self._sector_profile_memory or {}
        by_ticker = memory.get('by_ticker', {})
        by_sic = memory.get('by_sic', {})

        inferred = self._infer_sector_profile(
            company_name=company_name,
            ticker=ticker,
            data_by_year=data_by_year,
            sic_description=sic_description,
        )
        # Strong override for explicitly inferred structural sectors.
        if inferred in ('bank', 'insurance', 'technology'):
            return inferred

        if t and isinstance(by_ticker.get(t), str):
            return by_ticker.get(t)
        if sic_key and isinstance(by_sic.get(sic_key), str):
            return by_sic.get(sic_key)
        return inferred

    def _resolve_sub_sector_profile(
        self,
        company_name: str,
        ticker: str,
        sector_profile: str,
        sic_description: str = '',
    ) -> str:
        t = str(ticker or '').upper().strip()
        txt = f"{company_name or ''} {sic_description or ''}".lower()
        sic_txt = str(sic_description or '').lower()
        sector = str(sector_profile or '').strip().lower()

        ticker_map = {
            'NVDA': 'semiconductor_fabless',
            'AMD': 'semiconductor_fabless',
            'QCOM': 'semiconductor_fabless',
            'MRVL': 'semiconductor_fabless',
            'AVGO': 'semiconductor_fabless',
            'MSFT': 'software_saas',
            'ORCL': 'software_saas',
            'CRM': 'software_saas',
            'SNOW': 'software_saas',
            'ADBE': 'software_saas',
            'AAPL': 'hardware_platform',
            'DELL': 'hardware_platform',
            'HPQ': 'hardware_platform',
            'JPM': 'commercial_bank',
            'BAC': 'commercial_bank',
            'WFC': 'commercial_bank',
            'C': 'commercial_bank',
            'USB': 'commercial_bank',
            'MS': 'investment_bank',
            'GS': 'investment_bank',
            'BX': 'investment_bank',
            'KKR': 'investment_bank',
            'SCHW': 'investment_bank',
            'PRU': 'insurance_life',
            'MET': 'insurance_life',
            'LNC': 'insurance_life',
            'AFL': 'insurance_life',
            'AIG': 'insurance_pc',
            'PGR': 'insurance_pc',
            'TRV': 'insurance_pc',
            'ALL': 'insurance_pc',
            'CB': 'insurance_pc',
            'AON': 'insurance_broker',
            'MMC': 'insurance_broker',
            'WTW': 'insurance_broker',
            'RYAN': 'insurance_broker',
            'XOM': 'integrated_oil',
            'CVX': 'integrated_oil',
            'BP': 'integrated_oil',
            'SHEL': 'integrated_oil',
            'KO': 'consumer_staples',
            'PEP': 'consumer_staples',
            'PG': 'consumer_staples',
            'CL': 'consumer_staples',
            'KMB': 'consumer_staples',
            'TSLA': 'ev_automaker',
            'NIO': 'ev_automaker',
            'RIVN': 'ev_automaker',
            'LCID': 'ev_automaker',
        }
        if t in ticker_map:
            return ticker_map[t]

        if sector == 'technology':
            if 'semiconductor' in txt or 'micro devices' in txt:
                return 'semiconductor_fabless'
            if any(tok in txt for tok in ('software', 'cloud', 'saas', 'productivity', 'enterprise applications')):
                return 'software_saas'
            if any(tok in txt for tok in ('iphone', 'mac', 'hardware', 'consumer electronics')):
                return 'hardware_platform'
        if sector == 'bank':
            if any(tok in txt for tok in ('investment bank', 'broker', 'securities', 'capital markets', 'asset management')):
                return 'investment_bank'
            return 'commercial_bank'
        if sector == 'insurance':
            if 'insurance agents' in sic_txt or any(tok in txt for tok in ('broker', 'brokerage', 'risk services', 'reinsurance brokerage')):
                return 'insurance_broker'
            if 'life insurance' in sic_txt or any(tok in txt for tok in ('life insurance', 'annuities', 'retirement services')):
                return 'insurance_life'
            return 'insurance_pc'
        if sector == 'industrial':
            if 'petroleum' in sic_txt or any(tok in txt for tok in ('oil', 'petroleum', 'upstream', 'downstream', 'refining')):
                return 'integrated_oil'
            if any(tok in txt for tok in ('beverage', 'consumer products', 'soft drinks', 'cola')):
                return 'consumer_staples'
            if any(tok in txt for tok in ('electric vehicle', 'ev', 'automotive')):
                return 'ev_automaker'
        return sector or 'unknown'

    def _resolve_sector_ratio_blocklist(self, sector_profile: str, sub_sector_profile: str = None) -> set:
        sub = str(sub_sector_profile or '').strip().lower()
        if sub in self.SUB_SECTOR_RATIO_BLOCKLIST:
            return set(self.SUB_SECTOR_RATIO_BLOCKLIST.get(sub, set()))
        return set(self.SECTOR_RATIO_BLOCKLIST.get(sector_profile, set()))

    def _resolve_sector_strategic_blocklist(self, sector_profile: str, sub_sector_profile: str = None) -> set:
        sub = str(sub_sector_profile or '').strip().lower()
        if sub in self.SUB_SECTOR_STRATEGIC_BLOCKLIST:
            return set(self.SUB_SECTOR_STRATEGIC_BLOCKLIST.get(sub, set()))
        return set(self.SECTOR_STRATEGIC_BLOCKLIST.get(sector_profile, set()))

    def _learn_sector_profile(self, ticker: str, sic_description: str, sector_profile: str):
        try:
            t = str(ticker or '').upper().strip()
            sic_key = str(sic_description or '').strip().lower()
            s = str(sector_profile or '').strip().lower()
            if s not in ('bank', 'insurance', 'industrial', 'technology', 'unknown'):
                return
            mem = self._sector_profile_memory or {}
            mem.setdefault('by_ticker', {})
            mem.setdefault('by_sic', {})
            if t:
                mem['by_ticker'][t] = s
            if sic_key:
                mem['by_sic'][sic_key] = s
            self._sector_profile_memory = mem
            self._save_sector_profile_memory()
        except Exception:
            pass

    def _enforce_filing_year_integrity(self, data_by_year: dict, start_year: int, end_year: int, filing_years: set):
        """
        Enforce strict year integrity:
        - if no 10-K exists for a requested year, keep an explicit empty row for that year.
        - never display carried values as if they were real SEC filings.
        """
        out = {}
        filing_years = set(int(y) for y in (filing_years or set()) if isinstance(y, int) or str(y).isdigit())
        missing_years = []
        for y in range(int(start_year), int(end_year) + 1):
            if y in filing_years:
                row = (data_by_year or {}).get(y, {}) or {}
                out[y] = dict(row) if isinstance(row, dict) else {}
            else:
                out[y] = {}
                missing_years.append(y)
        return out, missing_years

    def _clear_missing_filing_year_ratios(self, ratios_by_year: dict, missing_years):
        out = dict(ratios_by_year or {})
        for y in (missing_years or []):
            row = dict(out.get(y, {}) or {})
            reasons = dict(row.get('_ratio_reasons') or {})
            for k in list(row.keys()):
                if str(k).startswith('_'):
                    continue
                row[k] = None
                reasons[k] = 'FILING_NOT_AVAILABLE_FOR_YEAR'
            row['_ratio_reasons'] = reasons
            row['filing_availability'] = 'MISSING_10K'
            out[y] = row
        return out

    def _normalize_accession(self, accn):
        if not accn:
            return accn
        return str(accn).replace('-', '').replace('_', '').strip().lower()

    def _safe_float(self, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            try:
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    return None
                return fv
            except:
                return None
        try:
            s = str(v).strip()
            s = s.replace(',', '')
            if s == '':
                return None
            if s.startswith('(') and s.endswith(')'):
                s = '-' + s[1:-1]
            fv = float(s)
            if math.isnan(fv) or math.isinf(fv):
                return None
            return fv
        except:
            return None

    def _safe_int(self, v):
        if v is None:
            return None
        try:
            s = str(v).strip().lower()
            if s in ('', 'inf', '-inf', 'nan'):
                return None
            return int(float(s))
        except Exception:
            return None

    def _normalize_million_value(self, value):
        """
        Normalize absolute USD values into million-USD scale when needed.
        Rule: values above 1B are treated as absolute USD and converted to millions.
        """
        fv = self._safe_float(value)
        if fv is None:
            return None
        if abs(fv) > 1_000_000_000:
            return fv / 1_000_000.0
        return fv

    def _resolve_bank_anchor_value(
        self,
        data: dict,
        *,
        kind: str,
        assets: float | None,
        prev_value: float | None,
    ):
        """
        Resolve bank balance anchors (loans/deposits) robustly across issuer/year tag drift.

        Returns: (value, tag, confidence, details)
        - value is in the same scale as SEC layer values in this app (typically USD_million).
        - confidence is 0..1.
        """
        if not isinstance(data, dict) or not data:
            return None, None, 0.0, "empty_row"

        kind = str(kind or "").strip().lower()
        if kind not in ("loans", "deposits"):
            return None, None, 0.0, "unsupported_kind"

        assets_v = self._safe_float(assets)
        prev_v = self._safe_float(prev_value)

        if kind == "loans":
            primary = [
                "LoansAndLeasesReceivableNetReportedAmount",
                "LoansAndLeasesReceivable",
                "LoansReceivable",
                "FinancingReceivableExcludingAccruedInterestBeforeAllowanceForCreditLoss",
                "FinancingReceivableExcludingAccruedInterestAfterAllowanceForCreditLoss",
                "FinancingReceivable",
                "NetLoans",
                "LoansHeldForSale",
            ]
            include_tokens = ("loan", "loans", "lease", "financingreceivable", "financing receivable")
            exclude_tokens = ("allowance", "loss", "provision", "nonperform", "reserve", "chargeoff", "pastdue")
            target = 0.45
            min_ratio = 0.05
            max_ratio = 0.95
        else:
            primary = [
                "Deposits",
                "DepositLiabilities",
                "CustomerDeposits",
                "DepositsInterestBearing",
                "DepositsNoninterestBearing",
                "TotalDeposits",
            ]
            include_tokens = ("deposit", "deposits")
            exclude_tokens = ("premium", "insurance")
            target = 0.70
            min_ratio = 0.05
            max_ratio = 1.20

        # Build candidate tags: primary list + token matches in row.
        candidates = []
        seen = set()
        for tag in primary:
            if tag in seen:
                continue
            seen.add(tag)
            candidates.append((tag, "primary"))
        for k in list(data.keys()):
            lk = str(k or "").lower().replace("_", "").replace("-", "").replace(" ", "")
            if not lk:
                continue
            if any(tok.replace(" ", "") in lk for tok in include_tokens) and not any(tok.replace(" ", "") in lk for tok in exclude_tokens):
                if k not in seen:
                    seen.add(k)
                    candidates.append((k, "token"))

        scales = [1.0, 1e-3, 1e-6, 1e3, 1e6]
        best = None  # (score, value, tag, conf, details)

        for tag, source_kind in candidates:
            raw = self._safe_float(data.get(tag))
            if raw in (None, 0):
                continue
            for s in scales:
                v = float(raw) * float(s)
                if v in (None, 0) or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                    continue
                ratio_to_assets = None
                if assets_v not in (None, 0):
                    ratio_to_assets = abs(v) / max(abs(float(assets_v)), 1e-12)
                    if ratio_to_assets < min_ratio or ratio_to_assets > max_ratio:
                        continue
                # Prefer closeness to previous year when available (stability).
                prev_pen = 0.0
                if prev_v not in (None, 0):
                    gap = max(abs(v), abs(prev_v)) / max(min(abs(v), abs(prev_v)), 1e-12)
                    if gap > 1.50:
                        prev_pen = min(1.0, (gap - 1.50) / 2.0)
                # Prefer minimal scaling adjustments.
                try:
                    scale_pen = abs(math.log10(abs(s))) if s not in (0.0, 1.0) else 0.0
                except Exception:
                    scale_pen = 0.0
                # Core plausibility: target ratio to assets.
                if ratio_to_assets is not None:
                    core = abs(ratio_to_assets - target)
                else:
                    core = 0.25  # weak without assets
                score = core + (0.25 * prev_pen) + (0.10 * scale_pen)

                base_conf = 0.90 if source_kind == "primary" else 0.70
                if ratio_to_assets is None:
                    base_conf -= 0.20
                base_conf -= min(0.30, 0.10 * scale_pen)
                base_conf -= min(0.30, 0.20 * prev_pen)
                conf = max(0.0, min(1.0, base_conf))

                details = f"{source_kind}:{tag}:scale={s}"
                if best is None or score < best[0]:
                    best = (score, v, tag, conf, details)

        if best is None:
            return None, None, 0.0, "no_candidate"
        return best[1], best[2], float(best[3]), best[4]

    @staticmethod
    def _infer_money_scale_from_anchors(anchor_values: list):
        try:
            if not anchor_values:
                return None
            abs_vals = [abs(float(v)) for v in anchor_values if v not in (None, 0)]
            if not abs_vals:
                return None
            # Heuristic: if we see a mix of very large (raw USD) and small (already USD_million) anchors,
            # prefer scaling raw USD to million USD. Use a high threshold to avoid scaling large banks' assets.
            has_small_million = any(1.0 <= v <= 1_000_000.0 for v in abs_vals)
            has_raw_like = any(v >= 100_000_000.0 for v in abs_vals)  # >= $100m in raw USD scale
            if has_small_million and has_raw_like:
                return 1_000_000.0

            # Secondary: thousands-scale (rare) when anchors look like thousands of USD.
            has_small_thousands = any(1.0 <= v <= 1_000.0 for v in abs_vals)
            has_large_thousands = any(v >= 100_000.0 for v in abs_vals)
            if has_small_thousands and has_large_thousands:
                return 1_000.0

            return None
        except Exception:
            return None

    def _harmonize_layer1_money_units_to_million(self, layer1_by_year: dict):
        """
        Ensure Layer1 statement values are consistently in USD_million when a clear scale mismatch is detected.
        This prevents EPS/BVPS explosions when net income/revenue are in raw USD while equity/assets are in USD_million.
        """
        if not isinstance(layer1_by_year, dict):
            return layer1_by_year, {}

        out = {}
        diagnostics = {}
        skip_tokens = (
            'share',
            'shares',
            'eps',
            'per share',
            'ratio',
            'margin',
            '%',
            'return',
            'roe',
            'roa',
            'roic',
            'turnover',
            'days',
        )
        anchor_keys = (
            'Revenues',
            'Revenue',
            'SalesRevenueNet',
            'RevenueFromContractWithCustomerExcludingAssessedTax',
            'NetIncomeLoss',
            'ProfitLoss',
            'Assets',
            'TotalAssets',
            'StockholdersEquity',
            'TotalEquity',
            'Liabilities',
            'LiabilitiesCurrent',
        )
        for y, row in (layer1_by_year or {}).items():
            if not isinstance(row, dict):
                out[y] = row
                continue
            row2 = dict(row)
            anchors = []
            for ak in anchor_keys:
                if ak in row2:
                    fv = self._safe_float(row2.get(ak))
                    if fv is not None:
                        anchors.append(fv)
            scale = self._infer_money_scale_from_anchors(anchors)
            adjusted = 0
            if scale:
                for k, v in list(row2.items()):
                    if not isinstance(k, str):
                        continue
                    lk = k.lower()
                    if any(tok in lk for tok in skip_tokens):
                        continue
                    fv = self._safe_float(v)
                    if fv is None:
                        continue
                    if abs(fv) >= 1_000_000.0:
                        row2[k] = fv / scale
                        adjusted += 1
            else:
                # Even when anchors look consistent, individual non-anchor concepts can still
                # appear in raw USD in a single year (e.g., COGS=653,000,000 alongside
                # Revenues=5,253 in USD_million). Apply a high-threshold guard per cell.
                for k, v in list(row2.items()):
                    if not isinstance(k, str):
                        continue
                    lk = k.lower()
                    if any(tok in lk for tok in skip_tokens):
                        continue
                    fv = self._safe_float(v)
                    if fv is None:
                        continue
                    # 10,000,000 in USD_million is $10T; beyond plausible for line items here.
                    if abs(fv) >= 10_000_000.0:
                        row2[k] = fv / 1_000_000.0
                        adjusted += 1
            out[y] = row2
            diagnostics[str(y)] = {
                'scale_applied': scale,
                'adjusted_key_count': adjusted,
            }
        return out, diagnostics

    def _reconcile_balance_sheet_totals(self, layer1_by_year: dict):
        """
        Reconcile balance-sheet anchors when duplicate concepts exist at different scales.

        Real-world issue observed in some bank tickers:
        - Assets/Liabilities concepts may appear 1,000x smaller than the true totals.
        - LiabilitiesAndStockholdersEquity (or similarly named "total liabilities and shareholders' equity")
          can carry the correct total-assets scale.

        Policy:
        - Fail-closed unless mismatch is extreme (>= 100x).
        - When applied, record diagnostics so it is not a silent repair.
        """
        if not isinstance(layer1_by_year, dict):
            return layer1_by_year, {}

        def _sf(x):
            return self._safe_float(x)

        def _find_total_key(row: dict):
            cands = []
            for k, v in (row or {}).items():
                if not isinstance(k, str):
                    continue
                lk = k.lower()
                if (
                    ("liabilit" in lk)
                    and ("equity" in lk)
                    and (("shareholder" in lk) or ("stockholder" in lk))
                ):
                    fv = _sf(v)
                    if fv is not None and fv != 0:
                        cands.append((abs(fv), k, float(fv)))
            if not cands:
                return None
            cands.sort(key=lambda t: t[0], reverse=True)
            return cands[0]

        out = {}
        diag = {}
        for y, row in (layer1_by_year or {}).items():
            if not isinstance(row, dict):
                out[y] = row
                continue
            row2 = dict(row)
            found = _find_total_key(row2)
            if not found:
                out[y] = row2
                continue

            total_abs, total_key, total_val = found
            assets_val = _sf(row2.get("Assets") if "Assets" in row2 else row2.get("TotalAssets"))
            liab_val = _sf(row2.get("Liabilities") if "Liabilities" in row2 else row2.get("TotalLiabilities"))
            equity_val = _sf(
                row2.get("StockholdersEquity")
                if "StockholdersEquity" in row2
                else (row2.get("TotalEquity") if "TotalEquity" in row2 else row2.get("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"))
            )

            applied = {}
            # A) Assets: override only when the mismatch is extreme.
            if assets_val is None or (abs(total_val) >= max(1.0, abs(assets_val)) * 1e2):
                if assets_val is None or abs(total_val - assets_val) > 1e-9:
                    row2["Assets"] = float(total_val)
                    row2["TotalAssets"] = float(total_val)
                    applied["assets"] = {"from": assets_val, "to": float(total_val), "source_key": total_key}

            # B) Liabilities: if equity exists, derive liabilities from total and equity when mismatch is extreme.
            if isinstance(equity_val, (int, float)) and equity_val not in (None, 0):
                derived_liab = float(total_val) - float(equity_val)
                if liab_val is None or (abs(derived_liab) >= max(1.0, abs(liab_val)) * 1e2):
                    row2["Liabilities"] = float(derived_liab)
                    row2["TotalLiabilities"] = float(derived_liab)
                    applied["liabilities"] = {"from": liab_val, "to": float(derived_liab), "source": f"{total_key} - StockholdersEquity"}

            out[y] = row2
            if applied:
                diag[str(y)] = applied

        return out, diag

    def _harmonize_layer1_series_scale_to_million(self, layer1_by_year: dict):
        """
        Fix within-concept mixed scales across years (e.g., Deposits reported as 1,260.934 in one year
        and 1,197,259 in another, both representing USD_million but one accidentally in USD_billion).

        Heuristic (safe, fail-closed):
        - Only applies when a concept has at least one "large" observation (>= 100_000) and one "small"
          observation (<= 10_000).
        - Each small observation is scaled by 1000x only when doing so makes it consistent with the
          large-cluster median (allows real growth across years; not a strict 1000x ratio check).
        - Skips per-share/ratio-like concepts using the same token list as money harmonizer.
        """
        if not isinstance(layer1_by_year, dict) or not layer1_by_year:
            return layer1_by_year or {}, {}

        skip_tokens = (
            'share',
            'shares',
            'eps',
            'per share',
            'ratio',
            'margin',
            '%',
            'return',
            'roe',
            'roa',
            'roic',
            'turnover',
            'days',
        )

        # Build key -> {year: value}
        keys = sorted({k for y in layer1_by_year.keys() for k in (layer1_by_year.get(y) or {}).keys() if isinstance(k, str)})
        diagnostics = {}
        out = {int(y): dict(row or {}) for y, row in (layer1_by_year or {}).items() if str(y).isdigit()}

        def _sf(x):
            return self._safe_float(x)

        for k in keys:
            lk = k.lower()
            if any(tok in lk for tok in skip_tokens):
                continue
            series = []
            for y, row in out.items():
                v = _sf((row or {}).get(k))
                if v is None or v == 0:
                    continue
                series.append((y, float(v)))
            if len(series) < 3:
                continue
            abs_vals = [abs(v) for _, v in series]
            large = [v for v in abs_vals if v >= 100_000.0]
            small = [v for v in abs_vals if v <= 10_000.0]
            if not large or not small:
                continue
            # Reference: median of large cluster (USD_million scale).
            large_sorted = sorted(large)
            large_med = large_sorted[len(large_sorted) // 2]
            if large_med <= 0:
                continue

            # Scale small years up by 1000 (billion -> million).
            changed_years = {}
            for y, v in series:
                if abs(v) <= 10_000.0:
                    new_v = v * 1000.0
                    # Accept scaling only when the scaled value is within a wide but bounded window
                    # of the large-cluster median (allows growth, but avoids arbitrary scaling).
                    if not (large_med / 5.0 <= abs(new_v) <= large_med * 5.0):
                        continue
                    out[y][k] = new_v
                    changed_years[str(y)] = {
                        "from": v,
                        "to": new_v,
                        "rule": "x1000_series_scale",
                        "reference_large_median": large_med,
                    }
            if changed_years:
                diagnostics[k] = changed_years

        return out, diagnostics

    def _normalize_shares_to_million(self, value):
        """
        Normalize share counts to "million shares" base.
        SEC can expose absolute shares (billions) while statement rows are in millions.
        """
        fv = self._safe_float(value)
        if fv is None:
            return None
        av = abs(fv)
        # Absolute shares (e.g., 15,300,000,000) -> 15,300 million shares.
        if av >= 100_000_000.0:
            return fv / 1_000_000.0
        # Thousands-of-shares (common in SEC facts): 16,701,272 -> 16,701.272 million.
        if av >= 100_000.0:
            return fv / 1_000.0
        return fv

    def _normalize_shares_to_million_with_anchor(self, value, price=None, market_cap_m=None):
        """
        Market-aware shares normalization to million-share base.
        Tries candidate scales and selects the one consistent with market cap and price.
        """
        fv = self._safe_float(value)
        if fv is None:
            return None
        px = self._safe_float(price)
        mcap = self._safe_float(market_cap_m)
        cands = [
            fv,                 # already in millions
            fv / 1_000.0,       # thousands -> millions
            fv / 1_000_000.0,   # absolute shares -> millions
            fv * 1_000.0,       # billions -> millions
        ]
        cands = [c for c in cands if c is not None and c > 0]
        if not cands:
            return None
        # Plausibility window in million shares.
        plausible = [c for c in cands if 1.0 <= abs(c) <= 500_000.0]
        pool = plausible if plausible else cands
        if px not in (None, 0) and mcap not in (None, 0):
            # In this codebase market cap is in million USD and price in USD,
            # therefore expected shares in million = mcap / price.
            target_sh_m = abs(mcap / px)
            if target_sh_m > 0:
                return min(pool, key=lambda c: abs(abs(c) - target_sh_m))
        # Fallback to generic normalization.
        return self._normalize_shares_to_million(fv)

    def _normalize_market_scalar(self, value, metric_key=None, reference=None, price=None, shares=None):
        """
        Normalize market scalars to system million-USD base for valuation math.
        This keeps price-based ratios unit-consistent with SEC statement rows.
        """
        fv = self._safe_float(value)
        if fv is None:
            return None
        k = str(metric_key or '').strip().lower()
        ref = self._safe_float(reference)
        px = self._safe_float(price)
        sh = self._safe_float(shares)

        if k in self.MARKET_MILLION_KEYS:
            # Strong anchor for market cap: compare against price*shares when available.
            if k == 'market_cap' and px not in (None, 0) and sh not in (None, 0):
                # SEC rows often use shares in millions, while market layers use absolute shares.
                # Build both anchors deterministically to avoid double 1e6 conversion.
                if abs(sh) < 1_000_000:
                    expected_m = abs(px * sh)
                    expected_usd = expected_m * 1_000_000.0
                else:
                    expected_usd = abs(px * sh)
                    expected_m = expected_usd / 1_000_000.0
                if expected_usd > 0 and expected_m > 0:
                    rel_raw = abs(abs(fv) - expected_usd) / expected_usd
                    rel_m = abs(abs(fv) - expected_m) / expected_m
                    if rel_m <= 0.35:
                        return fv
                    if rel_raw <= 0.35:
                        return fv / 1_000_000.0
            # Secondary anchor: choose debt/EV scale plausible vs statement reference.
            if ref not in (None, 0):
                cands = [fv, fv / 1_000.0, fv / 1_000_000.0, fv / 1_000_000_000.0]
                plausible = [c for c in cands if 0.01 <= (abs(c) / max(abs(ref), 1e-9)) <= 500.0]
                if plausible:
                    target = 20.0 if k == 'market_cap' else (15.0 if k == 'enterprise_value' else 0.35)
                    return min(plausible, key=lambda c: abs((abs(c) / max(abs(ref), 1e-9)) - target))
            if abs(fv) > 1_000_000_000:
                return fv / 1_000_000.0
        return fv

    def pick_correct_unit(self, candidates: dict):
        """
        Golden unit rule requested by user:
        if candidate values differ by ~1,000,000x then pick the smaller one
        (typically the one already in millions).
        """
        if not isinstance(candidates, dict) or not candidates:
            return None
        values = []
        for _k, v in candidates.items():
            fv = self._safe_float(v)
            if fv is None:
                continue
            av = abs(fv)
            if av > 0:
                values.append(av)
        if not values:
            return None
        min_v = min(values)
        max_v = max(values)
        if min_v > 0 and (max_v / min_v) > 500_000:
            return min_v
        # Keep deterministic preference to first inserted candidate.
        for _k, v in candidates.items():
            fv = self._safe_float(v)
            if fv is not None:
                return fv
        return None

    def _pick_canonical_label_value(self, row: dict, concept_key: str):
        """
        Pick value using canonical label priority first, then controlled fallback labels.
        Returns: (value, label, source_kind) where source_kind in {'canonical','fallback','missing'}.
        """
        if not isinstance(row, dict):
            return None, None, 'missing'

        context_seed = []
        for key in (
            'Revenues', 'Revenue', 'CostOfRevenue', 'NetIncomeLoss',
            'Assets', 'TotalAssets', 'StockholdersEquity', 'Total Equity',
            'Liabilities', 'TotalLiabilities',
        ):
            fv = self._safe_float(row.get(key))
            if fv is not None and abs(fv) > 0:
                context_seed.append(abs(fv))
        if context_seed:
            context_seed = sorted(context_seed)
            context_median = context_seed[len(context_seed) // 2]
        else:
            context_median = None

        def _learn_mapping_once(financial_concept: str, sec_concept: str):
            if not financial_concept or not sec_concept:
                return
            if self.auto_learner is None:
                return
            seen = getattr(self, '_learning_seen_pairs', None)
            if seen is None:
                seen = set()
                setattr(self, '_learning_seen_pairs', seen)
            pair = (str(financial_concept), str(sec_concept))
            if pair in seen:
                return
            seen.add(pair)
            try:
                self.auto_learner.learn_from_usage(str(financial_concept), str(sec_concept), True)
            except Exception:
                pass

        def _gather(labels):
            cands = {}
            for lbl in labels:
                if concept_key == 'Equity' and lbl in self.CONTROL_TOTALS_NOT_EQUITY:
                    continue
                if lbl in row:
                    fv = self._safe_float(row.get(lbl))
                    if fv is not None:
                        fv = self._detect_and_normalize(fv, context_median=context_median)
                        # Net income can appear duplicated in full dollars under ProfitLoss.
                        if concept_key == 'NetIncome' and lbl == 'ProfitLoss' and abs(fv) > 1_000_000:
                            fv = fv / 1_000_000.0
                        # COGS/Revenue frequently duplicated in full dollars.
                        if concept_key in ('Revenue', 'COGS') and abs(fv) > 1_000_000:
                            fv = fv / 1_000_000.0
                        cands[lbl] = fv
            return cands

        canonical_labels = list(self.CANONICAL_LABELS.get(concept_key, []))
        canonical_cands = _gather(canonical_labels)
        if canonical_cands:
            # Revenue-specific plausibility arbitration:
            # some issuers expose both annual revenue and smaller quarter-like revenue labels.
            # Prefer the candidate that is economically consistent with COGS / gross profit / net income.
            if concept_key == 'Revenue':
                fallback_labels = list(self.CANONICAL_LABEL_FALLBACKS.get(concept_key, []))
                fallback_cands = _gather(fallback_labels)
                merged_revenue_cands = dict(canonical_cands)
                for _k, _v in (fallback_cands or {}).items():
                    if _k not in merged_revenue_cands:
                        merged_revenue_cands[_k] = _v
                if len(merged_revenue_cands) > 1:
                    cogs_ref = None
                    for _rk in ('CostOfRevenue', 'CostOfGoodsAndServicesSold', 'CostOfSales', 'COGS'):
                        _rv = self._safe_float(row.get(_rk))
                        if _rv is not None:
                            cogs_ref = max(cogs_ref or 0.0, abs(_rv))
                    gross_ref = self._safe_float(row.get('GrossProfit'))
                    gross_ref = abs(gross_ref) if gross_ref is not None else None
                    net_ref = None
                    for _rk in ('NetIncomeLoss', 'NetIncome', 'ProfitLoss'):
                        _rv = self._safe_float(row.get(_rk))
                        if _rv is not None:
                            net_ref = max(net_ref or 0.0, abs(_rv))

                    best_label = None
                    best_score = None
                    for lbl, val in merged_revenue_cands.items():
                        av = abs(float(val))
                        if av <= 0:
                            continue
                        score = 0.0
                        if cogs_ref is not None and av < (0.80 * cogs_ref):
                            score += 8.0 + ((0.80 * cogs_ref - av) / max(cogs_ref, 1e-9))
                        if gross_ref is not None and av <= gross_ref:
                            score += 8.0 + ((gross_ref - av) / max(gross_ref, 1e-9))
                        if net_ref is not None:
                            nm = net_ref / av
                            if nm > 0.70:
                                score += 20.0 * (nm - 0.70)
                            score += abs(min(nm, 1.0) - 0.15)
                        if lbl in canonical_labels:
                            score -= 0.15
                        else:
                            score += 0.15
                        if best_score is None or score < best_score:
                            best_score = score
                            best_label = lbl
                    if best_label is not None:
                        src_kind = 'canonical' if best_label in canonical_labels else 'fallback'
                        picked_val = merged_revenue_cands[best_label]
                        # Unit sanity fix: if revenue is far below COGS/gross profit,
                        # try a 10x correction before giving up (common SEC unit slip).
                        try:
                            pv = float(picked_val)
                            if cogs_ref is not None and pv > 0:
                                if pv < (0.5 * cogs_ref) and (pv * 10.0) >= (0.8 * cogs_ref):
                                    picked_val = pv * 10.0
                            if gross_ref is not None and pv > 0:
                                if pv <= gross_ref and (pv * 10.0) > gross_ref:
                                    picked_val = max(picked_val, pv * 10.0)
                        except Exception:
                            pass
                        _learn_mapping_once(concept_key, best_label)
                        return picked_val, best_label, src_kind

            picked_abs = self.pick_correct_unit(canonical_cands)
            if picked_abs is None:
                return None, None, 'missing'
            for lbl in canonical_labels:
                if lbl in canonical_cands and abs(abs(canonical_cands[lbl]) - abs(picked_abs)) < 1e-9:
                    _learn_mapping_once(concept_key, lbl)
                    return canonical_cands[lbl], lbl, 'canonical'
            first_lbl = next(iter(canonical_cands.keys()))
            _learn_mapping_once(concept_key, first_lbl)
            return canonical_cands[first_lbl], first_lbl, 'canonical'

        fallback_labels = list(self.CANONICAL_LABEL_FALLBACKS.get(concept_key, []))
        fallback_cands = _gather(fallback_labels)
        if fallback_cands:
            picked_abs = self.pick_correct_unit(fallback_cands)
            if picked_abs is None:
                return None, None, 'missing'
            for lbl in fallback_labels:
                if lbl in fallback_cands and abs(abs(fallback_cands[lbl]) - abs(picked_abs)) < 1e-9:
                    _learn_mapping_once(concept_key, lbl)
                    return fallback_cands[lbl], lbl, 'fallback'
            first_lbl = next(iter(fallback_cands.keys()))
            _learn_mapping_once(concept_key, first_lbl)
            return fallback_cands[first_lbl], first_lbl, 'fallback'

        return None, None, 'missing'

    def _detect_and_normalize(self, value, context_median=None):
        """
        Unit normalization guard from expert protocol:
        - if value is disproportionally large vs context, scale down.
        - if absolute value clearly looks like full dollars, normalize to millions.
        """
        v = self._safe_float(value)
        if v is None:
            return None
        av = abs(v)
        if av <= 0:
            return v
        cm = self._safe_float(context_median)
        if cm is not None and cm > 0:
            ratio = av / abs(cm)
            if ratio > 500_000:
                return v / 1_000_000.0
            if ratio > 500:
                return v / 1_000.0
        if av > 1_000_000_000:
            return v / 1_000_000.0
        return v

    def _validate_ratio_gate(self, ratio_name: str, value):
        rule = self.VALIDATION_RULES.get(str(ratio_name or '').lower())
        if not rule:
            return value
        v = self._safe_float(value)
        if v is None:
            return value
        if 'min' in rule and 'max' in rule:
            if v < rule['min'] or v > rule['max']:
                action = str(rule.get('action', 'FLAG')).upper()
                if action == 'HALT':
                    raise ValueError(
                        f"UNIT_ERROR:{ratio_name}={v} خارج النطاق [{rule['min']},{rule['max']}]"
                    )
                return None
        return v

    def _normalize_value_by_decimals(self, value, decimals):
        """
        SEC strict protocol (requested):
        If decimals is negative, interpret it as a scaling factor.
        Example: value=100, decimals=-6 => 100,000,000
        """
        fv = self._safe_float(value)
        if fv is None:
            return None
        dec = self._safe_int(decimals)
        if dec is None:
            return fv
        if dec < 0:
            try:
                return fv * (10 ** abs(dec))
            except Exception:
                return fv
        return fv

    def _normalize_concept_key(self, text):
        s = str(text or '').strip().lower()
        if not s:
            return ''
        s = re.sub(r'[\s\-_:/\.\(\)\[\]]+', '', s)
        return s

    def _tokenize_concept_key(self, text):
        s = str(text or '').strip()
        if not s:
            return []
        # split CamelCase + separators
        s = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', s)
        parts = re.split(r'[^A-Za-z0-9]+', s.lower())
        return [p for p in parts if p]

    def _semantic_bucket_hints(self):
        return {
            'revenue': {'need_any': {'revenue', 'sales'}, 'exclude': {'cost', 'expense', 'tax', 'interest', 'per', 'share'}},
            'cogs': {'need_any': {'cost', 'cogs'}, 'need_all_one_of': [{'revenue'}, {'sales'}, {'goods'}], 'exclude': {'operating', 'interest', 'tax'}},
            'gross_profit': {'need_any': {'gross', 'profit'}, 'exclude': {'comprehensive'}},
            'operating_income': {'need_any': {'operating', 'income', 'profit'}, 'exclude': {'comprehensive', 'other', 'tax'}},
            'net_income': {'need_any': {'net', 'income', 'profit'}, 'exclude': {'comprehensive', 'other', 'tax', 'interest'}},
            'assets': {'need_any': {'assets', 'asset'}, 'exclude': {'current', 'noncurrent'}},
            'current_assets': {'need_any': {'current', 'assets', 'asset'}, 'exclude': {'noncurrent'}},
            'liabilities': {'need_any': {'liabilities', 'liability'}, 'exclude': {'current', 'noncurrent', 'andstockholdersequity'}},
            'current_liabilities': {'need_any': {'current', 'liabilities', 'liability'}, 'exclude': {'noncurrent'}},
            'equity': {'need_any': {'equity', 'stockholders'}, 'exclude': {'liabilitiesandstockholdersequity'}},
            'ar': {'need_any': {'receivable', 'receivables', 'accounts'}, 'exclude': {'allowance', 'doubtful'}},
            'ap': {'need_any': {'payable', 'payables', 'accounts'}, 'exclude': {'interest'}},
            'inventory': {'need_any': {'inventory', 'inventories'}, 'exclude': set()},
            'cash': {'need_any': {'cash', 'cashequivalents'}, 'exclude': {'interest'}},
            'ocf': {'need_any': {'operating', 'cash', 'activities'}, 'exclude': {'investing', 'financing'}},
            'capex': {'need_any': {'acquire', 'property', 'plant', 'equipment', 'capital', 'expenditures'}, 'exclude': {'proceeds'}},
            'shares': {'need_any': {'shares', 'share', 'weightedaverage'}, 'exclude': {'price'}},
            'interest_expense': {'need_any': {'interest', 'expense', 'costs'}, 'exclude': {'income'}},
            'depreciation': {'need_any': {'depreciation', 'amortization', 'depletion'}, 'exclude': set()},
            'dividends': {'need_any': {'dividend', 'dividends', 'paid'}, 'exclude': {'per', 'share'}},
            'net_interest_income': {'need_any': {'net', 'interest', 'income'}, 'exclude': {'expense'}},
            'interest_income': {'need_any': {'interest', 'income'}, 'exclude': {'expense'}},
            'noninterest_income': {'need_any': {'noninterest', 'income'}, 'exclude': {'expense'}},
            'noninterest_expense': {'need_any': {'noninterest', 'expense'}, 'exclude': {'income'}},
            'deposits': {'need_any': {'deposit', 'deposits'}, 'exclude': {'interestexpense'}},
            'loans': {'need_any': {'loan', 'loans', 'leases', 'receivable'}, 'exclude': {'allowance'}},
            'cet1': {'need_any': {'cet1', 'tier1', 'capital'}, 'exclude': {'ratio'}},
        }

    def _build_semantic_concept_index(self, data):
        idx = {
            'norm_to_values': {},
            'rows': [],
        }
        for k, v in (data or {}).items():
            fv = self._safe_float(v)
            if fv is None:
                continue
            norm = self._normalize_concept_key(k)
            toks = set(self._tokenize_concept_key(k))
            norm_fv = self._detect_and_normalize(fv)
            lk = str(k or '').lower()
            if (
                isinstance(norm_fv, (int, float))
                and not any(tok in lk for tok in ('share', 'per', 'ratio', 'margin', 'turnover', 'days', 'yield', 'score'))
                and abs(norm_fv) >= 10_000_000.0
            ):
                norm_fv = norm_fv / 1_000_000.0
            row = {'key': k, 'value': float(norm_fv), 'norm': norm, 'tokens': toks}
            idx['rows'].append(row)
            if norm and norm not in idx['norm_to_values']:
                idx['norm_to_values'][norm] = float(norm_fv)
        return idx

    def _semantic_pick_bucket_value(self, bucket, index):
        hints = self._semantic_bucket_hints().get(str(bucket or '').lower())
        if not hints:
            return None, None
        need_any = set(hints.get('need_any') or [])
        exclude = set(hints.get('exclude') or [])
        need_all_one_of = hints.get('need_all_one_of') or []
        best = None
        for row in (index or {}).get('rows', []):
            toks = row.get('tokens') or set()
            if exclude and any(t in toks for t in exclude):
                continue
            if need_any and not any(t in toks for t in need_any):
                continue
            ok_groups = True
            for grp in need_all_one_of:
                if grp and not any(g in toks for g in grp):
                    ok_groups = False
                    break
            if not ok_groups:
                continue
            overlap = len(toks & need_any) if need_any else 1
            score = overlap - (0.0000001 * abs(row.get('value', 0.0)))
            if (best is None) or (score > best[0]):
                best = (score, row)
        if best is None:
            return None, None
        return best[1].get('value'), best[1].get('key')

    def _select_per_share_scaled_value(self, numerator, shares, price_hint=None, target_ratio=None):
        """
        Choose the most plausible per-share value under mixed SEC scales.
        Tries numerator scales and shares scales to handle SEC mixed units
        (e.g., numerator in millions and shares in thousands or millions).
        """
        num = self._safe_float(numerator)
        sh = self._safe_float(shares)
        px = self._safe_float(price_hint)
        trg = self._safe_float(target_ratio)
        if num is None or sh in (None, 0):
            return None

        num_scales = [1e-6, 1e-3, 1.0, 1_000.0, 1_000_000.0]
        sh_scales = [1.0, 1_000.0, 1_000_000.0]
        candidates = []
        for ns in num_scales:
            for ss in sh_scales:
                denom = sh * ss
                if denom == 0:
                    continue
                value = (num * ns) / denom
                if isinstance(value, (int, float)) and value == value and abs(value) != float('inf'):
                    shift = abs(math.log10(ns)) + abs(math.log10(ss))
                    candidates.append({
                        'value': value,
                        'num_scale': ns,
                        'share_scale': ss,
                        'scale_shift': shift,
                    })
        if not candidates:
            return None

        sh_abs = abs(sh)
        num_abs = abs(num)

        # If we know a market ratio (e.g., PE), infer target per-share value from price.
        if px not in (None, 0) and trg not in (None, 0):
            try:
                implied = px / trg
                plausible = [c for c in candidates if 0.0001 <= abs(c['value']) <= 100000]
                if plausible:
                    best = min(
                        plausible,
                        key=lambda c: (
                            abs(abs(c['value']) - abs(implied)),
                            c['scale_shift'],
                        ),
                    )
                    return best['value']
            except Exception:
                pass

        # SEC annual direct extraction usually stores numerator and shares in millions.
        # In that common case, forcing extra scale shifts produces tiny artifacts (e.g. 0.02 EPS).
        if 100 <= sh_abs <= 500_000 and num_abs >= 10:
            direct = next(
                (c for c in candidates if c.get('num_scale') == 1.0 and c.get('share_scale') == 1.0),
                None,
            )
            if direct is not None:
                dv = self._safe_float(direct.get('value'))
                if dv is not None and 0.0001 <= abs(dv) <= 100000:
                    return dv

        # Otherwise, prefer candidate yielding a realistic valuation band when price exists.
        if px not in (None, 0):
            scored = []
            for c in candidates:
                val = c['value']
                if val in (None, 0):
                    continue
                pe = abs(px / val)
                if 1.0 <= pe <= 250.0:
                    scored.append((abs(pe - 25.0), c['scale_shift'], val))
            if scored:
                scored.sort(key=lambda x: x[0])
                return scored[0][2]

        def _candidate_score(c):
            v = abs(float(c.get('value') or 0.0))
            score = 0.0
            if not (0.0001 <= v <= 100000):
                score += 100.0
            score += float(c.get('scale_shift') or 0.0) * 0.35
            if v < 0.05:
                score += 12.0 + (0.05 - v) * 40.0
            if v > 10000:
                score += 8.0 + (v / 10000.0)
            # Baseline attraction around realistic per-share magnitudes.
            score += abs(math.log10(max(v, 1e-12)) - math.log10(8.0)) * 0.8

            # If shares look like "millions", avoid synthetic denominator rescaling.
            if 100 <= sh_abs <= 500_000:
                if c.get('share_scale') != 1.0:
                    score += 8.0
                if num_abs >= 10 and c.get('num_scale') != 1.0:
                    score += 4.0
            # If shares are already large absolute counts, discourage upscaling denominator.
            if sh_abs >= 1_000_000 and float(c.get('share_scale') or 1.0) > 1.0:
                score += 5.0
            return score

        plausible = [c for c in candidates if 0.0001 <= abs(c['value']) <= 100000]
        if plausible:
            best = min(plausible, key=_candidate_score)
            return best['value']
        return candidates[0]['value']

    def _line_item_to_concept(self, line_item):
        txt = str(line_item or "").strip().lower()
        if not txt:
            return None
        t = re.sub(r"[^a-z0-9 ]+", " ", txt)
        t = re.sub(r"\s+", " ", t).strip()

        movement_tokens = (
            "increase in",
            "decrease in",
            "changes in",
            "change in",
            "net change in",
        )

        def is_movement_line():
            return any(tok in t for tok in movement_tokens)

        def has(*parts):
            return all(p in t for p in parts)

        if (
            has("net cash provided by", "operating activities")
            or has("net cash from", "operating activities")
            or has("cash generated by", "operating activities")
        ):
            return "NetCashProvidedByUsedInOperatingActivities"
        if has("net cash provided by", "investing activities") or has("net cash from", "investing activities"):
            return "NetCashProvidedByUsedInInvestingActivities"
        if has("net cash provided by", "financing activities") or has("net cash from", "financing activities"):
            return "NetCashProvidedByUsedInFinancingActivities"
        if has("deferred", "revenue"):
            return "DeferredRevenue"
        if (
            has("net sales")
            or has("sales net")
            or has("net revenue")
            or has("sales to customers")
            or (
                has("sales")
                and not has("cost", "sales")
                and not has("sales taxes")
                and not has("proceeds from sales")
            )
            or (has("revenue") and not has("cost", "revenue") and not has("deferred", "revenue"))
        ):
            return "Revenues"
        if has("cost of sales") or has("cost of revenue") or has("cost of goods"):
            return "CostOfRevenue"
        if has("gross profit"):
            return "GrossProfit"
        if has("operating income") or has("income from operations"):
            return "OperatingIncomeLoss"
        if has("earnings before", "tax") or has("income before", "tax"):
            return "IncomeBeforeTax"
        if has("net income") or has("net earnings"):
            return "NetIncomeLoss"
        if has("total assets") and not is_movement_line():
            return "Assets"
        if (
            has("total liabilities and stockholders equity")
            or has("total liabilities and shareholders equity")
            or (
                ("total" in t and "liabilities" in t and "equity" in t)
                and ("stockholder" in t or "shareholder" in t or "stockholders" in t or "shareholders" in t)
            )
            or has("liabilities and equity")
        ):
            return "LiabilitiesAndStockholdersEquity"
        if has("total liabilities") and not is_movement_line():
            return "Liabilities"
        if has("total equity") or has("total stockholders equity") or has("total shareholders equity"):
            return "StockholdersEquity"
        if has("current assets") and not is_movement_line():
            return "AssetsCurrent"
        if has("current liabilities") and not is_movement_line():
            return "LiabilitiesCurrent"
        if has("cash and cash equivalents"):
            return "CashAndCashEquivalentsAtCarryingValue"
        if (has("vendor non trade receivables") or has("vendor non-trade receivables")) and not is_movement_line():
            return "VendorNonTradeReceivables"
        if has("accounts receivable") and not is_movement_line():
            return "AccountsReceivableNetCurrent"
        if (has("inventories") or has("inventory")) and not is_movement_line():
            return "InventoryNet"
        if has("accounts payable") and not is_movement_line():
            return "AccountsPayableCurrent"
        if has("capital expenditures") or has("purchases of property") or has("additions to property"):
            return "PaymentsToAcquirePropertyPlantAndEquipment"
        if (
            has("cash paid for", "interest")
            or has("interest expense")
            or has("interest and debt expense")
            or has("interest paid")
            or has("finance costs")
            or has("borrowing costs")
            or (has("interest") and has("expense"))
            or (
                ("interest" in t or "finance cost" in t or "borrowing cost" in t)
                and ("expense" in t or "cost" in t or "paid" in t or "charge" in t)
                and ("income" not in t and "rate" not in t and "accrued" not in t)
            )
        ):
            return "InterestExpense"
        if has("depreciation") or has("amortization"):
            return "DepreciationDepletionAndAmortization"
        if has("research and development"):
            return "ResearchAndDevelopmentExpense"
        if has("selling general and administrative") or has("marketing general and administrative"):
            return "SellingGeneralAndAdministrativeExpense"
        if has("retained earnings"):
            return "RetainedEarningsAccumulatedDeficit"
        if has("noncontrolling", "interest") or has("minority", "interest"):
            return "NoncontrollingInterest"
        if has("redeemable", "noncontrolling", "interest"):
            return "RedeemableNoncontrollingInterest"
        if has("weighted average shares") and has("basic"):
            return "WeightedAverageNumberOfSharesOutstandingBasic"
        if has("basic") and has("in shares"):
            return "WeightedAverageNumberOfSharesOutstandingBasic"
        if has("dividends paid"):
            return "DividendsPaid"
        return None

    def _build_data_by_year_from_direct_csv(self, csv_path, start_year=None, end_year=None):
        if not isinstance(csv_path, str) or not csv_path or not os.path.exists(csv_path):
            return {}

        data_by_year = {}
        with open(csv_path, 'r', encoding='utf-8-sig', newline='') as fh:
            reader = csv.DictReader(fh)
            fields = reader.fieldnames or []
            year_cols = []
            for c in fields:
                m = re.fullmatch(r"\d{4}", str(c or "").strip())
                if m:
                    y = int(m.group(0))
                    if start_year is not None and y < int(start_year):
                        continue
                    if end_year is not None and y > int(end_year):
                        continue
                    year_cols.append(y)
            year_cols = sorted(set(year_cols), reverse=True)
            if not year_cols:
                return {}

            stock_like_concepts = {
                'Assets',
                'Liabilities',
                'StockholdersEquity',
                'AssetsCurrent',
                'LiabilitiesCurrent',
                'AccountsReceivableNetCurrent',
                'AccountsPayableCurrent',
                'InventoryNet',
                'CashAndCashEquivalentsAtCarryingValue',
                'WeightedAverageNumberOfSharesOutstandingBasic',
                'SharesBasic',
            }

            for row in reader:
                raw_label = row.get('Line Item')
                concept = self._line_item_to_concept(raw_label)
                normalized_name, _, _ = self._normalize_sec_label(raw_label)
                if not concept:
                    continue
                for y in year_cols:
                    raw = row.get(str(y))
                    val = self._safe_float(raw)
                    if val is None:
                        continue
                    slot = data_by_year.setdefault(int(y), {})
                    concept_overwritten = False
                    if concept not in slot or slot.get(concept) is None:
                        slot[concept] = val
                    else:
                        cur = self._safe_float(slot.get(concept))
                        # Prefer economically plausible stock values:
                        # - prefer positive over negative when available
                        # - then prefer larger absolute magnitude
                        if cur is not None and concept in stock_like_concepts:
                            choose_new = False
                            if cur < 0 <= val:
                                choose_new = True
                            elif (cur >= 0 and val >= 0 and abs(val) > abs(cur)):
                                choose_new = True
                            elif (cur < 0 and val < 0 and abs(val) < abs(cur)):
                                choose_new = True
                            if choose_new:
                                slot[concept] = val
                                concept_overwritten = True
                    modern = self.MODERN_METRIC_ALIASES.get(concept)
                    concept_val = slot.get(concept)
                    if modern and ((modern not in slot or slot.get(modern) is None) or concept_overwritten):
                        slot[modern] = concept_val
                    if normalized_name and ((normalized_name not in slot or slot.get(normalized_name) is None) or concept_overwritten):
                        slot[normalized_name] = concept_val

        if not data_by_year:
            return {}
        enriched = self._apply_accounting_hierarchy(data_by_year)
        for _, row in (enriched or {}).items():
            if not isinstance(row, dict):
                continue
            for legacy_key, modern_key in self.MODERN_METRIC_ALIASES.items():
                if row.get(modern_key) is None and row.get(legacy_key) is not None:
                    row[modern_key] = row.get(legacy_key)
        return enriched

    @staticmethod
    def _is_non_consolidated_context(frame):
        txt = str(frame or '').lower()
        if not txt:
            return False
        blocked = (
            'segment', 'axis', 'member', 'geograph', 'product', 'service',
            'adjustment', 'elimination'
        )
        return any(k in txt for k in blocked)

    @staticmethod
    def _is_consolidated_context(frame):
        txt = str(frame or '').lower()
        if not txt:
            return False
        preferred = ('consolidated', 'dei:entitycommonstocksharesoutstanding')
        return any(k in txt for k in preferred)

    def _build_exact_normalization_map(self):
        """
        Layer 2: exact SEC label -> normalized financial label.
        """
        return {
            'Revenues': 'Revenues',
            'RevenueFromContractWithCustomerExcludingAssessedTax': 'Revenues',
            'SalesRevenueNet': 'Revenues',
            'Net sales': 'Revenues',
            'CostOfRevenue': 'Cost of Revenue',
            'CostOfGoodsAndServicesSold': 'Cost of Revenue',
            'CostOfSales': 'Cost of Revenue',
            'Cost of sales': 'Cost of Revenue',
            'GrossProfit': 'Gross Profit',
            'Gross margin': 'Gross Profit',
            'OperatingIncomeLoss': 'Operating Income',
            'NetIncomeLoss': 'Net Income',
            'ProfitLoss': 'Net Income',
            'Assets': 'Total Assets',
            'AssetsCurrent': 'Current Assets',
            'AssetsNoncurrent': 'Non-current Assets',
            'Liabilities': 'Total Liabilities',
            'LiabilitiesCurrent': 'Current Liabilities',
            'LiabilitiesNoncurrent': 'Non-current Liabilities',
            'StockholdersEquity': 'Total Equity',
            'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest': 'Total Equity',
            'NoncontrollingInterest': 'Total Equity',
            'MinorityInterest': 'Total Equity',
            'RedeemableNoncontrollingInterest': 'Total Equity',
            'LiabilitiesAndStockholdersEquity': 'Total Assets',
            'CashAndCashEquivalentsAtCarryingValue': 'Cash and Cash Equivalents',
            'NetCashProvidedByUsedInOperatingActivities': 'Operating Cash Flow',
            'NetCashProvidedByUsedInInvestingActivities': 'Investing Cash Flow',
            'NetCashProvidedByUsedInFinancingActivities': 'Financing Cash Flow',
            'PaymentsToAcquirePropertyPlantAndEquipment': 'Capital Expenditures',
            'DepreciationDepletionAndAmortization': 'Depreciation and Amortization',
            'InterestExpense': 'Interest Expense',
            'IncomeTaxExpenseBenefit': 'Income Tax Expense',
            # Strict hierarchy outputs
            'TotalCurrentAssets_Hierarchy': 'Current Assets',
            'TotalCurrentLiabilities_Hierarchy': 'Current Liabilities',
            'TotalCurrentAssets_Parent': 'Current Assets',
            'TotalCurrentLiabilities_Parent': 'Current Liabilities',
            'OperatingExpenses_Hierarchy': 'Operating Expenses',
            'AccountsReceivableNetCurrent': 'Accounts Receivable',
            'AccountsReceivableNetCurrent_Hierarchy': 'Accounts Receivable',
            'AccountsPayableCurrent': 'Accounts Payable',
            'AccountsPayableCurrent_Hierarchy': 'Accounts Payable',
            'InventoryNet_Hierarchy': 'Inventory',
            'CashAndCashEquivalents_Hierarchy': 'Cash and Cash Equivalents',
            # Strict override bucket for accumulated/comprehensive items
            'AccumulatedComprehensiveEquity': 'Accumulated Comprehensive Equity',
        }

    def _build_keyword_normalization_rules(self):
        """
        Layer 2: keyword rules -> normalized label.
        Rule format: (required_keywords, normalized_label)
        """
        return [
            (['cost', 'sales'], 'Cost of Revenue'),
            (['cost', 'revenue'], 'Cost of Revenue'),
            (['cost', 'goods'], 'Cost of Revenue'),
            (['deferred', 'revenue'], 'Deferred Revenue'),
            (['net', 'revenue'], 'Revenues'),
            (['net', 'sales'], 'Revenues'),
            (['revenue'], 'Revenues'),
            (['sales'], 'Revenues'),
            (['gross', 'profit'], 'Gross Profit'),
            (['operating', 'income'], 'Operating Income'),
            (['operating', 'profit'], 'Operating Income'),
            (['operating', 'expenses'], 'Operating Expenses'),
            (['net', 'income'], 'Net Income'),
            (['profit', 'loss'], 'Net Income'),
            (['accounts', 'receivable', 'net'], 'Accounts Receivable'),
            (['accounts', 'payable'], 'Accounts Payable'),
            (['inventory'], 'Inventory'),
            (['total', 'assets'], 'Total Assets'),
            (['current', 'assets'], 'Current Assets'),
            (['total', 'liabilities'], 'Total Liabilities'),
            (['current', 'liabilities'], 'Current Liabilities'),
            (['total', 'equity'], 'Total Equity'),
            (['stockholders', 'equity'], 'Total Equity'),
            (['cash', 'equivalents'], 'Cash and Cash Equivalents'),
            (['cash', 'operating', 'activities'], 'Operating Cash Flow'),
            (['cash', 'investing', 'activities'], 'Investing Cash Flow'),
            (['cash', 'financing', 'activities'], 'Financing Cash Flow'),
            (['payments', 'acquire', 'property', 'plant', 'equipment'], 'Capital Expenditures'),
            (['depreciation'], 'Depreciation and Amortization'),
            (['amortization'], 'Depreciation and Amortization'),
            (['interest', 'expense'], 'Interest Expense'),
            (['income', 'tax'], 'Income Tax Expense'),
        ]

    def _sanitize_year_row_for_integrity(self, row):
        """Fix ambiguous legacy labels that can corrupt accounting ratios."""
        if not isinstance(row, dict):
            return row
        out = dict(row)

        def _num(v):
            try:
                if v is None:
                    return None
                return float(v)
            except Exception:
                return None

        rev = _num(out.get('Revenue'))
        revs = _num(out.get('Revenues'))
        sales_net = _num(out.get('SalesRevenueNet'))
        cogs = _num(out.get('CostOfRevenue'))
        if rev is not None and cogs is not None and revs is not None:
            if abs(rev - cogs) <= 1e-9 and abs(revs - rev) > 1e-9:
                out['Revenue_Legacy_Conflicted'] = out.get('Revenue')
                out.pop('Revenue', None)
        # If revenues are polluted and equal COGS, repair from better anchors.
        if revs is not None and cogs is not None and abs(revs - cogs) <= 1e-9:
            repaired = None
            if sales_net is not None and abs(sales_net - cogs) > 1e-9:
                repaired = sales_net
            else:
                gp = _num(out.get('GrossProfit')) or _num(out.get('GrossProfit_Hierarchy'))
                if gp is not None:
                    repaired = gp + cogs
            if repaired is not None:
                out['Revenues_Legacy_Conflicted'] = out.get('Revenues')
                out['Revenues'] = repaired

        assets = _num(out.get('Assets')) or _num(out.get('TotalAssets'))
        total_assets_label = _num(out.get('Total Assets'))
        if assets is not None and total_assets_label is not None and assets > 0:
            if (total_assets_label / assets) < 0.80:
                out['Total Assets (Legacy Current Assets)'] = out.get('Total Assets')
                out.pop('Total Assets', None)

        liab = _num(out.get('Liabilities')) or _num(out.get('TotalLiabilities'))
        total_liab_label = _num(out.get('Total Liabilities'))
        if liab is not None and total_liab_label is not None and liab > 0:
            if (total_liab_label / liab) < 0.80:
                out['Total Liabilities (Legacy Current Liabilities)'] = out.get('Total Liabilities')
                out.pop('Total Liabilities', None)
        equity_parent = _num(out.get('StockholdersEquity'))
        equity_incl = _num(out.get('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'))
        equity_total = _num(out.get('TotalEquity'))
        nci_components = [
            _num(out.get('NoncontrollingInterest')),
            _num(out.get('MinorityInterest')),
            _num(out.get('RedeemableNoncontrollingInterest')),
            _num(out.get('MinorityInterestInConsolidatedSubsidiaries')),
        ]
        nci_sum = sum(v for v in nci_components if v is not None) if any(v is not None for v in nci_components) else None
        equity = equity_parent or equity_incl or equity_total
        if equity is not None and nci_sum is not None and (equity_incl is None):
            # Build total equity including NCI for balance-sheet identity checks.
            out['StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'] = equity + nci_sum
            if _num(out.get('TotalEquity')) is None:
                out['TotalEquity'] = equity + nci_sum
            equity = equity + nci_sum
        # Guardrail: some filings expose control totals under liabilities aliases.
        # If liabilities ~= assets while equity is materially non-zero, recalculate
        # liabilities from the accounting identity.
        if assets is not None and liab is not None and equity not in (None, 0):
            same_as_assets = abs(liab - assets) <= max(1.0, abs(assets) * 0.005)
            if same_as_assets:
                rebuilt_liab = assets - equity
                if rebuilt_liab >= 0:
                    out['Liabilities'] = rebuilt_liab
                    liab = rebuilt_liab
        # Enforce identity consistency when all three fields exist.
        if assets is not None and liab is not None and equity is not None:
            gap = assets - (liab + equity)
            if abs(gap) > max(1.0, abs(assets) * 0.01):
                rebuilt_liab = assets - equity
                if rebuilt_liab >= 0:
                    out['Liabilities'] = rebuilt_liab
                    liab = rebuilt_liab

        # Keep aliases synchronized to a single trusted anchor value to avoid
        # cross-sheet inconsistencies (e.g., TotalLiabilities accidentally equal to Assets).
        if liab is not None:
            raw_total_liab = _num(out.get('TotalLiabilities'))
            if raw_total_liab is not None and assets is not None:
                same_as_assets = abs(raw_total_liab - assets) <= max(1.0, abs(assets) * 0.005)
                if same_as_assets and abs(liab - raw_total_liab) > max(1.0, abs(liab) * 0.001):
                    out['TotalLiabilities_Legacy_Conflicted'] = out.get('TotalLiabilities')
            out['TotalLiabilities'] = liab
            if out.get('Total Liabilities') is not None:
                out['Total Liabilities'] = liab

        # Pick best equity anchor and synchronize aliases.
        if assets is not None and liab is not None:
            target_equity = assets - liab
        else:
            target_equity = None
        equity_candidates = [
            _num(out.get('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest')),
            _num(out.get('StockholdersEquity')),
            _num(out.get('TotalEquity')),
            _num(out.get('Total Equity')),
        ]
        equity_candidates = [v for v in equity_candidates if v is not None]
        if equity_candidates:
            if target_equity is not None:
                eq_best = min(equity_candidates, key=lambda ev: abs(ev - target_equity))
            else:
                eq_best = equity_candidates[0]
            if target_equity is not None and abs(eq_best - target_equity) > max(1.0, abs(target_equity) * 0.02):
                # If all aliases are inconsistent, enforce accounting identity.
                eq_best = target_equity
            out['StockholdersEquity'] = eq_best
            out['TotalEquity'] = eq_best
            if out.get('Total Equity') is not None:
                out['Total Equity'] = eq_best
            # Keep comprehensive-equity alias when available but prevent corruption.
            if out.get('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest') is not None:
                out['StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'] = eq_best

        cash = _num(out.get('CashAndCashEquivalents')) or _num(out.get('CashAndCashEquivalentsAtCarryingValue'))
        curr_assets = _num(out.get('AssetsCurrent')) or _num(out.get('CurrentAssets'))
        h_curr_assets = _num(out.get('TotalCurrentAssets_Hierarchy'))
        if curr_assets is not None:
            low_vs_cash = (cash is not None and curr_assets < cash)
            low_vs_hierarchy = (h_curr_assets is not None and curr_assets < (0.5 * h_curr_assets))
            if low_vs_cash or low_vs_hierarchy:
                # Integrity rule: keep SEC parent as source of truth.
                # Do not overwrite AssetsCurrent with hierarchy sum.
                out['AssetsCurrent_ParentPreferred'] = curr_assets
                if h_curr_assets is not None:
                    out['AssetsCurrent_HierarchyConflict'] = h_curr_assets

        curr_liab = _num(out.get('LiabilitiesCurrent')) or _num(out.get('CurrentLiabilities'))
        h_curr_liab = _num(out.get('TotalCurrentLiabilities_Hierarchy'))
        if curr_liab is not None and h_curr_liab is not None and curr_liab < (0.6 * h_curr_liab):
            out['LiabilitiesCurrent_ParentPreferred'] = curr_liab
            out['LiabilitiesCurrent_HierarchyConflict'] = h_curr_liab
        # Resolve alias collision: keep a single trusted current-liabilities anchor.
        liab_parent = _num(out.get('LiabilitiesCurrent'))
        liab_alias = _num(out.get('CurrentLiabilities'))
        if liab_parent is not None and liab_alias is not None:
            delta = abs(liab_parent - liab_alias) / max(abs(liab_parent), 1.0)
            if delta > 0.20:
                out['CurrentLiabilities_Legacy_Conflicted'] = out.get('CurrentLiabilities')
                out['CurrentLiabilities'] = liab_parent
        elif liab_parent is not None and liab_alias is None:
            out['CurrentLiabilities'] = liab_parent
        elif liab_alias is not None and liab_parent is None:
            out['LiabilitiesCurrent'] = liab_alias

        # Normalize share-related facts to million-share scale at source.
        share_keys = (
            'WeightedAverageNumberOfSharesOutstandingBasic',
            'WeightedAverageNumberOfDilutedSharesOutstanding',
            'WeightedAverageNumberOfShareOutstandingBasicAndDiluted',
            'CommonStockSharesOutstanding',
            'EntityCommonStockSharesOutstanding',
            'SharesBasic',
            'SharesOutstanding',
            'Basic (shares)',
            'Diluted (shares)',
            'Basic (in shares)',
            'Diluted (in shares)',
        )
        for sk in share_keys:
            sv = _num(out.get(sk))
            if sv is None:
                continue
            out[sk] = self._normalize_shares_to_million(sv)

        return out

    def _sanitize_data_by_year_for_integrity(self, data_by_year):
        cleaned = {}
        for year, row in (data_by_year or {}).items():
            cleaned[year] = self._sanitize_year_row_for_integrity(row or {})

        # Temporal consistency repair for key annual anchors:
        # detect simple one-year forward shifts that create duplicated consecutive values
        # (common when a filing-year mapping leaks into period-year slots).
        try:
            years_sorted = sorted([y for y in cleaned.keys() if isinstance(y, int)])
            if len(years_sorted) >= 4:
                key_aliases = {
                    'Revenues': ['Revenues', 'Revenue', 'SalesRevenueNet', 'Total revenues', 'total revenues'],
                    'CostOfRevenue': ['CostOfRevenue', 'Cost of revenue', 'costofrevenue'],
                    'NetIncomeLoss': ['NetIncomeLoss', 'Net income', 'netincomeloss'],
                    'Assets': ['Assets', 'TotalAssets', 'assets'],
                    'Liabilities': ['Liabilities', 'TotalLiabilities', 'liabilities'],
                    'StockholdersEquity': [
                        'StockholdersEquity',
                        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                        'stockholdersequity',
                    ],
                }

                def _num(v):
                    return float(v) if isinstance(v, (int, float)) else None

                def _get_ci(row, aliases):
                    if not isinstance(row, dict):
                        return None, None
                    norm = {re.sub(r'[^a-z0-9]+', '', str(k).lower()): k for k in row.keys()}
                    for a in aliases:
                        if a in row and isinstance(row.get(a), (int, float)):
                            return float(row.get(a)), a
                        nk = re.sub(r'[^a-z0-9]+', '', str(a).lower())
                        rk = norm.get(nk)
                        if rk is not None and isinstance(row.get(rk), (int, float)):
                            return float(row.get(rk)), rk
                    return None, None

                for cname, aliases in key_aliases.items():
                    vals = []
                    keys = []
                    for yy in years_sorted:
                        v, rk = _get_ci(cleaned.get(yy, {}) or {}, aliases)
                        vals.append(v)
                        keys.append(rk)
                    # Pattern to repair:
                    # y[i] has value of y[i+1], y[i+1] == y[i+2], and previous year differs.
                    for i in range(1, len(years_sorted) - 2):
                        v_prev = vals[i - 1]
                        v0 = vals[i]
                        v1 = vals[i + 1]
                        v2 = vals[i + 2]
                        if None in (v0, v1, v2):
                            continue
                        if abs(v1 - v2) > max(1.0, abs(v1) * 1e-9):
                            continue
                        if abs(v0 - v1) > max(1.0, abs(v1) * 1e-9):
                            continue
                        # Keep if prior differs enough; indicates a likely one-year duplication shift.
                        if v_prev is None or abs(v_prev - v0) <= max(1.0, abs(v0) * 0.02):
                            continue
                        # Apply conservative repair: set middle duplicated year to None.
                        y_fix = years_sorted[i + 1]
                        k_fix = keys[i + 1]
                        if k_fix and isinstance((cleaned.get(y_fix, {}) or {}).get(k_fix), (int, float)):
                            cleaned[y_fix][k_fix] = None
        except Exception:
            pass
        return cleaned

    def _collect_data_quality_warnings(self, ticker, data_by_year):
        """
        Collect explicit warnings for years where core balance-sheet anchors are missing.
        This prevents silent NaN propagation in balance-derived ratios.
        """
        warnings = []
        for year in sorted((data_by_year or {}).keys()):
            row = (data_by_year or {}).get(year, {}) or {}
            assets = self._safe_float(row.get('Assets')) or self._safe_float(row.get('TotalAssets')) or self._safe_float(row.get('Total Assets'))
            liab = self._safe_float(row.get('Liabilities')) or self._safe_float(row.get('TotalLiabilities')) or self._safe_float(row.get('Total Liabilities'))
            equity = (
                self._safe_float(row.get('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'))
                or self._safe_float(row.get('StockholdersEquity'))
                or self._safe_float(row.get('TotalEquity'))
                or self._safe_float(row.get('Total Equity'))
            )
            if assets is None or liab is None or equity is None:
                missing = []
                if assets is None:
                    missing.append('Assets')
                if liab is None:
                    missing.append('Liabilities')
                if equity is None:
                    missing.append('Equity')
                warnings.append({
                    'year': int(year),
                    'code': 'MISSING_BALANCE_SHEET_ANCHOR',
                    'message': f'{ticker} {year}: Balance Sheet anchor(s) unavailable ({", ".join(missing)}). Balance-based ratios may be N/A.',
                    'missing_fields': missing,
                })
        return warnings

    def _normalize_tokens(self, label):
        """
        Convert SEC/CamelCase label into lowercase token set for keyword matching.
        """
        if not label:
            return set()
        s = str(label)
        out = []
        cur = ''
        for ch in s:
            if ch.isupper() and cur and not cur[-1].isupper():
                out.append(cur)
                cur = ch
            elif ch in ['_', '-', ' ', '.', '/']:
                if cur:
                    out.append(cur)
                    cur = ''
            else:
                cur += ch
        if cur:
            out.append(cur)
        return {w.lower() for w in out if w}

    def _normalize_sec_label(self, raw_label):
        """
        Returns: (normalized_label, match_type, match_rule)
        """
        tokens = self._normalize_tokens(raw_label)
        # Liabilities + Equity combined control total; keep it isolated and do not force it into equity.
        if ('liabilities' in tokens) and ('equity' in tokens):
            return raw_label, 'identity', 'combined_liabilities_equity_control_total'
        # Strict rule: only comprehensive-equity items are forced into the equity bucket.
        # Do NOT route all "accumulated" concepts (e.g. accumulated depreciation) to equity.
        if ('comprehensive' in tokens) and (('accumulated' in tokens) or ('income' in tokens)):
            return 'AccumulatedComprehensiveEquity', 'strict_equity_override', 'accumulated_or_comprehensive'

        exact_map = self._build_exact_normalization_map()
        if raw_label in exact_map:
            return exact_map[raw_label], 'exact', raw_label

        for required, normalized_name in self._build_keyword_normalization_rules():
            if set(required).issubset(tokens):
                return normalized_name, 'keyword', ','.join(required)

        return raw_label, 'identity', ''

    def _classify_normalized_name(self, normalized_name):
        """
        Layer 3 classification bucket.
        """
        mapping = {
            'Revenue': 'Income Statement',
            'Cost of Revenue': 'Income Statement',
            'Gross Profit': 'Income Statement',
            'Operating Income': 'Income Statement',
            'Net Income': 'Income Statement',
            'Interest Expense': 'Income Statement',
            'Income Tax Expense': 'Income Statement',
            'Depreciation and Amortization': 'Income Statement',
            'Operating Expenses': 'Income Statement',
            'Total Assets': 'Balance Sheet',
            'Current Assets': 'Balance Sheet',
            'Non-current Assets': 'Balance Sheet',
            'Total Liabilities': 'Balance Sheet',
            'Current Liabilities': 'Balance Sheet',
            'Non-current Liabilities': 'Balance Sheet',
            'Total Equity': 'Balance Sheet',
            'Cash and Cash Equivalents': 'Balance Sheet',
            'Accounts Receivable': 'Balance Sheet',
            'Accounts Payable': 'Balance Sheet',
            'Inventory': 'Balance Sheet',
            'Accumulated Comprehensive Equity': 'Balance Sheet',
            'Operating Cash Flow': 'Cash Flow',
            'Investing Cash Flow': 'Cash Flow',
            'Financing Cash Flow': 'Cash Flow',
            'Capital Expenditures': 'Cash Flow',
        }
        if normalized_name in mapping:
            return mapping[normalized_name]

        t = {w.lower() for w in str(normalized_name).replace('-', ' ').split()}
        if {'revenue', 'income', 'expense', 'profit'} & t:
            return 'Income Statement'
        if {'assets', 'liabilities', 'equity', 'cash'} & t:
            return 'Balance Sheet'
        if {'operating', 'investing', 'financing', 'capex', 'expenditures'} & t:
            return 'Cash Flow'
        return 'Unclassified'

    def _build_data_layers(self, data_by_year):
        """
        Build 3 layers from SEC yearly data:
        1) raw labels
        2) normalized labels
        3) classified financial model labels
        """
        years = sorted([y for y in data_by_year.keys() if isinstance(y, int)])
        all_raw_labels = sorted({k for y in years for k in data_by_year.get(y, {}).keys()})

        layer1_by_year = {}
        for y in years:
            layer1_by_year[y] = dict(data_by_year.get(y, {}))

        layer2_by_year = {y: {} for y in years}
        layer3_by_year = {y: {} for y in years}
        layer2_candidates = {y: {} for y in years}
        layer3_candidates = {y: {} for y in years}

        label_rows = []

        def _match_priority(match_type):
            return {
                'strict_equity_override': 300,
                'golden_forced': 280,
                'exact': 250,
                'keyword': 150,
                'identity': 50,
            }.get(match_type, 0)

        def _raw_label_priority(normalized_name, raw_label):
            rl = str(raw_label or '').lower()
            score = 0
            if rl.endswith('_hierarchy') and normalized_name not in ('Accounts Receivable', 'Accounts Payable'):
                score += 80
            if normalized_name == 'Total Assets' and raw_label == 'Assets':
                score += 120
            if normalized_name == 'Current Assets' and raw_label == 'AssetsCurrent':
                score += 120
            if normalized_name == 'Total Liabilities' and raw_label == 'Liabilities':
                score += 120
            if normalized_name == 'Current Liabilities' and raw_label == 'LiabilitiesCurrent':
                score += 120
            if normalized_name == 'Total Equity' and raw_label == 'StockholdersEquity':
                score += 120
            if normalized_name == 'Accounts Receivable':
                if raw_label == 'AccountsReceivableNetCurrent':
                    score += 180
                if raw_label == 'AccountsReceivableNetCurrent_Hierarchy':
                    score += 120
                if 'sale' in rl or 'allowance' in rl or 'beforeallowance' in rl:
                    score -= 120
            if normalized_name == 'Accounts Payable' and raw_label in ('AccountsPayableCurrent_Hierarchy', 'AccountsPayableCurrent'):
                score += 120
            return score

        def _pick_best(candidates):
            if not candidates:
                return None
            best = None
            best_key = None
            for c in candidates:
                val = c.get('value')
                abs_val = abs(val) if isinstance(val, (int, float)) else 0.0
                k = (c.get('priority', 0), abs_val)
                if best is None or k > best_key:
                    best = c
                    best_key = k
            return best

        for raw_label in all_raw_labels:
            normalized_name, match_type, match_rule = self._normalize_sec_label(raw_label)
            category = self._classify_normalized_name(normalized_name)
            golden_entry = self.GOLDEN_TAG_CLASSIFICATION.get(raw_label)
            if golden_entry:
                normalized_name, category = golden_entry
                if match_type == 'identity':
                    match_type = 'golden_forced'
                    match_rule = 'golden_tag_contract'
            label_rows.append({
                'raw_label': raw_label,
                'normalized_name': normalized_name,
                'category': category,
                'match_type': match_type,
                'match_rule': match_rule
            })

            for y in years:
                value = data_by_year.get(y, {}).get(raw_label)
                if value is None:
                    continue
                priority = _match_priority(match_type) + _raw_label_priority(normalized_name, raw_label)
                layer2_candidates[y].setdefault(normalized_name, []).append({
                    'value': value,
                    'priority': priority,
                    'raw_label': raw_label,
                    'match_type': match_type,
                })
                key3 = f"{category}::{normalized_name}"
                layer3_candidates[y].setdefault(key3, []).append({
                    'value': value,
                    'priority': priority,
                    'raw_label': raw_label,
                    'match_type': match_type,
                })

        for y in years:
            for normalized_name, cands in layer2_candidates[y].items():
                best = _pick_best(cands)
                if best is not None:
                    layer2_by_year[y][normalized_name] = best.get('value')
            for key3, cands in layer3_candidates[y].items():
                best = _pick_best(cands)
                if best is not None:
                    layer3_by_year[y][key3] = best.get('value')

            # Balance sheet consistency fallback when one core aggregate is missing.
            ta = layer2_by_year[y].get('Total Assets')
            tl = layer2_by_year[y].get('Total Liabilities')
            te = layer2_by_year[y].get('Total Equity')
            if isinstance(ta, (int, float)) and isinstance(te, (int, float)) and not isinstance(tl, (int, float)):
                layer2_by_year[y]['Total Liabilities'] = ta - te
                layer3_by_year[y]['Balance Sheet::Total Liabilities'] = ta - te
            elif isinstance(ta, (int, float)) and isinstance(tl, (int, float)) and not isinstance(te, (int, float)):
                layer2_by_year[y]['Total Equity'] = ta - tl
                layer3_by_year[y]['Balance Sheet::Total Equity'] = ta - tl

            # Golden tags must always map into a financial statement category (never Unclassified path).
            for raw_tag, (norm_name, forced_category) in self.GOLDEN_TAG_CLASSIFICATION.items():
                val = layer1_by_year.get(y, {}).get(raw_tag)
                if not isinstance(val, (int, float)):
                    continue
                if norm_name not in layer2_by_year[y]:
                    layer2_by_year[y][norm_name] = val
                layer3_key = f"{forced_category}::{norm_name}"
                if layer3_key not in layer3_by_year[y]:
                    layer3_by_year[y][layer3_key] = val

        return {
            'label_rows': label_rows,
            'layer1_by_year': layer1_by_year,
            'layer2_by_year': layer2_by_year,
            'layer3_by_year': layer3_by_year
        }

    def _flatten_layer_payload_to_year_map(self, payload, value_key):
        years = {}
        periods = (payload or {}).get('periods', {}) or {}
        for y, period_obj in periods.items():
            try:
                yi = int(y)
            except Exception:
                continue
            fields = (period_obj or {}).get(value_key, {}) or {}
            row = {}
            for k, v in fields.items():
                if isinstance(v, dict):
                    vv = v.get('value')
                    if isinstance(vv, (int, float)):
                        lk = str(k).lower()
                        unit_name = str(v.get('unit') or '').lower()
                        if any(tok in lk for tok in ('market:market_cap', 'yahoo:market_cap', 'market:enterprise_value', 'yahoo:enterprise_value', 'market:total_debt', 'yahoo:total_debt')):
                            # System base is million-USD for statement compatibility.
                            # Convert only when source unit is raw USD or value is clearly absolute.
                            if 'usd' in unit_name:
                                vv = self._normalize_million_value(vv)
                            elif abs(float(vv)) > 1_000_000_000:
                                vv = float(vv) / 1_000_000.0
                        row[k] = vv
            years[yi] = row
        return years

    def _is_missing_numeric(self, row, key):
        if not isinstance(row, dict):
            return True
        return self._safe_float(row.get(key)) is None

    def _normalize_backfilled_fact_value(self, sec_concept, alias_key, value):
        """
        Normalize SEC backfilled values into system scale.
        Monetary facts -> million USD.
        Share/ratio/per-share facts keep their natural scale.
        """
        fv = self._safe_float(value)
        if fv is None:
            return None
        concept_l = str(sec_concept or '').lower()
        alias_l = str(alias_key or '').lower()
        if any(tok in concept_l for tok in ('pershare', 'per_share', 'ratio')):
            return fv
        if any(tok in alias_l for tok in ('share', 'per share', 'ratio')):
            return fv
        # SEC payload/companyconcept monetary facts are reported in absolute USD.
        # Convert to million USD for system consistency with statement rows.
        if concept_l.startswith('us-gaap:') or concept_l.startswith('ifrs-full:') or concept_l.startswith('dei:'):
            return fv / 1_000_000.0
        return self._normalize_million_value(fv)

    def _backfill_layer1_from_sec_payload(self, layer1_by_year, sec_payload):
        """
        Backfill critical accounting concepts from SEC companyfacts payload
        into layer1 rows when direct statement CSV lacks specific tags.
        """
        out = {}
        for y, row in (layer1_by_year or {}).items():
            out[int(y)] = dict(row or {})

        if not isinstance(sec_payload, dict):
            return out
        periods = (sec_payload.get('periods') or {})
        if not isinstance(periods, dict):
            return out

        concept_aliases = {
            'us-gaap:Assets': ['Assets', 'TotalAssets'],
            'us-gaap:Liabilities': ['Liabilities', 'TotalLiabilities'],
            'us-gaap:StockholdersEquity': ['StockholdersEquity', 'TotalEquity'],
            'us-gaap:MinorityInterest': ['NoncontrollingInterest', 'MinorityInterest'],
            'us-gaap:NoncontrollingInterest': ['NoncontrollingInterest', 'MinorityInterest'],
            'us-gaap:RedeemableNoncontrollingInterest': ['RedeemableNoncontrollingInterest'],
            'us-gaap:Revenues': ['Revenues'],
            'us-gaap:SalesRevenueNet': ['SalesRevenueNet', 'Revenues'],
            'us-gaap:NetIncomeLoss': ['NetIncomeLoss', 'NetIncome'],
            'us-gaap:OperatingIncomeLoss': ['OperatingIncomeLoss', 'OperatingIncome'],
            'us-gaap:AssetsCurrent': ['AssetsCurrent', 'CurrentAssets'],
            'us-gaap:LiabilitiesCurrent': ['LiabilitiesCurrent', 'CurrentLiabilities'],
            'us-gaap:CashAndCashEquivalentsAtCarryingValue': ['CashAndCashEquivalents', 'CashAndCashEquivalentsAtCarryingValue'],
            'us-gaap:AccountsReceivableNetCurrent': ['AccountsReceivable', 'AccountsReceivableNetCurrent'],
            'us-gaap:InventoryNet': ['Inventory', 'InventoryNet'],
            'us-gaap:AccountsPayableCurrent': ['AccountsPayable', 'AccountsPayableCurrent'],
            'us-gaap:InterestExpense': ['InterestExpense', 'InterestExpense_Hierarchy'],
            'us-gaap:InterestExpenseNonoperating': ['InterestExpenseNonoperating'],
            'us-gaap:InterestAndDebtExpense': ['InterestAndDebtExpense'],
            'us-gaap:InterestExpenseDebt': ['InterestExpenseDebt'],
            'us-gaap:InterestPaidNet': ['InterestPaidNet'],
            'us-gaap:InterestCostsIncurred': ['InterestCostsIncurred'],
            'us-gaap:DepreciationAndAmortization': ['DepreciationDepletionAndAmortization', 'DepreciationAmortization'],
            'us-gaap:CostOfGoodsAndServicesSold': ['CostOfRevenue'],
            'us-gaap:NetCashProvidedByUsedInOperatingActivities': ['NetCashProvidedByUsedInOperatingActivities'],
            'us-gaap:PaymentsToAcquirePropertyPlantAndEquipment': ['PaymentsToAcquirePropertyPlantAndEquipment'],
            'us-gaap:EarningsPerShareBasic': ['EarningsPerShareBasic'],
            'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic': ['WeightedAverageNumberOfSharesOutstandingBasic'],
            # Bank-specific concepts
            'us-gaap:LoansReceivableNet': ['LoansReceivable', 'NetLoans'],
            'us-gaap:LoansAndLeasesReceivableNetReportedAmount': ['LoansReceivable'],
            'us-gaap:LoansHeldForSale': ['LoansHeldForSale'],
            'us-gaap:LoansAndLeasesReceivable': ['LoansReceivable'],
            'us-gaap:Deposits': ['Deposits', 'DepositLiabilities'],
            'us-gaap:DepositLiabilities': ['Deposits', 'DepositLiabilities'],
            'us-gaap:InterestBearingDepositsInBanks': ['Deposits'],
            'us-gaap:NoninterestBearingDeposits': ['Deposits'],
            'us-gaap:NetInterestIncome': ['NetInterestIncome'],
            'us-gaap:InterestIncomeOperating': ['InterestIncomeOperating'],
            'us-gaap:InterestAndDividendIncomeOperating': ['InterestAndDividendIncomeOperating'],
            'us-gaap:ProvisionForCreditLosses': ['ProvisionForCreditLosses'],
            'us-gaap:ProvisionForLoanLosses': ['ProvisionForCreditLosses'],
            'us-gaap:AllowanceForCreditLosses': ['AllowanceForCreditLosses'],
            'us-gaap:CommonEquityTier1Capital': ['CommonEquityTier1Capital', 'CET1Capital'],
            'us-gaap:CommonEquityTier1CapitalRatio': ['CommonEquityTier1CapitalRatio', 'CET1CapitalRatio'],
            # Insurance-specific concepts
            'us-gaap:PremiumsEarned': ['PremiumsEarned'],
            'us-gaap:PremiumsEarnedNet': ['PremiumsEarned', 'PremiumsEarnedNet'],
            'us-gaap:DirectPremiumsEarned': ['PremiumsEarned'],
            'us-gaap:AssumedPremiumsEarned': ['PremiumsEarned'],
            'us-gaap:PolicyholderBenefitsAndClaimsIncurredNet': ['PolicyholderBenefits'],
            'us-gaap:PolicyholderBenefits': ['PolicyholderBenefits'],
            'us-gaap:PolicyClaimsAndBenefits': ['PolicyClaims'],
            'us-gaap:IncurredClaimsPropertyCasualtyAndLiability': ['PolicyClaims'],
            'us-gaap:BenefitsLossesAndExpenses': ['PolicyClaims'],
            'us-gaap:DeferredPolicyAcquisitionCosts': ['DeferredAcquisitionCosts'],
            'us-gaap:ReinsuranceRecoverables': ['ReinsuranceRecoverables'],
            'us-gaap:LossAndLossAdjustmentExpense': ['PolicyClaims'],
        }

        for y_str, pobj in periods.items():
            try:
                yi = int(y_str)
            except Exception:
                continue
            facts = (pobj or {}).get('facts', {}) or {}
            if not isinstance(facts, dict):
                continue
            row = out.setdefault(yi, {})
            for sec_concept, aliases in concept_aliases.items():
                fobj = facts.get(sec_concept)
                if not isinstance(fobj, dict):
                    continue
                val = self._safe_float(fobj.get('value'))
                if val is None:
                    continue
                for key in aliases:
                    if self._is_missing_numeric(row, key):
                        norm_val = self._normalize_backfilled_fact_value(sec_concept, key, val)
                        if norm_val is None:
                            continue
                        if 'interestexpense' in str(sec_concept).lower() or key in ('InterestExpense', 'InterestExpense_Hierarchy', 'InterestExpenseDebt', 'InterestAndDebtExpense'):
                            norm_val = abs(norm_val)
                        row[key] = norm_val

            # Semantic fallback: pick any interest-expense-like SEC fact if canonical one missing.
            if row.get('InterestExpense') is None:
                best_val = None
                for sec_concept, fobj in facts.items():
                    if not isinstance(fobj, dict):
                        continue
                    concept_l = str(sec_concept).lower()
                    if 'interest' not in concept_l:
                        continue
                    if any(x in concept_l for x in ['income', 'rate', 'accrued', 'benefit', 'penalt']):
                        continue
                    if not any(x in concept_l for x in ['expense', 'debt', 'paid', 'cost']):
                        continue
                    v = self._safe_float(fobj.get('value'))
                    if v is None:
                        continue
                    best_val = self._normalize_million_value(abs(v))
                    # Prefer explicit expense tags first.
                    if 'expense' in concept_l:
                        break
                if best_val is not None:
                    row['InterestExpense'] = best_val
                    row['InterestExpense_Hierarchy'] = best_val
        return out

    def _needs_companyconcept_backfill(self, layer1_by_year, start_year, end_year):
        """
        Run expensive companyconcept backfill only when core annual anchors are missing.
        This preserves institutional quality while avoiding unnecessary network-heavy calls.
        """
        rows = layer1_by_year or {}
        years = range(int(start_year), int(end_year) + 1)

        def _has_any(row, keys):
            for k in keys:
                v = (row or {}).get(k)
                if isinstance(v, (int, float)):
                    return True
            return False

        # Trigger only for ratio-critical anchors, not for every secondary concept.
        revenue_keys = ['Revenues', 'Revenue', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax']
        net_income_keys = ['NetIncomeLoss', 'NetIncome', 'ProfitLoss']
        assets_keys = ['Assets', 'TotalAssets']
        liabilities_keys = ['Liabilities', 'TotalLiabilities']
        equity_keys = ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'TotalEquity']

        severe_missing_years = 0
        total_years = 0
        for y in years:
            row = rows.get(int(y), {}) or {}
            total_years += 1

            # Very sparse year row can justify backfill, but keep threshold conservative.
            if len(row) < 4:
                severe_missing_years += 1
                continue

            has_revenue = _has_any(row, revenue_keys)
            has_net_income = _has_any(row, net_income_keys)
            has_assets = _has_any(row, assets_keys)
            has_capital_side = _has_any(row, liabilities_keys) or _has_any(row, equity_keys)

            # A year is "severely missing" only when multiple anchor blocks are absent.
            missing_blocks = sum([
                0 if has_revenue else 1,
                0 if has_net_income else 1,
                0 if has_assets else 1,
                0 if has_capital_side else 1,
            ])
            if missing_blocks >= 2:
                severe_missing_years += 1

        # Run expensive backfill only when the gap is meaningful across period.
        if total_years <= 1:
            return severe_missing_years >= 1
        return severe_missing_years >= 2

    def _fetch_companyconcept_series(self, cik_padded, taxonomy, concept, start_year, end_year):
        """
        Direct SEC companyconcept fetch:
        /api/xbrl/companyconcept/CIK##########/{taxonomy}/{concept}.json
        Returns {year: value} using latest filed annual fact per year.
        """
        key = (str(cik_padded), str(taxonomy), str(concept), int(start_year), int(end_year))
        if key in self._companyconcept_cache:
            return dict(self._companyconcept_cache[key])

        url = f"{self.base_url}/api/xbrl/companyconcept/CIK{cik_padded}/{taxonomy}/{concept}.json"
        req_headers = dict(self.headers or {})
        req_headers.pop('Host', None)
        try:
            r = requests.get(url, headers=req_headers, timeout=30)
            if r.status_code != 200:
                self._companyconcept_cache[key] = {}
                return {}
            payload = r.json() or {}
        except Exception:
            self._companyconcept_cache[key] = {}
            return {}

        out = {}
        units = (payload.get('units') or {})
        for unit_name, entries in units.items():
            for e in (entries or []):
                form = str(e.get('form') or '').upper()
                if not (form.startswith('10-K') or form.startswith('20-F')):
                    continue
                end_date = str(e.get('end') or '')
                if len(end_date) < 4:
                    continue
                try:
                    y = int(end_date[:4])
                except Exception:
                    continue
                if y < int(start_year) or y > int(end_year):
                    continue
                frame = str(e.get('frame') or '')
                if frame and 'Q' in frame:
                    continue

                val = self._safe_float(e.get('val'))
                if val is None:
                    continue
                filed = str(e.get('filed') or '')
                prev = out.get(y)
                if prev is None or filed > prev.get('filed', ''):
                    out[y] = {
                        'value': val,
                        'filed': filed,
                        'unit': unit_name,
                        'tag': f'{taxonomy}:{concept}',
                    }

        result = {y: v.get('value') for y, v in out.items() if isinstance(v.get('value'), (int, float))}
        self._companyconcept_cache[key] = dict(result)
        return result

    def _fetch_companyconcept_entries(self, cik_padded, taxonomy, concept, start_year, end_year):
        """
        Direct SEC companyconcept fetch with full entry metadata per year.
        Returns:
            {
                year: {
                    value, filed, unit, tag, start, end, fy, fp, form
                }
            }
        """
        key = (str(cik_padded), str(taxonomy), str(concept), int(start_year), int(end_year))
        if key in self._companyconcept_entries_cache:
            return dict(self._companyconcept_entries_cache[key])

        url = f"{self.base_url}/api/xbrl/companyconcept/CIK{cik_padded}/{taxonomy}/{concept}.json"
        req_headers = dict(self.headers or {})
        req_headers.pop('Host', None)
        try:
            r = requests.get(url, headers=req_headers, timeout=30)
            if r.status_code != 200:
                self._companyconcept_entries_cache[key] = {}
                return {}
            payload = r.json() or {}
        except Exception:
            self._companyconcept_entries_cache[key] = {}
            return {}

        by_year = {}
        units = (payload.get('units') or {})
        for unit_name, entries in units.items():
            for e in (entries or []):
                form = str(e.get('form') or '').upper()
                if not (form.startswith('10-K') or form.startswith('20-F')):
                    continue
                end_date = str(e.get('end') or '')
                if len(end_date) < 4:
                    continue
                try:
                    y = int(end_date[:4])
                except Exception:
                    continue
                if y < int(start_year) or y > int(end_year):
                    continue
                frame = str(e.get('frame') or '')
                if frame and 'Q' in frame:
                    continue
                val = self._safe_float(e.get('val'))
                if val is None:
                    continue
                filed = str(e.get('filed') or '')
                prev = by_year.get(y)
                if prev is None or filed > str(prev.get('filed') or ''):
                    by_year[y] = {
                        'value': val,
                        'filed': filed,
                        'unit': unit_name,
                        'tag': f'{taxonomy}:{concept}',
                        'start': str(e.get('start') or ''),
                        'end': end_date,
                        'fy': str(e.get('fy') or ''),
                        'fp': str(e.get('fp') or ''),
                        'form': form,
                    }

        self._companyconcept_entries_cache[key] = dict(by_year)
        return dict(by_year)

    def _resolve_interest_expense_fact(self, row, year, is_bank=False):
        """
        Resolve interest expense with strict priority:
        1) Current-year row labels
        2) SEC companyconcept (same year only, annual forms only)
        Returns a structured trace object.
        """
        result = {
            'status': 'NOT_COMPUTABLE',
            'reason': 'MISSING_SEC_CONCEPT',
            'value': None,
            'source': 'MISSING_SEC_INTEREST_EXPENSE',
            'reliability': 0,
            'tag': None,
            'unit': None,
            'filed': None,
            'period': str(year),
            'missing_inputs': [],
            'is_estimated': False,
        }

        def _norm_positive(v):
            fv = self._safe_float(v)
            if fv is None:
                return None
            fv = abs(fv)
            # Interest expense in system outputs is expected in million-USD scale.
            # Values like 247000000 are almost always raw USD and must be normalized.
            if fv >= 1_000_000.0:
                fv = fv / 1_000_000.0
            nv = self._normalize_million_value(fv)
            if nv is None or nv <= 0:
                return None
            return float(nv)

        candidates = []
        row_aliases = [
            'InterestExpense',
            'InterestExpense_Hierarchy',
            'InterestExpenseDebt',
            'InterestAndDebtExpense',
            'InterestExpenseAndDebtExpense',
            'InterestCostsIncurred',
            'InterestPaidNet',
            'Cash paid for interest',
        ]
        if is_bank:
            row_aliases = [
                'InterestExpenseDeposits',
                'InterestExpenseDebt',
                'InterestExpense',
                'InterestAndDebtExpense',
                'InterestExpenseAndDebtExpense',
                'InterestPaidNet',
                'Cash paid for interest',
            ]
        for idx, lbl in enumerate(row_aliases):
            vv = _norm_positive((row or {}).get(lbl))
            if vv is None:
                continue
            # Explicit expense tags are higher confidence than paid-cash proxies.
            rel = 100 if ('expense' in str(lbl).lower() or 'cost' in str(lbl).lower()) else 85
            score = rel - (idx * 0.2)
            candidates.append({
                'score': score,
                'value': vv,
                'source': f"SEC_DIRECT_INTEREST_EXPENSE[row:{lbl}]",
                'reliability': rel,
                'tag': lbl,
                'unit': 'USDm',
                'filed': None,
                'is_estimated': lbl in ('InterestPaidNet', 'Cash paid for interest'),
            })

        concepts = [
            ('us-gaap', 'InterestExpense'),
            ('us-gaap', 'InterestExpenseNonoperating'),
            ('us-gaap', 'InterestAndDebtExpense'),
            ('us-gaap', 'InterestExpenseAndDebtExpense'),
            ('us-gaap', 'InterestExpenseDebt'),
            ('us-gaap', 'InterestCostsIncurred'),
            ('us-gaap', 'InterestPaidNet'),
        ]
        if is_bank:
            concepts = [
                ('us-gaap', 'InterestExpenseDeposits'),
                ('us-gaap', 'InterestExpenseDebt'),
                ('us-gaap', 'InterestAndDebtExpense'),
                ('us-gaap', 'InterestExpenseAndDebtExpense'),
                ('us-gaap', 'InterestExpense'),
                ('us-gaap', 'InterestPaidNet'),
            ]

        ctx_cik = str(self._active_cik_padded or '').strip()
        ctx_start = int(self._active_start_year or year)
        ctx_end = int(self._active_end_year or year)
        if ctx_cik:
            for idx, (taxonomy, concept) in enumerate(concepts):
                by_year = self._fetch_companyconcept_entries(
                    cik_padded=ctx_cik,
                    taxonomy=taxonomy,
                    concept=concept,
                    start_year=ctx_start,
                    end_year=ctx_end,
                )
                entry = by_year.get(int(year))
                if not isinstance(entry, dict):
                    continue
                raw_val = self._safe_float(entry.get('value'))
                if raw_val is None:
                    continue
                norm_val = self._normalize_backfilled_fact_value(
                    f'{taxonomy}:{concept}',
                    'InterestExpense',
                    abs(raw_val),
                )
                norm_val = _norm_positive(norm_val)
                if norm_val is None:
                    continue
                unit_name = str(entry.get('unit') or '')
                rel = max(90, 99 - idx)
                score = rel - (idx * 0.2)
                candidates.append({
                    'score': score,
                    'value': norm_val,
                    'source': 'SEC_COMPANYCONCEPT_INTEREST_EXPENSE',
                    'reliability': rel,
                    'tag': f'{taxonomy}:{concept}',
                    'unit': unit_name,
                    'filed': str(entry.get('filed') or ''),
                    'is_estimated': False,
                })

        if not candidates:
            result['missing_inputs'] = [f'{t}:{c}' for t, c in concepts]
            return result

        # Outlier guard: if one candidate is orders of magnitude above the rest,
        # prefer the sane expense concept rather than carrying a raw-USD slip.
        positive_vals = [float(c.get('value') or 0.0) for c in candidates if self._safe_float(c.get('value')) not in (None, 0)]
        if len(positive_vals) >= 2:
            min_val = min(v for v in positive_vals if v > 0)
            if min_val > 0:
                sane_candidates = []
                for c in candidates:
                    cv = self._safe_float(c.get('value'))
                    if cv is None or cv <= 0:
                        continue
                    if (cv / min_val) > 1000.0:
                        continue
                    sane_candidates.append(c)
                if sane_candidates:
                    candidates = sane_candidates

        best = max(candidates, key=lambda c: float(c.get('score') or 0.0))
        result.update({
            'status': 'COMPUTED',
            'reason': None,
            'value': float(best.get('value') or 0.0),
            'source': str(best.get('source') or ''),
            'reliability': int(best.get('reliability') or 0),
            'tag': best.get('tag'),
            'unit': best.get('unit'),
            'filed': best.get('filed'),
            'is_estimated': bool(best.get('is_estimated')),
            'missing_inputs': [],
        })
        return result

    def _apply_smart_companyconcept_backfill(self, layer1_by_year, cik_padded, start_year, end_year):
        """
        Smart backfill using direct companyconcept pulls.
        - Pulls critical concepts directly from SEC.
        - Chooses the best concept family by cross-year coverage.
        - Fills canonical aliases used by ratio engine.
        """
        out = {int(y): dict(row or {}) for y, row in (layer1_by_year or {}).items()}
        years = [y for y in range(int(start_year), int(end_year) + 1)]
        for y in years:
            out.setdefault(y, {})

        concept_families = {
            'revenue': {
                'aliases': ['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax'],
                'candidates': [
                    ('us-gaap', 'Revenues'),
                    ('us-gaap', 'SalesRevenueNet'),
                    ('us-gaap', 'RevenueFromContractWithCustomerExcludingAssessedTax'),
                    ('us-gaap', 'OperatingRevenue'),
                ],
            },
            'net_income': {
                'aliases': ['NetIncomeLoss', 'NetIncome', 'ProfitLoss'],
                'candidates': [
                    ('us-gaap', 'NetIncomeLoss'),
                    ('us-gaap', 'ProfitLoss'),
                ],
            },
            'operating_income': {
                'aliases': ['OperatingIncomeLoss', 'OperatingIncome'],
                'candidates': [
                    ('us-gaap', 'OperatingIncomeLoss'),
                    ('us-gaap', 'IncomeLossFromOperations'),
                ],
            },
            'gross_profit': {
                'aliases': ['GrossProfit', 'Gross Profit'],
                'candidates': [
                    ('us-gaap', 'GrossProfit'),
                ],
            },
            'cogs': {
                'aliases': ['CostOfRevenue', 'CostOfGoodsAndServicesSold', 'CostOfSales'],
                'candidates': [
                    ('us-gaap', 'CostOfRevenue'),
                    ('us-gaap', 'CostOfGoodsAndServicesSold'),
                    ('us-gaap', 'CostOfSales'),
                    ('us-gaap', 'CostsAndExpenses'),
                    ('us-gaap', 'OtherCostAndExpenseOperating'),
                ],
            },
            'assets': {
                'aliases': ['Assets', 'TotalAssets'],
                'candidates': [('us-gaap', 'Assets')],
                'force_overwrite': True,
            },
            'liabilities': {
                'aliases': ['Liabilities', 'TotalLiabilities'],
                'candidates': [('us-gaap', 'Liabilities')],
                'force_overwrite': True,
            },
            'equity': {
                'aliases': ['StockholdersEquity', 'TotalEquity'],
                'candidates': [
                    ('us-gaap', 'StockholdersEquity'),
                    ('us-gaap', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'),
                ],
                'force_overwrite': True,
            },
            'current_assets': {
                'aliases': ['AssetsCurrent', 'CurrentAssets'],
                'candidates': [('us-gaap', 'AssetsCurrent')],
                'force_overwrite': True,
            },
            'current_liabilities': {
                'aliases': ['LiabilitiesCurrent', 'CurrentLiabilities'],
                'candidates': [('us-gaap', 'LiabilitiesCurrent')],
                'force_overwrite': True,
            },
            'ocf': {
                'aliases': ['NetCashProvidedByUsedInOperatingActivities', 'OperatingCashFlow'],
                'candidates': [('us-gaap', 'NetCashProvidedByUsedInOperatingActivities')],
            },
            'capex': {
                'aliases': ['PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpenditures'],
                'candidates': [('us-gaap', 'PaymentsToAcquirePropertyPlantAndEquipment')],
            },
            'debt_current': {
                'aliases': [
                    'DebtCurrent',
                    'ShortTermBorrowings',
                    'CommercialPaper',
                    'LongTermDebtCurrent',
                    'CurrentPortionOfLongTermDebt',
                ],
                'candidates': [
                    ('us-gaap', 'DebtCurrent'),
                    ('us-gaap', 'ShortTermBorrowings'),
                    ('us-gaap', 'CommercialPaper'),
                    ('us-gaap', 'LongTermDebtCurrent'),
                    ('us-gaap', 'CurrentPortionOfLongTermDebt'),
                ],
                'force_overwrite': True,
            },
            'debt_noncurrent': {
                'aliases': [
                    'LongTermDebtNoncurrent',
                    'DebtNoncurrent',
                    'LongTermDebt',
                    'LongTermDebtAndCapitalLeaseObligation',
                    'LongTermDebtAndCapitalLeaseObligations',
                ],
                'candidates': [
                    ('us-gaap', 'LongTermDebtNoncurrent'),
                    ('us-gaap', 'DebtNoncurrent'),
                    ('us-gaap', 'LongTermDebt'),
                    ('us-gaap', 'LongTermDebtAndCapitalLeaseObligation'),
                    ('us-gaap', 'LongTermDebtAndCapitalLeaseObligations'),
                ],
                'force_overwrite': True,
            },
            'interest_expense': {
                'aliases': ['InterestExpense', 'InterestExpense_Hierarchy', 'InterestPaidNet'],
                'candidates': [
                    ('us-gaap', 'InterestExpense'),
                    ('us-gaap', 'InterestExpenseNonoperating'),
                    ('us-gaap', 'InterestAndDebtExpense'),
                    ('us-gaap', 'InterestExpenseDebt'),
                    ('us-gaap', 'InterestExpenseDeposits'),
                    ('us-gaap', 'InterestPaidNet'),
                ],
            },
            'loans': {
                'aliases': ['LoansReceivable', 'NetLoans', 'LoansAndLeasesReceivableNetReportedAmount'],
                'candidates': [
                    ('us-gaap', 'LoansAndLeasesReceivableNetReportedAmount'),
                    ('us-gaap', 'LoansReceivableNet'),
                    ('us-gaap', 'LoansAndLeasesReceivable'),
                    ('us-gaap', 'FinancingReceivableExcludingAccruedInterestBeforeAllowanceForCreditLoss'),
                    ('us-gaap', 'FinancingReceivableExcludingAccruedInterestAfterAllowanceForCreditLoss'),
                ],
            },
            'deposits': {
                'aliases': ['Deposits', 'DepositLiabilities'],
                'candidates': [
                    ('us-gaap', 'Deposits'),
                    ('us-gaap', 'DepositLiabilities'),
                    ('us-gaap', 'InterestBearingDepositsInBanks'),
                    ('us-gaap', 'NoninterestBearingDeposits'),
                ],
            },
            'net_interest_income': {
                'aliases': ['NetInterestIncome', 'InterestIncomeNet', 'InterestIncomeOperating', 'InterestAndDividendIncomeOperating'],
                'candidates': [
                    ('us-gaap', 'NetInterestIncome'),
                    ('us-gaap', 'InterestIncomeOperating'),
                    ('us-gaap', 'InterestAndDividendIncomeOperating'),
                    ('us-gaap', 'InterestAndFeeIncomeLoansAndLeases'),
                    ('us-gaap', 'InterestIncomeExpenseAfterProvisionForLoanLoss'),
                ],
            },
            'cet1': {
                'aliases': ['CommonEquityTier1Capital', 'CET1Capital', 'CommonEquityTier1CapitalRatio', 'CET1CapitalRatio'],
                'candidates': [
                    ('us-gaap', 'CommonEquityTier1Capital'),
                    ('us-gaap', 'CommonEquityTier1CapitalRatio'),
                    ('us-gaap', 'Tier1Capital'),
                ],
            },
            'premiums': {
                'aliases': ['PremiumsEarned'],
                'candidates': [
                    ('us-gaap', 'PremiumsEarned'),
                    ('us-gaap', 'PremiumsEarnedNet'),
                    ('us-gaap', 'DirectPremiumsEarned'),
                    ('us-gaap', 'AssumedPremiumsEarned'),
                ],
            },
            'claims': {
                'aliases': ['PolicyholderBenefits', 'PolicyClaims', 'LossAndLossAdjustmentExpense'],
                'candidates': [
                    ('us-gaap', 'PolicyholderBenefitsAndClaimsIncurredNet'),
                    ('us-gaap', 'PolicyholderBenefits'),
                    ('us-gaap', 'PolicyClaimsAndBenefits'),
                    ('us-gaap', 'IncurredClaimsPropertyCasualtyAndLiability'),
                    ('us-gaap', 'BenefitsLossesAndExpenses'),
                    ('us-gaap', 'LossAndLossAdjustmentExpense'),
                ],
            },
            'dividends': {
                'aliases': [
                    'DividendsPaid',
                    'PaymentsOfDividends',
                    'PaymentsOfDividendsCommonStock',
                    'DividendsCommonStockCash',
                    'Dividends',
                ],
                'candidates': [
                    ('us-gaap', 'PaymentsOfDividends'),
                    ('us-gaap', 'PaymentsOfDividendsCommonStock'),
                    ('us-gaap', 'DividendsCash'),
                    ('us-gaap', 'DividendsCommonStockCash'),
                    ('us-gaap', 'Dividends'),
                ],
            },
        }

        def score_series(series):
            present = [y for y in years if isinstance(series.get(y), (int, float))]
            if not present:
                return (-1, -1)
            return (len(present), -min(present))

        for family in concept_families.values():
            candidates = []
            for taxonomy, concept in family['candidates']:
                ys = self._fetch_companyconcept_series(
                    cik_padded=str(cik_padded),
                    taxonomy=taxonomy,
                    concept=concept,
                    start_year=int(start_year),
                    end_year=int(end_year),
                )
                if ys:
                    candidates.append((f'{taxonomy}:{concept}', ys))
            if not candidates:
                continue

            candidates.sort(key=lambda kv: score_series(kv[1]), reverse=True)
            best_series = candidates[0][1]

            for y in years:
                v = best_series.get(y)
                if v is None:
                    continue
                row = out.setdefault(y, {})
                force_overwrite = bool(family.get('force_overwrite'))
                for alias in family['aliases']:
                    if force_overwrite or self._is_missing_numeric(row, alias):
                        norm_v = self._normalize_backfilled_fact_value('us-gaap:smart_backfill', alias, v)
                        if norm_v is not None:
                            row[alias] = norm_v

        # Rebuild missing balance anchors directly from accounting identity when
        # SEC does not expose `Liabilities` as a standalone concept for a filer.
        for y in years:
            row = out.setdefault(y, {})
            assets = self._safe_float(row.get('Assets')) or self._safe_float(row.get('TotalAssets'))
            liabilities = self._safe_float(row.get('Liabilities')) or self._safe_float(row.get('TotalLiabilities'))
            equity = (
                self._safe_float(row.get('StockholdersEquity'))
                or self._safe_float(row.get('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'))
                or self._safe_float(row.get('TotalEquity'))
            )

            if liabilities is None and assets is not None and equity is not None:
                implied_liab = assets - equity
                if implied_liab >= 0:
                    row['Liabilities'] = implied_liab
                    row['TotalLiabilities'] = implied_liab
                    liabilities = implied_liab

            if assets is None and liabilities is not None and equity is not None:
                implied_assets = liabilities + equity
                if implied_assets >= 0:
                    row['Assets'] = implied_assets
                    row['TotalAssets'] = implied_assets
                    assets = implied_assets

            if equity is None and assets is not None and liabilities is not None:
                implied_equity = assets - liabilities
                row['StockholdersEquity'] = implied_equity
                row['TotalEquity'] = implied_equity
                equity = implied_equity

            # If liabilities are present but materially inconsistent with A = L + E,
            # keep the original and synchronize to the identity-consistent anchor.
            if assets is not None and liabilities is not None and equity is not None:
                implied_liab = assets - equity
                if implied_liab >= 0 and abs(liabilities - implied_liab) > max(1.0, abs(assets) * 0.02):
                    row['Liabilities_Legacy_Conflicted'] = row.get('Liabilities')
                    row['Liabilities'] = implied_liab
                    row['TotalLiabilities'] = implied_liab

        return out

    def _build_source_layers(self, cik, ticker, start_year, end_year, fiscal_period_end_by_year=None):
        """
        Build independent source layers (SEC fixed as layer1 externally, market/macro as layer2/layer3).
        This never mutates SEC layer1 extraction logic.
        """
        out = {'MARKET': {}, 'MACRO': {}, 'YAHOO': {}, 'payloads': {}, 'extra': {}}
        if build_layer_registry is None:
            return out

        try:
            registry = build_layer_registry(
                user_agent=self.headers.get('User-Agent', 'Financial-Analysis-System/1.0'),
                polygon_api_key=os.getenv('POLYGON_API_KEY'),
                fred_api_key=os.getenv('FRED_API_KEY'),
                output_dir='exports/institutional',
                enable_sec=True,
                enable_market=bool(os.getenv('POLYGON_API_KEY')),
                enable_macro=True,
                enable_yahoo=True,
            )
        except Exception:
            return out

        try:
            sec_layer = registry.get('SEC')
            if sec_layer is not None:
                sec_payload = sec_layer.fetch(
                    cik=cik,
                    start_year=int(start_year),
                    end_year=int(end_year),
                ).payload
                out['payloads']['SEC'] = sec_payload
        except Exception:
            pass

        try:
            market_layer = registry.get('MARKET')
            if market_layer is not None:
                market_payload = market_layer.fetch(
                    ticker=ticker,
                    start_year=int(start_year),
                    end_year=int(end_year),
                ).payload
                out['payloads']['MARKET'] = market_payload
                out['MARKET'] = self._flatten_layer_payload_to_year_map(market_payload, 'fields')
        except Exception:
            out['MARKET'] = {}

        try:
            macro_layer = registry.get('MACRO')
            if macro_layer is not None:
                macro_payload = macro_layer.fetch(
                    start_year=int(start_year),
                    end_year=int(end_year),
                ).payload
                out['payloads']['MACRO'] = macro_payload
                out['MACRO'] = self._flatten_layer_payload_to_year_map(macro_payload, 'fields')
        except Exception:
            out['MACRO'] = {}

        try:
            yahoo_layer = registry.get('YAHOO')
            if yahoo_layer is not None:
                yahoo_payload = yahoo_layer.fetch(
                    ticker=ticker,
                    start_year=int(start_year),
                    end_year=int(end_year),
                    fiscal_period_end_by_year=fiscal_period_end_by_year,
                ).payload
                out['payloads']['YAHOO'] = yahoo_payload
                out['YAHOO'] = self._flatten_layer_payload_to_year_map(yahoo_payload, 'fields')
        except Exception:
            out['YAHOO'] = {}

        # If Polygon market layer is unavailable, map Yahoo fields into market namespace.
        if (not out.get('MARKET')) and out.get('YAHOO'):
            mapped_market = {}
            alias = {
                'yahoo:price': 'market:price',
                'yahoo:market_cap': 'market:market_cap',
                'yahoo:enterprise_value': 'market:enterprise_value',
                'yahoo:total_debt': 'market:total_debt',
                'yahoo:beta': 'market:beta',
                'yahoo:dividend_yield': 'market:dividend_yield',
                'yahoo:volume': 'market:volume',
                'yahoo:total_return': 'market:total_return',
                'yahoo:shares_outstanding': 'market:shares_outstanding',
                'yahoo:pe_ratio': 'market:pe_ratio',
                'yahoo:pb_ratio': 'market:pb_ratio',
                'yahoo:trailing_pe': 'market:pe_ratio',
                'yahoo:price_to_book': 'market:pb_ratio',
            }
            for y, row in (out.get('YAHOO') or {}).items():
                mrow = {}
                for sk, sv in (row or {}).items():
                    mk = alias.get(sk)
                    if mk:
                        mrow[mk] = sv
                mapped_market[y] = mrow
            out['MARKET'] = mapped_market

            ypayload = out.get('payloads', {}).get('YAHOO', {}) or {}
            mperiods = {}
            for y, pobj in (ypayload.get('periods', {}) or {}).items():
                yfields = (pobj or {}).get('fields', {}) or {}
                mfields = {}
                for sk, mk in alias.items():
                    if sk in yfields:
                        mfields[mk] = yfields[sk]
                mperiods[y] = {'fields': mfields}
            out['payloads']['MARKET'] = {
                'layer': 'MARKET',
                'status': 'OK',
                'source': 'Yahoo-Mapped',
                'source_endpoint': 'yfinance',
                'periods': mperiods,
                'data_source_trace': {'mapped_from': 'YAHOO', 'ticker': ticker},
                'timestamp': datetime.now().isoformat(),
                'dependency_map': {'depends_on': ['YAHOO'], 'provides': ['MARKET']},
            }

        # Optional placeholder extra layers (future sources), controlled by env.
        # Example: EXTRA_DATA_LAYERS=NEWS,SATELLITE,SUPPLYCHAIN
        extra_names = [
            str(x).strip().upper()
            for x in str(os.getenv('EXTRA_DATA_LAYERS', '')).split(',')
            if str(x).strip()
        ]
        for name in extra_names:
            out['extra'][name] = {}
            out['payloads'][name] = {
                'layer': name,
                'status': 'DISABLED_NO_SOURCE',
                'periods': {str(y): {'fields': {}} for y in range(int(start_year), int(end_year) + 1)},
                'source': name,
                'timestamp': datetime.now().isoformat(),
            }

        return out

    def _build_layer_definitions(self, data_layers, start_year, end_year):
        """
        Build explicit layer definitions with fields and yearly coverage.
        This is the canonical layer catalog used by UI and diagnostics.
        """
        year_keys = [int(y) for y in range(int(start_year), int(end_year) + 1)]

        def _summarize_layer(layer_key, title, source, purpose):
            by_year = (data_layers.get(layer_key) or {})
            fields_by_year = {}
            all_fields = set()
            coverage = {}
            for y in year_keys:
                row = by_year.get(y, {}) if isinstance(by_year, dict) else {}
                if not isinstance(row, dict):
                    row = {}
                keys = sorted([str(k) for k in row.keys()])
                fields_by_year[str(y)] = keys
                all_fields.update(keys)
                coverage[str(y)] = len(keys)
            return {
                'key': layer_key,
                'title': title,
                'source': source,
                'purpose': purpose,
                'years': [str(y) for y in year_keys],
                'fields': sorted(all_fields),
                'fields_by_year': fields_by_year,
                'coverage_by_year': coverage,
            }

        layer_defs = [
            _summarize_layer(
                'layer1_by_year',
                'Layer 1 - SEC XBRL (EDGAR)',
                'SEC',
                'Official accounting statements and filing facts.',
            ),
            _summarize_layer(
                'layer2_by_year',
                'Layer 2 - Market (Polygon)',
                'MARKET',
                'Market trading and valuation fields used by ratios.',
            ),
            _summarize_layer(
                'layer3_by_year',
                'Layer 3 - Macro (FRED)',
                'MACRO',
                'Macroeconomic indicators for strategic and hybrid ratios.',
            ),
            _summarize_layer(
                'layer4_by_year',
                'Layer 4 - Yahoo (yfinance)',
                'YAHOO',
                'Real-time market data, estimates, and analyst metrics.',
            ),
        ]

        for extra_name, extra_map in (data_layers.get('extra_layers_by_year') or {}).items():
            ek = f'extra::{extra_name}'
            data_layers[ek] = extra_map
            layer_defs.append(
                _summarize_layer(
                    ek,
                    f'Layer - {extra_name}',
                    extra_name,
                    f'Additional source layer for {extra_name}.',
                )
            )

        return layer_defs

    def _enrich_layer2_parent_totals(self, layer2_by_year):
        enriched = {}
        for year, row in (layer2_by_year or {}).items():
            r = dict(row or {})

            def g(*keys):
                for k in keys:
                    v = self._safe_float(r.get(k))
                    if v is not None:
                        return v
                return None

            def sum_existing(keys):
                vals = [self._safe_float(r.get(k)) for k in keys]
                vals = [v for v in vals if v is not None]
                return sum(vals) if vals else None

            if g('Current Assets') is None:
                ca = sum_existing(['Cash and Cash Equivalents', 'Accounts Receivable', 'Inventory', 'Other Current Assets'])
                if ca is not None:
                    r['Current Assets'] = ca

            if g('Current Liabilities') is None:
                cl = sum_existing(['Accounts Payable', 'Accrued Liabilities', 'Short-term Debt', 'Other Current Liabilities'])
                if cl is not None:
                    r['Current Liabilities'] = cl

            if g('Total Assets') is None:
                ca = g('Current Assets')
                nca = g('Non-current Assets')
                ta = (ca + nca) if (ca is not None and nca is not None) else None
                if ta is None and g('Total Liabilities') is not None and g('Total Equity') is not None:
                    ta = g('Total Liabilities') + g('Total Equity')
                if ta is not None:
                    r['Total Assets'] = ta

            if g('Total Liabilities') is None and g('Total Assets') is not None and g('Total Equity') is not None:
                r['Total Liabilities'] = g('Total Assets') - g('Total Equity')

            if g('Total Equity') is None and g('Total Assets') is not None and g('Total Liabilities') is not None:
                r['Total Equity'] = g('Total Assets') - g('Total Liabilities')

            rev_for_gp = g('Revenues', 'Revenue')
            if g('Gross Profit') is None and rev_for_gp is not None and g('Cost of Revenue') is not None:
                r['Gross Profit'] = rev_for_gp - g('Cost of Revenue')

            enriched[year] = r
        return enriched

    def _inject_market_ratios_from_layers(self, financial_ratios, data_by_year, data_layers):
        """
        Fill market-dependent ratios (P/E, P/B) from MARKET/YAHOO layer data
        so UI ratio table uses merged layer outputs instead of leaving N/A.
        """
        ratios = financial_ratios or {}
        l2 = (data_layers or {}).get('layer2_by_year', {}) or {}
        l3 = (data_layers or {}).get('layer3_by_year', {}) or {}
        l4 = (data_layers or {}).get('layer4_by_year', {}) or {}
        rows = data_by_year or {}

        def _num(v):
            try:
                if v is None:
                    return None
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    return None
                return fv
            except Exception:
                return None

        def _pick_market(year, key_market, key_yahoo):
            v = _num((l2.get(year, {}) or {}).get(key_market))
            if v is not None:
                return v
            return _num((l4.get(year, {}) or {}).get(key_yahoo))

        def _pick_market_million(year, key_market, key_yahoo, metric_key, reference=None, price=None, shares=None):
            raw_v = _pick_market(year, key_market, key_yahoo)
            return self._normalize_market_scalar(
                raw_v,
                metric_key=metric_key,
                reference=reference,
                price=price,
                shares=shares,
            )

        def _book_value_per_share(equity, shares, price_hint=None, target_ratio=None):
            # Use shared scale guard to avoid persistent x1000/x1e6 mismatches.
            return self._select_per_share_scaled_value(
                numerator=equity,
                shares=shares,
                price_hint=price_hint,
                target_ratio=target_ratio,
            )

        all_years = sorted(set(list(ratios.keys()) + list(rows.keys()) + list(l2.keys()) + list(l4.keys())))
        ticker_ctx = str(getattr(self, '_active_ticker', '') or '').upper()
        manual_split_rules = {
            'NVDA': [
                {'effective_year': 2021, 'factor': 4.0},
                {'effective_year': 2024, 'factor': 10.0},
            ],
        }

        def _effective_split_ratio(year, market_ratio=None):
            actions = manual_split_rules.get(ticker_ctx)
            if actions:
                factor = 1.0
                for action in actions:
                    try:
                        effective_year = int(action.get('effective_year') or 0)
                        split_factor = float(action.get('factor') or 1.0)
                    except Exception:
                        continue
                    if split_factor > 1.0 and int(year) < effective_year:
                        factor *= split_factor
                if factor > 1.0:
                    return factor
            if market_ratio not in (None, 0):
                try:
                    mr = abs(float(market_ratio))
                    if mr > 1.0:
                        return mr
                except Exception:
                    pass
            return 1.0
        allow_market_debt_override = str(os.environ.get('ALLOW_MARKET_DEBT_OVERRIDE', '0')).strip().lower() in ('1', 'true', 'yes')
        market_debt_series = {
            y: _pick_market(y, 'market:total_debt', 'yahoo:total_debt')
            for y in all_years
        }

        def _is_bank_like_row(row_obj):
            if not isinstance(row_obj, dict):
                return False
            for k in (
                'Deposits',
                'DepositLiabilities',
                'NetInterestIncome',
                'InterestIncomeNet',
                'LoansReceivable',
                'LoansAndLeasesReceivableNetReportedAmount',
            ):
                if _num(row_obj.get(k)) is not None:
                    return True
            return False

        # Build a historical bank debt/liabilities anchor from non-interpolated years.
        bank_debt_liab_anchor = None
        _bank_ratio_candidates = []
        for yy in all_years:
            _row = rows.get(yy, {}) or {}
            if not _is_bank_like_row(_row):
                continue
            _liab = _num(_row.get('Liabilities')) or _num(_row.get('TotalLiabilities')) or _num(_row.get('Total Liabilities'))
            _td = _num((ratios.get(yy, {}) or {}).get('total_debt'))
            _src = str((ratios.get(yy, {}) or {}).get('total_debt_source') or '')
            if _liab in (None, 0) or _td in (None, 0):
                if _liab in (None, 0):
                    continue
                _raw_market_td = _num(market_debt_series.get(yy))
                if _raw_market_td in (None, 0):
                    continue
                _norm_market_td = self._normalize_market_scalar(
                    _raw_market_td,
                    metric_key='total_debt',
                    reference=_liab,
                )
                if _norm_market_td in (None, 0):
                    continue
                _rr = abs(_norm_market_td) / max(abs(_liab), 1e-9)
                if 0.02 <= _rr <= 0.80:
                    _bank_ratio_candidates.append(_rr)
                continue
            if 'MARKET_INTERPOLATED_TIME_SERIES' not in _src:
                _rr = abs(_td) / max(abs(_liab), 1e-9)
                if 0.02 <= _rr <= 0.80:
                    _bank_ratio_candidates.append(_rr)
        if _bank_ratio_candidates:
            _bank_ratio_candidates = sorted(_bank_ratio_candidates)
            bank_debt_liab_anchor = _bank_ratio_candidates[len(_bank_ratio_candidates) // 2]

        def _interpolate_series_value(year, series_map):
            if year not in all_years:
                return None
            idx = all_years.index(year)
            prev_val = None
            next_val = None
            for j in range(idx - 1, -1, -1):
                v = _num(series_map.get(all_years[j]))
                if v is not None and v > 0:
                    prev_val = v
                    break
            for j in range(idx + 1, len(all_years)):
                v = _num(series_map.get(all_years[j]))
                if v is not None and v > 0:
                    next_val = v
                    break
            if prev_val is not None and next_val is not None:
                return (prev_val + next_val) / 2.0
            return prev_val if prev_val is not None else next_val

        for year in all_years:
            r = ratios.setdefault(year, {})
            row = rows.get(year, {}) or {}
            bank_like = _is_bank_like_row(row)

            price = _pick_market(year, 'market:price', 'yahoo:price')
            market_cap_hint = _pick_market_million(
                year,
                'market:market_cap',
                'yahoo:market_cap',
                metric_key='market_cap',
            )
            # Hard guard for market price scale slips (e.g., 0.003 instead of 177):
            # use PE*EPS anchor when available.
            pe_hint_local = _pick_market(year, 'market:pe_ratio', 'yahoo:pe_ratio')
            eps_hint_local = (
                _num(row.get('EarningsPerShareBasic'))
                or _num(row.get('EarningsPerShareDiluted'))
                or _num(r.get('eps_basic'))
            )
            if (
                price not in (None, 0)
                and pe_hint_local not in (None, 0)
                and eps_hint_local not in (None, 0)
            ):
                implied_candidates = []
                try:
                    eps_base = float(eps_hint_local)
                    pe_base = abs(float(pe_hint_local))
                    for sf in (1.0, 1_000.0, 1_000_000.0):
                        ip = abs(pe_base * (eps_base * sf))
                        if 1.0 <= ip <= 10_000.0:
                            implied_candidates.append(ip)
                except Exception:
                    implied_candidates = []
                if implied_candidates:
                    implied_price = max(implied_candidates) if float(price) < 0.5 else min(implied_candidates, key=lambda x: abs(x - abs(float(price))))
                    ratio = max(abs(float(price)), implied_price) / max(min(abs(float(price)), implied_price), 1e-9)
                    if float(price) < 0.5 or ratio > 20.0:
                        price = implied_price
            equity_for_debt = _num(row.get('StockholdersEquity')) or _num(row.get('TotalEquity')) or _num(row.get('Total Equity'))
            assets_for_debt = _num(row.get('Assets')) or _num(row.get('TotalAssets')) or _num(row.get('Total Assets'))
            liab_for_debt = _num(row.get('Liabilities')) or _num(row.get('TotalLiabilities')) or _num(row.get('Total Liabilities'))
            shares_ref = (
                _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic'))
                or _num(row.get('SharesBasic'))
                or _num(r.get('shares_outstanding'))
                or _pick_market(year, 'market:shares_outstanding', 'yahoo:shares_outstanding')
            )
            shares_ref = self._normalize_shares_to_million_with_anchor(
                shares_ref,
                price=price,
                market_cap_m=market_cap_hint,
            )
            market_cap = _pick_market_million(
                year,
                'market:market_cap',
                'yahoo:market_cap',
                metric_key='market_cap',
                reference=(assets_for_debt or equity_for_debt),
                price=price,
                shares=shares_ref,
            )
            if market_cap is None and price not in (None, 0) and shares_ref not in (None, 0):
                # Deterministic fallback from historical price * shares.
                # Shares can appear in units, thousands, or millions depending on source/year.
                sh_candidates = [
                    shares_ref,
                    shares_ref * 1_000.0,
                    shares_ref * 1_000_000.0,
                ]
                mcap_candidates = []
                for sh in sh_candidates:
                    if sh in (None, 0):
                        continue
                    mcap_m = (price * sh) / 1_000_000.0
                    if mcap_m == mcap_m and not math.isinf(mcap_m) and 1.0 <= abs(mcap_m) <= 20_000_000.0:
                        mcap_candidates.append(mcap_m)
                if mcap_candidates:
                    ref = assets_for_debt or equity_for_debt
                    if ref not in (None, 0):
                        plausible = [m for m in mcap_candidates if 0.2 <= (abs(m) / abs(ref)) <= 500.0]
                        picks = plausible if plausible else mcap_candidates
                        market_cap = min(picks, key=lambda m: abs((abs(m) / abs(ref)) - 20.0))
                    else:
                        market_cap = max(mcap_candidates, key=lambda m: abs(m))
            # Secondary guard: if market cap is implausibly tiny vs assets and we have
            # annual price + shares, rebuild from annual anchor.
            if market_cap not in (None, 0) and price not in (None, 0) and shares_ref not in (None, 0) and assets_for_debt not in (None, 0):
                try:
                    mcap_anchor = abs(float(price) * float(shares_ref))
                except Exception:
                    mcap_anchor = None
                if mcap_anchor is not None and 1.0 <= mcap_anchor <= 20_000_000.0:
                    tiny_vs_assets = (abs(float(market_cap)) / max(abs(float(assets_for_debt)), 1e-9)) < 0.05
                    huge_gap = abs(float(market_cap) - mcap_anchor) / max(mcap_anchor, 1e-9) > 0.80
                    if tiny_vs_assets or huge_gap:
                        market_cap = mcap_anchor
            # Strong annual valuation anchor using PE*EPS*Shares.
            if pe_hint_local not in (None, 0) and eps_hint_local not in (None, 0) and shares_ref not in (None, 0):
                peeps_cands = []
                try:
                    pe_v = abs(float(pe_hint_local))
                    eps_v = abs(float(eps_hint_local))
                    sh_v = abs(float(shares_ref))
                    for esf in (1.0, 1_000.0, 1_000_000.0):
                        for ssf in (1.0, 1_000.0, 1_000_000.0):
                            mc = pe_v * (eps_v * esf) * (sh_v * ssf)
                            if 1.0 <= mc <= 20_000_000.0:
                                peeps_cands.append(mc)
                except Exception:
                    peeps_cands = []
                if peeps_cands:
                    if assets_for_debt not in (None, 0):
                        plausible = [v for v in peeps_cands if 0.2 <= (v / max(abs(float(assets_for_debt)), 1e-9)) <= 500.0]
                        pool = plausible if plausible else peeps_cands
                        mcap_peeps_anchor = max(pool)
                    else:
                        mcap_peeps_anchor = max(peeps_cands)
                    if market_cap in (None, 0):
                        market_cap = mcap_peeps_anchor
                    else:
                        tiny_vs_assets = (
                            assets_for_debt not in (None, 0)
                            and (abs(float(market_cap)) / max(abs(float(assets_for_debt)), 1e-9)) < 0.05
                        )
                        far_gap = abs(float(market_cap) - mcap_peeps_anchor) / max(mcap_peeps_anchor, 1e-9) > 0.80
                        if tiny_vs_assets or far_gap:
                            market_cap = mcap_peeps_anchor
            # Fundamental anchor: MarketCap ~= PE * NetIncome (both annual).
            # This protects against broken historical price scales.
            pe_hint = _pick_market(year, 'market:pe_ratio', 'yahoo:pe_ratio')
            ni_hint = (
                _num(row.get('NetIncomeLoss'))
                or _num(row.get('NetIncome'))
                or _num(row.get('ProfitLoss'))
            )
            if pe_hint not in (None, 0) and ni_hint not in (None, 0):
                base_anchor = abs(float(pe_hint) * float(ni_hint))
                mcap_pe_anchor = None
                pe_anchors = [
                    base_anchor,
                    base_anchor * 1_000.0,
                    base_anchor * 1_000_000.0,
                ]
                pe_anchors = [v for v in pe_anchors if 1.0 <= v <= 20_000_000.0]
                if pe_anchors:
                    # Prefer anchor consistent with price*shares when available.
                    ps_anchor = None
                    if price not in (None, 0) and shares_ref not in (None, 0):
                        ps_cands = [abs(float(price) * float(shares_ref) * s) for s in (1.0, 1_000.0, 1_000_000.0)]
                        ps_cands = [v for v in ps_cands if 1.0 <= v <= 20_000_000.0]
                        if ps_cands:
                            ps_anchor = max(ps_cands)
                    if ps_anchor not in (None, 0):
                        mcap_pe_anchor = min(pe_anchors, key=lambda v: abs(v - ps_anchor))
                    else:
                        if assets_for_debt not in (None, 0):
                            plausible = [v for v in pe_anchors if 0.2 <= (v / max(abs(float(assets_for_debt)), 1e-9)) <= 500.0]
                            pool = plausible if plausible else pe_anchors
                            mcap_pe_anchor = max(pool)
                        else:
                            mcap_pe_anchor = max(pe_anchors)
                if mcap_pe_anchor is not None:
                    if market_cap in (None, 0):
                        market_cap = mcap_pe_anchor
                    else:
                        # If current cap is implausibly small vs assets or far from PE*NI, trust PE*NI.
                        tiny_vs_assets = (
                            assets_for_debt not in (None, 0)
                            and (abs(float(market_cap)) / max(abs(float(assets_for_debt)), 1e-9)) < 0.05
                        )
                        far_from_pe_anchor = abs(float(market_cap) - mcap_pe_anchor) / max(mcap_pe_anchor, 1e-9) > 0.80
                        if tiny_vs_assets or far_from_pe_anchor:
                            market_cap = mcap_pe_anchor
            market_total_debt = self._normalize_market_scalar(
                market_debt_series.get(year),
                metric_key='total_debt',
                reference=(assets_for_debt or equity_for_debt or liab_for_debt),
            )
            interpolated_debt = None
            if market_total_debt is None:
                interpolated_debt = _interpolate_series_value(year, market_debt_series)
                interpolated_debt = self._normalize_market_scalar(
                    interpolated_debt,
                    metric_key='total_debt',
                    reference=(assets_for_debt or equity_for_debt or liab_for_debt),
                )
            # Split-aware market-cap repair:
            # targeted override for known split issuers (e.g., NVDA pre-2024).
            try:
                if market_cap is not None and price not in (None, 0):
                    sh_sec = _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic')) or _num(r.get('shares_outstanding'))
                    if sh_sec not in (None, 0):
                        split_rule = manual_split_rules.get(ticker_ctx)
                        if split_rule and int(year) < int(split_rule.get('cutoff_year', 0)):
                            sf = float(split_rule.get('ratio') or 1.0)
                            if sf > 1.0:
                                # Shares can come in mixed units; evaluate absolute-share candidates.
                                abs_share_candidates = []
                                for mul in (1.0, 1_000.0, 1_000_000.0):
                                    sh_abs = float(sh_sec) * mul
                                    if sh_abs > 0:
                                        abs_share_candidates.append(sh_abs)
                                split_mcap_candidates = [
                                    (price * (sh_abs * sf)) / 1_000_000.0
                                    for sh_abs in abs_share_candidates
                                ]
                                split_mcap_candidates = [v for v in split_mcap_candidates if v > 0]
                                if split_mcap_candidates:
                                    target = abs(float(market_cap)) * sf
                                    mcap_split = min(split_mcap_candidates, key=lambda v: abs(abs(v) - target))
                                    # Apply only when current cap looks compressed versus split-adjusted anchor.
                                    compressed = abs(float(market_cap)) < (abs(float(mcap_split)) * 0.4)
                                    if compressed:
                                        market_cap = mcap_split
                                        r['market_cap_source'] = f'SPLIT_REPAIRED_X{sf:g}'
            except Exception:
                pass

            if market_cap is not None:
                r['market_cap'] = market_cap
                fcf_val = _num(r.get('free_cash_flow'))
                if fcf_val is not None and market_cap not in (None, 0):
                    fy = fcf_val / market_cap
                    if -1.0 <= fy <= 1.0:
                        r['fcf_yield'] = fy

            market_pe = _pick_market(year, 'market:pe_ratio', 'yahoo:pe_ratio')
            market_pb = _pick_market(year, 'market:pb_ratio', 'yahoo:pb_ratio')
            market_div_yield = _pick_market(year, 'market:dividend_yield', 'yahoo:dividend_yield')
            split_latest_ratio = _effective_split_ratio(
                year,
                _pick_market(year, 'market:split_latest_ratio', 'yahoo:split_latest_ratio'),
            )
            payout_ratio = _pick_market(year, 'market:payout_ratio', 'yahoo:payout_ratio')
            market_bvps = _pick_market(year, 'market:book_value_per_share', 'yahoo:book_value_per_share')

            eps = _num(r.get('eps_basic'))
            if eps is None:
                eps = _num(row.get('EarningsPerShareBasic'))
            if eps is None:
                ni = _num(row.get('NetIncomeLoss')) or _num(row.get('NetIncome'))
                sh = _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic')) or _num(r.get('shares_outstanding'))
                if ni is not None and sh not in (None, 0):
                    eps = self._select_per_share_scaled_value(
                        numerator=ni,
                        shares=sh,
                        price_hint=price,
                        target_ratio=market_pe,
                    )
                    if eps is not None:
                        r['eps_basic'] = eps
            # Split-aware SEC shares alignment using market/yahoo shares anchor
            # even when market PE is unavailable.
            if split_latest_ratio not in (None, 0):
                ni = _num(row.get('NetIncomeLoss')) or _num(row.get('NetIncome'))
                sh_sec = _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic')) or _num(row.get('SharesBasic'))
                sh_mkt = _pick_market(year, 'market:shares_outstanding', 'yahoo:shares_outstanding')
                if ni not in (None, 0) and sh_sec not in (None, 0) and sh_mkt not in (None, 0):
                    try:
                        sf = abs(float(split_latest_ratio))
                    except Exception:
                        sf = None
                    if sf not in (None, 0) and sf > 1.0:
                        sec_raw_candidates = [sh_sec, sh_sec * 1_000.0, sh_sec * 1_000_000.0]
                        sec_raw_candidates = [v for v in sec_raw_candidates if v not in (None, 0)]
                        if sec_raw_candidates:
                            best_raw = min(sec_raw_candidates, key=lambda v: abs(v - sh_mkt))
                            raw_gap = abs(best_raw - sh_mkt) / max(abs(sh_mkt), 1.0)
                            split_gap = abs((best_raw * sf) - sh_mkt) / max(abs(sh_mkt), 1.0)
                            # apply split only if it materially improves alignment.
                            if split_gap + 0.05 < raw_gap:
                                eps_adj = self._select_per_share_scaled_value(
                                    numerator=ni,
                                    shares=(sh_sec * sf),
                                    price_hint=price,
                                    target_ratio=market_pe,
                                )
                                if eps_adj not in (None, 0):
                                    eps = eps_adj
                                    r['eps_basic'] = eps_adj
                                    r['eps_source'] = f'SEC_SPLIT_ADJUSTED_X{sf:g}_SHARES_ANCHOR'
            # Guardrail: if EPS exists but conflicts materially with market P/E,
            # rescale EPS from SEC NI/shares under the market-implied anchor.
            if eps not in (None, 0) and price not in (None, 0) and market_pe not in (None, 0):
                try:
                    implied_eps = price / market_pe
                except Exception:
                    implied_eps = None
                if implied_eps not in (None, 0):
                    ratio_gap = max(abs(eps), abs(implied_eps)) / max(min(abs(eps), abs(implied_eps)), 1e-12)
                    if ratio_gap >= 5.0:
                        ni = _num(row.get('NetIncomeLoss')) or _num(row.get('NetIncome'))
                        sh = _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic')) or _num(r.get('shares_outstanding'))
                        if ni is not None and sh not in (None, 0):
                            split_factors = [1.0]
                            if split_latest_ratio not in (None, 0):
                                try:
                                    sf = abs(float(split_latest_ratio))
                                    if sf > 1.0:
                                        split_factors.extend([sf, sf / 2.0, sf / 4.0, sf / 5.0])
                                except Exception:
                                    pass
                            # Common split factors as robust fallback when source split metadata is absent.
                            split_factors.extend([2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 40.0])
                            dedup = []
                            seen_sf = set()
                            for sf in split_factors:
                                if sf <= 0:
                                    continue
                                key = round(sf, 8)
                                if key in seen_sf:
                                    continue
                                seen_sf.add(key)
                                dedup.append(sf)
                            split_factors = dedup

                            eps_candidates = []
                            for sf in split_factors:
                                sh_adj = sh * sf
                                if sh_adj in (None, 0):
                                    continue
                                eps_recalc = self._select_per_share_scaled_value(
                                    numerator=ni,
                                    shares=sh_adj,
                                    price_hint=price,
                                    target_ratio=market_pe,
                                )
                                if eps_recalc in (None, 0):
                                    continue
                                pe_recalc = abs(price / eps_recalc) if eps_recalc not in (None, 0) else None
                                if pe_recalc is None:
                                    continue
                                if not (1.0 <= pe_recalc <= 2_000.0):
                                    continue
                                score = abs(pe_recalc - abs(market_pe))
                                if sf != 1.0:
                                    score += 0.05
                                eps_candidates.append((score, eps_recalc, sf, pe_recalc))

                            if eps_candidates:
                                _, eps_pick, sf_pick, pe_pick = min(eps_candidates, key=lambda x: x[0])
                                eps = eps_pick
                                r['eps_basic'] = eps_pick
                                if sf_pick != 1.0:
                                    r['eps_source'] = f'SEC_SPLIT_ADJUSTED_X{sf_pick:g}_USING_MARKET_PE'
                                else:
                                    r['eps_source'] = 'SEC_RESCALED_USING_MARKET_PE'
                                r['pe_ratio_used'] = pe_pick
                                r['pe_ratio_used_source'] = 'ANNUAL_PRICE_OVER_EPS_SPLIT_ADJUSTED'
            # Split-aware fallback when market PE is missing:
            # if annual price/eps is implausibly low/high and split metadata exists,
            # try reconstructing EPS with split-adjusted shares.
            if (
                eps not in (None, 0)
                and price not in (None, 0)
                and market_pe in (None, 0)
                and split_latest_ratio not in (None, 0)
            ):
                try:
                    pe_now = abs(price / eps)
                except Exception:
                    pe_now = None
                if pe_now not in (None, 0) and (pe_now < 4.0 or pe_now > 1500.0):
                    ni = _num(row.get('NetIncomeLoss')) or _num(row.get('NetIncome'))
                    sh = _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic')) or _num(r.get('shares_outstanding'))
                    if ni is not None and sh not in (None, 0):
                        split_factors = [1.0]
                        try:
                            sf = abs(float(split_latest_ratio))
                            if sf > 1.0:
                                split_factors.extend([sf, sf / 2.0, sf / 4.0, sf / 5.0])
                        except Exception:
                            pass
                        split_factors.extend([2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 40.0])
                        dedup = []
                        seen_sf = set()
                        for sf in split_factors:
                            if sf <= 0:
                                continue
                            key = round(sf, 8)
                            if key in seen_sf:
                                continue
                            seen_sf.add(key)
                            dedup.append(sf)
                        split_factors = dedup

                        candidates = []
                        for sf in split_factors:
                            sh_adj = sh * sf
                            if sh_adj in (None, 0):
                                continue
                            eps_recalc = self._select_per_share_scaled_value(
                                numerator=ni,
                                shares=sh_adj,
                                price_hint=price,
                                target_ratio=None,
                            )
                            if eps_recalc in (None, 0):
                                continue
                            pe_recalc = abs(price / eps_recalc)
                            if not (2.0 <= pe_recalc <= 500.0):
                                continue
                            # Prefer plausible valuation window and minimal split distortion.
                            score = abs(pe_recalc - 30.0)
                            if sf != 1.0:
                                score += 0.05
                            candidates.append((score, eps_recalc, sf, pe_recalc))
                        if candidates:
                            _, eps_pick, sf_pick, pe_pick = min(candidates, key=lambda x: x[0])
                            eps = eps_pick
                            r['eps_basic'] = eps_pick
                            if sf_pick != 1.0:
                                r['eps_source'] = f'SEC_SPLIT_ADJUSTED_X{sf_pick:g}_NO_MARKET_PE'
                            else:
                                r['eps_source'] = 'SEC_RESCALED_NO_MARKET_PE'
                            r['pe_ratio_used'] = pe_pick
                            r['pe_ratio_used_source'] = 'ANNUAL_PRICE_OVER_EPS_SPLIT_ADJUSTED'
            if market_pe is not None and market_pe > 0:
                # Canonical market truth when available.
                r['pe_ratio'] = market_pe
            elif eps not in (None, 0) and price is not None:
                r['pe_ratio'] = price / eps

            equity = _num(row.get('StockholdersEquity')) or _num(row.get('TotalEquity')) or _num(row.get('Total Equity'))
            shares = _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic')) or _num(r.get('shares_outstanding'))
            bvps = _num(r.get('book_value_per_share'))
            if market_bvps not in (None, 0) and (
                bvps in (None, 0)
                or (abs(bvps) > 0 and (max(abs(bvps), abs(market_bvps)) / min(abs(bvps), abs(market_bvps))) >= 5.0)
            ):
                bvps = market_bvps
                r['book_value_per_share'] = market_bvps

            price_for_bv = price
            if price_for_bv is None and market_cap is not None and shares not in (None, 0):
                # Derive implied price candidates from market cap / shares across unit scales.
                price_candidates = []
                for m_scale in (1.0, 1_000.0, 1_000_000.0):
                    for s_scale in (1.0, 1_000.0, 1_000_000.0):
                        denom = shares * s_scale
                        if denom == 0:
                            continue
                        p_val = (market_cap * m_scale) / denom
                        if p_val == p_val and not math.isinf(p_val) and 0.05 <= abs(p_val) <= 10_000:
                            price_candidates.append(p_val)
                if price_candidates:
                    price_for_bv = min(price_candidates, key=lambda p: abs(abs(p) - 120.0))

            if bvps in (None, 0) and equity is not None and shares not in (None, 0):
                bvps = _book_value_per_share(
                    equity,
                    shares,
                    price_hint=price_for_bv,
                    target_ratio=market_pb,
                )
                if bvps not in (None, 0):
                    r['book_value_per_share'] = bvps

            pb_val = None
            if market_pb is not None and market_pb > 0:
                # Canonical market truth when available.
                pb_val = market_pb
            elif price is not None and bvps not in (None, 0):
                trial = price / bvps
                if trial > 0:
                    pb_val = trial
            elif market_cap is not None and equity not in (None, 0):
                # Last-resort fallback under explicit scale exploration.
                eq_candidates = [equity, equity * 1_000.0, equity * 1_000_000.0]
                pb_candidates = []
                for eqc in eq_candidates:
                    if eqc in (None, 0):
                        continue
                    pb = market_cap / eqc
                    if pb > 0:
                        pb_candidates.append(pb)
                plausible = [p for p in pb_candidates if 0.1 <= p <= 100.0]
                if plausible:
                    pb_val = min(plausible, key=lambda x: abs(x - 8.0))
                elif pb_candidates:
                    pb_val = min(pb_candidates, key=lambda x: abs(x - 8.0))

            if pb_val is not None:
                r['pb_ratio'] = pb_val

            if market_div_yield is not None:
                r['dividend_yield'] = market_div_yield
            elif payout_ratio is not None and abs(payout_ratio) < 1e-9:
                # Explicit zero-payout issuers should show 0% yield, not N/A.
                r['dividend_yield'] = 0.0

            # Canonical valuation alignment (annual-consistent):
            # - Prefer SEC EPS facts when available (prevents bad NI/shares-derived EPS when NI tag is wrong).
            # - Compute PE/PB from the same price anchor implied by (market_cap / shares) in system units.
            # - Preserve market snapshot ratios in separate fields for transparency.
            try:
                if market_pe not in (None, 0):
                    r.setdefault('pe_ratio_market', market_pe)
                if market_pb not in (None, 0):
                    r.setdefault('pb_ratio_market', market_pb)

                eps_sec_basic = _num(row.get('EarningsPerShareBasic'))
                eps_sec_dil = _num(row.get('EarningsPerShareDiluted'))
                if eps_sec_dil not in (None, 0):
                    r['eps_diluted'] = eps_sec_dil
                    r['eps_source'] = 'SEC_EPS_DILUTED_FACT'
                if eps_sec_basic not in (None, 0):
                    r['eps_basic'] = eps_sec_basic
                    if not r.get('eps_source'):
                        r['eps_source'] = 'SEC_EPS_BASIC_FACT'

                # Shares (million shares canonical in this codebase).
                shares_end_raw = _num(row.get('EntityCommonStockSharesOutstanding')) or _num(row.get('CommonStockSharesOutstanding'))
                shares_end_m = self._normalize_shares_to_million(shares_end_raw) if shares_end_raw not in (None, 0) else None
                if shares_end_m not in (None, 0):
                    r['shares_outstanding_end'] = shares_end_m

                # Prefer end shares for price/market-cap identity; fall back to previously normalized shares_outstanding.
                shares_for_price = shares_end_m or _num(r.get('shares_outstanding'))
                if shares_for_price not in (None, 0):
                    r['shares_outstanding'] = shares_for_price

                # Price anchor: implied annual price from market_cap (usd_million) / shares (shares_million).
                price_anchor = None
                if market_cap not in (None, 0) and shares_for_price not in (None, 0):
                    price_anchor = float(market_cap) / float(shares_for_price)
                elif price not in (None, 0):
                    price_anchor = float(price)

                # Book value per share from equity / end shares (preferred).
                if equity not in (None, 0) and shares_for_price not in (None, 0):
                    bvps_canon = float(equity) / float(shares_for_price)
                    if bvps_canon not in (None, 0):
                        existing_bvps = _num(r.get('book_value_per_share'))
                        if existing_bvps in (None, 0):
                            r['book_value_per_share'] = bvps_canon
                        else:
                            # If existing BVPS is wildly off, replace with canonical.
                            try:
                                gap = max(abs(existing_bvps), abs(bvps_canon)) / max(min(abs(existing_bvps), abs(bvps_canon)), 1e-12)
                            except Exception:
                                gap = None
                            if gap is not None and gap >= 10.0:
                                r['book_value_per_share'] = bvps_canon

                # Canonical PB from market_cap/equity when available (most stable).
                if market_cap not in (None, 0) and equity not in (None, 0):
                    pb_canon = float(market_cap) / float(equity)
                    if pb_canon > 0:
                        r['pb_ratio'] = pb_canon
                        r['pb_ratio_source'] = 'CANONICAL_MARKET_CAP_OVER_EQUITY'
                elif price_anchor not in (None, 0) and _num(r.get('book_value_per_share')) not in (None, 0):
                    pb_canon = float(price_anchor) / float(_num(r.get('book_value_per_share')))
                    if pb_canon > 0:
                        r['pb_ratio'] = pb_canon
                        r['pb_ratio_source'] = 'CANONICAL_PRICE_OVER_BVPS'

                # Canonical PE from price / EPS (prefer diluted EPS when present).
                eps_for_pe = _num(r.get('eps_diluted')) or _num(r.get('eps_basic'))
                if price_anchor not in (None, 0) and eps_for_pe not in (None, 0):
                    pe_canon = float(price_anchor) / float(eps_for_pe)
                    r['pe_ratio'] = pe_canon
                    r['pe_ratio_source'] = 'CANONICAL_PRICE_OVER_EPS'
                    # Ensure audit identity uses the canonical value (some paths prefer pe_ratio_used).
                    r['pe_ratio_used'] = pe_canon
                    r['pe_ratio_used_source'] = 'CANONICAL_PRICE_OVER_EPS'
            except Exception:
                pass

            # Debt ratio correction fallback from market/yahoo is disabled by default.
            # SEC strict debt definition should remain the source of truth.
            if not allow_market_debt_override:
                market_total_debt = None
                interpolated_debt = None

            # Debt ratio correction fallback from market/yahoo total debt when explicitly enabled.
            existing_total_debt = _num(r.get('total_debt'))
            if market_total_debt is None and interpolated_debt is not None:
                # If direct market debt is missing for this year, use interpolated market debt
                # only when current debt is absent or implausibly tiny vs liabilities.
                implausibly_tiny = (
                    existing_total_debt is not None
                    and liab_for_debt not in (None, 0)
                    and (abs(existing_total_debt) / abs(liab_for_debt)) < 0.03
                )
                if (not bank_like) and (existing_total_debt is None or implausibly_tiny):
                    market_total_debt = interpolated_debt
                    r['total_debt_source'] = 'MARKET_INTERPOLATED_TIME_SERIES'

            # Bank-safe proxy: infer debt from liabilities using observed anchored ratio
            # rather than repeating a flat interpolated market debt over history.
            if (
                allow_market_debt_override
                and
                bank_like
                and market_total_debt is None
                and bank_debt_liab_anchor not in (None, 0)
                and liab_for_debt not in (None, 0)
            ):
                sec_tiny_or_missing = (
                    existing_total_debt is None
                    or (abs(existing_total_debt) / max(abs(liab_for_debt), 1e-9) < 0.03)
                )
                if sec_tiny_or_missing:
                    market_total_debt = abs(liab_for_debt) * float(bank_debt_liab_anchor)
                    r['total_debt_source'] = 'SEC_LIABILITIES_RATIO_PROXY'

            if market_total_debt is not None:
                debt_candidates = [
                    market_total_debt,
                    market_total_debt / 1_000.0,
                    market_total_debt / 1_000_000.0,
                ]
                debt_est = None
                ref = assets_for_debt or equity_for_debt
                if ref not in (None, 0):
                    plausible = [d for d in debt_candidates if 0.03 <= (abs(d) / abs(ref)) <= 2.5]
                    if plausible:
                        debt_est = min(plausible, key=lambda d: abs((abs(d) / abs(ref)) - 0.35))
                if debt_est is None and debt_candidates:
                    debt_est = debt_candidates[0]
                if debt_est is not None:
                    # Only override SEC debt when SEC debt is missing or implausible.
                    sec_debt = existing_total_debt
                    sec_debt_plausible = False
                    market_debt_plausible = False
                    if liab_for_debt not in (None, 0):
                        try:
                            sec_ratio = abs(sec_debt) / abs(liab_for_debt) if sec_debt is not None else None
                            mkt_ratio = abs(debt_est) / abs(liab_for_debt)
                            sec_debt_plausible = sec_ratio is not None and 0.02 <= sec_ratio <= 2.5
                            market_debt_plausible = 0.02 <= mkt_ratio <= 2.5
                        except Exception:
                            sec_debt_plausible = False
                            market_debt_plausible = False
                    use_market_debt = (
                        sec_debt is None
                        or (not sec_debt_plausible and market_debt_plausible)
                        or (
                            sec_debt is not None
                            and liab_for_debt not in (None, 0)
                            and abs(sec_debt) / abs(liab_for_debt) < 0.03
                            and market_debt_plausible
                        )
                    )
                    if use_market_debt:
                        # Persist normalized debt estimate as canonical total_debt.
                        r['total_debt'] = debt_est
                        r['total_debt_source'] = r.get('total_debt_source') or 'MARKET_LAYER_GUARDRAIL_OVERRIDE'
                    if equity_for_debt not in (None, 0):
                        de_now = _num(r.get('debt_to_equity'))
                        debt_for_ratio = _num(r.get('total_debt')) if use_market_debt else sec_debt
                        de_new = (debt_for_ratio / equity_for_debt) if debt_for_ratio is not None else None
                        if (
                            de_new is not None
                            and (
                                de_now is None
                            or de_now > 10
                            or de_now < 0
                            or (de_now > 3.0 and 0.1 <= de_new <= 3.0)
                            or (abs(de_now - de_new) > max(0.05, abs(de_new) * 0.30))
                            )
                        ):
                            r['debt_to_equity'] = de_new
                    if assets_for_debt not in (None, 0):
                        da_now = _num(r.get('debt_to_assets'))
                        debt_for_ratio = _num(r.get('total_debt')) if use_market_debt else sec_debt
                        da_new = (debt_for_ratio / assets_for_debt) if debt_for_ratio is not None else None
                        if (
                            da_new is not None
                            and (
                                da_now is None
                            or da_now > 2
                            or da_now < 0
                            or (da_now > 0.65 and 0.03 <= da_new <= 0.65)
                            or (abs(da_now - da_new) > max(0.03, abs(da_new) * 0.30))
                            )
                        ):
                            r['debt_to_assets'] = da_new

                    # Recompute net_debt_ebitda with normalized debt when possible.
                    cash_for_nd = (
                        _num(row.get('CashAndCashEquivalentsAtCarryingValue'))
                        or _num(row.get('CashAndCashEquivalents'))
                        or _num(row.get('Cash and Cash Equivalents'))
                    )
                    op = _num(row.get('OperatingIncomeLoss')) or _num(row.get('OperatingIncome'))
                    dep = _num(row.get('DepreciationDepletionAndAmortization')) or _num(row.get('DepreciationAmortization')) or 0.0
                    ebitda_for_nd = _num(row.get('EBITDA'))
                    if ebitda_for_nd is None and op is not None:
                        ebitda_for_nd = op + (dep or 0.0)
                    if cash_for_nd is not None and ebitda_for_nd not in (None, 0):
                        # Align cash to debt scale under common SEC million/absolute mismatches.
                        cash_candidates = [
                            cash_for_nd,
                            cash_for_nd / 1_000.0,
                            cash_for_nd / 1_000_000.0,
                            cash_for_nd * 1_000.0,
                            cash_for_nd * 1_000_000.0,
                        ]
                        plausible_cash = []
                        for cc in cash_candidates:
                            if cc is None:
                                continue
                            ratio = abs(cc / debt_est) if debt_est else None
                            if ratio is None:
                                continue
                            if 0.001 <= ratio <= 2.0:
                                plausible_cash.append(cc)
                        cash_norm = (
                            min(plausible_cash, key=lambda c: abs((abs(c / debt_est)) - 0.2))
                            if plausible_cash else cash_for_nd
                        )
                        nd_eb = (debt_est - cash_norm) / ebitda_for_nd
                        if -20 <= nd_eb <= 20:
                            r['net_debt_ebitda'] = nd_eb
            # Enterprise Value and EV/EBITDA (million-USD base).
            market_ev = _pick_market_million(
                year,
                'market:enterprise_value',
                'yahoo:enterprise_value',
                metric_key='enterprise_value',
                reference=(assets_for_debt or market_cap),
                price=price,
                shares=shares_ref,
            )
            debt_for_ev = _num(r.get('total_debt'))
            if debt_for_ev is None:
                debt_for_ev = market_total_debt
            cash_for_ev = (
                _num(row.get('CashAndCashEquivalentsAtCarryingValue'))
                or _num(row.get('CashAndCashEquivalents'))
                or _num(row.get('Cash and Cash Equivalents'))
            )
            if market_ev is None and market_cap is not None and debt_for_ev is not None:
                cash_adj = cash_for_ev
                if cash_for_ev is not None and market_cap != 0:
                    cash_candidates = [
                        cash_for_ev,
                        cash_for_ev / 1_000.0,
                        cash_for_ev / 1_000_000.0,
                        cash_for_ev * 1_000.0,
                        cash_for_ev * 1_000_000.0,
                    ]
                    plausible_cash = [
                        cc for cc in cash_candidates
                        if cc is not None and 0.0001 <= (abs(cc) / max(abs(market_cap), 1e-9)) <= 0.6
                    ]
                    if plausible_cash:
                        cash_adj = min(plausible_cash, key=lambda c: abs((abs(c) / max(abs(market_cap), 1e-9)) - 0.02))
                market_ev = market_cap + debt_for_ev - (cash_adj or 0.0)
            if market_ev is not None:
                r['enterprise_value'] = market_ev
                op_ev = _num(row.get('OperatingIncomeLoss')) or _num(row.get('OperatingIncome'))
                dep_ev = _num(row.get('DepreciationDepletionAndAmortization')) or _num(row.get('DepreciationAmortization')) or 0.0
                ebitda_for_ev = _num(row.get('EBITDA'))
                if ebitda_for_ev is None and op_ev is not None:
                    ebitda_for_ev = op_ev + (dep_ev or 0.0)
                if ebitda_for_ev not in (None, 0):
                    e_cands = [
                        ebitda_for_ev,
                        ebitda_for_ev / 1_000.0,
                        ebitda_for_ev / 1_000_000.0,
                        ebitda_for_ev * 1_000.0,
                        ebitda_for_ev * 1_000_000.0,
                    ]
                    plausible_e = [c for c in e_cands if c not in (None, 0) and 1.0 <= (abs(market_ev) / abs(c)) <= 500.0]
                    if plausible_e:
                        denom = min(plausible_e, key=lambda c: abs((abs(market_ev) / abs(c)) - 25.0))
                        r['ev_ebitda'] = market_ev / denom if denom else None

            # Recompute Altman Z using Market Cap for X4 when available.
            assets = _num(row.get('Assets')) or _num(row.get('TotalAssets')) or _num(row.get('Total Assets'))
            current_assets = (
                _num(row.get('AssetsCurrent'))
                or _num(row.get('CurrentAssets'))
                or _num(row.get('Current Assets'))
                or _num(row.get('TotalCurrentAssets_Hierarchy'))
            )
            current_liabilities = (
                _num(row.get('LiabilitiesCurrent'))
                or _num(row.get('CurrentLiabilities'))
                or _num(row.get('Current Liabilities'))
                or _num(row.get('TotalCurrentLiabilities_Hierarchy'))
            )
            retained = (
                _num(row.get('RetainedEarnings'))
                or _num(row.get('RetainedEarningsAccumulatedDeficit'))
                or 0.0
            )
            operating_income = _num(row.get('OperatingIncomeLoss')) or _num(row.get('OperatingIncome'))
            liabilities = _num(row.get('Liabilities')) or _num(row.get('TotalLiabilities')) or _num(row.get('Total Liabilities'))
            revenue = _num(row.get('Revenues')) or _num(row.get('SalesRevenueNet')) or _num(row.get('Revenue'))
            if (
                market_cap is not None and liabilities not in (None, 0)
                and assets not in (None, 0)
                and current_assets is not None and current_liabilities is not None
                and operating_income is not None and revenue is not None
            ):
                x1 = (current_assets - current_liabilities) / assets
                x2 = retained / assets
                x3 = operating_income / assets
                mc_candidates = [
                    market_cap,
                    market_cap / 1_000.0,
                    market_cap / 1_000_000.0,
                    market_cap / 1_000_000_000.0,
                ]
                x4_candidates = [(mc / liabilities) for mc in mc_candidates if liabilities not in (None, 0)]
                plausible_x4 = [x for x in x4_candidates if 0.1 <= x <= 50.0]
                if plausible_x4:
                    x4 = min(plausible_x4, key=lambda x: abs(x - 8.0))
                else:
                    x4 = x4_candidates[0] if x4_candidates else 0.0
                x5 = revenue / assets
                r['altman_z_score'] = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + x5

        # Secondary pass: valuation-cost metrics requiring previous-year debt context.
        sorted_years = sorted(all_years)
        for idx, year in enumerate(sorted_years):
            r = ratios.get(year, {}) or {}
            row = rows.get(year, {}) or {}
            prev_year = sorted_years[idx - 1] if idx > 0 else None

            debt_now = _num(r.get('total_debt'))
            debt_prev = _num((ratios.get(prev_year, {}) or {}).get('total_debt')) if prev_year is not None else None
            interest_exp = _num(r.get('interest_expense_used'))
            if debt_now not in (None, 0) and interest_exp not in (None, 0):
                debt_avg = (abs(debt_now) + abs(debt_prev)) / 2.0 if debt_prev not in (None, 0) else abs(debt_now)
                bank_signal = any(
                    _num(row.get(k)) is not None
                    for k in ('Deposits', 'DepositLiabilities', 'NetInterestIncome', 'InterestIncomeOperating', 'LoansReceivable')
                )
                dep_now_raw = _num(row.get('Deposits')) or _num(row.get('DepositLiabilities'))
                dep_prev_raw = None
                if prev_year is not None:
                    prev_row = rows.get(prev_year, {}) or {}
                    dep_prev_raw = _num(prev_row.get('Deposits')) or _num(prev_row.get('DepositLiabilities'))
                dep_now = self._normalize_million_value(dep_now_raw) if dep_now_raw is not None else None
                dep_prev = self._normalize_million_value(dep_prev_raw) if dep_prev_raw is not None else None

                funding_avg = None
                dep_proxy = dep_now if dep_now is not None else dep_prev
                if (bank_signal or dep_prev is not None) and dep_proxy is not None:
                    if dep_now is not None and dep_prev is not None:
                        funding_avg = ((abs(dep_now) + abs(dep_prev)) / 2.0) + (debt_avg or 0.0)
                    else:
                        funding_avg = abs(dep_proxy) + (debt_avg or 0.0)

                cod_base = funding_avg if (funding_avg is not None and funding_avg > 0) else debt_avg
                if cod_base and cod_base > 0:
                    bank_like = (bank_signal or dep_prev is not None)
                    insurance_like = any(_num(row.get(k)) is not None for k in ('PremiumsEarned', 'PolicyholderBenefits', 'PolicyClaims'))
                    if bank_like:
                        cod_min, cod_max, cod_target = 0.001, 0.15, 0.03
                    elif insurance_like:
                        cod_min, cod_max, cod_target = 0.001, 0.20, 0.04
                    else:
                        cod_min, cod_max, cod_target = 0.005, 0.25, 0.05
                    i_cands = [
                        abs(float(interest_exp)),
                        abs(float(interest_exp)) / 1_000.0,
                        abs(float(interest_exp)) / 1_000_000.0,
                        abs(float(interest_exp)) * 1_000.0,
                    ]
                    cod_candidates = []
                    for ic in i_cands:
                        if ic in (None, 0):
                            continue
                        cv = ic / cod_base
                        if cod_min <= cv <= cod_max:
                            cod_candidates.append((ic, cv))
                    if cod_candidates:
                        _ic, cod = min(cod_candidates, key=lambda t: abs(t[1] - cod_target))
                        r['cost_of_debt'] = cod
            elif debt_now not in (None, 0):
                # Conservative continuity fallback for years with missing interest fact.
                prev_cod = _num((ratios.get(prev_year, {}) or {}).get('cost_of_debt')) if prev_year is not None else None
                if prev_cod not in (None, 0) and 0.001 <= prev_cod <= 0.25:
                    r['cost_of_debt'] = prev_cod
                    r['cost_of_debt_source'] = 'CARRY_FORWARD_PROXY'

            beta = _pick_market(year, 'market:beta', 'yahoo:beta')
            rf_macro = _num((l3.get(year, {}) or {}).get('macro:risk_free_rate'))
            if rf_macro is None:
                rf_macro = _num((l3.get(year, {}) or {}).get('FRED:DGS10'))
            if rf_macro is None:
                rf_macro = 0.04
            elif abs(rf_macro) > 1.0:
                # Keep percent normalization without forbidden *100 or /100 arithmetic.
                rf_macro = rf_macro * 0.01
            market_risk_premium = 0.055
            cost_of_equity = None
            if beta is not None:
                cost_of_equity = rf_macro + (beta * market_risk_premium)
            elif _num(r.get('roe')) is not None:
                cost_of_equity = max(rf_macro + 0.02, min(_num(r.get('roe')), 0.35))

            market_cap = _num(r.get('market_cap'))
            if market_cap is None:
                price = _pick_market(year, 'market:price', 'yahoo:price')
                shares = (
                    _num(row.get('WeightedAverageNumberOfSharesOutstandingBasic'))
                    or _num(row.get('SharesBasic'))
                    or _num(r.get('shares_outstanding'))
                )
                if price not in (None, 0) and shares not in (None, 0):
                    market_cap = (price * shares) / 1_000_000.0
            cod_val = _num(r.get('cost_of_debt'))
            if (
                market_cap not in (None, 0)
                and debt_now not in (None, 0)
                and cod_val not in (None, 0)
                and cost_of_equity not in (None, 0)
            ):
                V = market_cap + debt_now
                if V > 0:
                    tax_rate = 0.21
                    wacc = (market_cap / V) * cost_of_equity + (debt_now / V) * cod_val * (1.0 - tax_rate)
                    if 0.01 <= wacc <= 0.40:
                        r['wacc'] = wacc

            # In strict mode we do not silently copy prior-year ratio values when
            # current-year anchors are missing. Keep provenance explicit.
            if r.get('balance_anchor_proxy_source') and prev_year is not None:
                r['balance_anchor_proxy_year'] = prev_year

            ratios[year] = r

        # Hard sanitize non-finite numeric artifacts (NaN/inf) after all injections.
        for year, r in list(ratios.items()):
            if not isinstance(r, dict):
                continue
            for k, v in list(r.items()):
                if isinstance(v, (int, float)) and (math.isnan(v) or math.isinf(v)):
                    r[k] = None
            ratios[year] = r

        return ratios

    def _load_companies(self):
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {
                'User-Agent': 'Company-Loader/1.0 (mihoubmessaoud@yahoo.fr)',
                'Accept': 'application/json'
            }
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                data = r.json()
                for item in data.values():
                    ticker = item.get('ticker', '').upper()
                    cik = str(item['cik_str']).zfill(10)
                    name = item.get('title', '')
                    self.companies_cache[ticker] = {'cik': cik, 'name': name, 'ticker': ticker}
                    name_key = name.upper().replace(',', '').replace('.', '').replace('INC', '').replace('CORP', '').strip()
                    if name_key:
                        self.companies_cache[name_key] = {'cik': cik, 'name': name, 'ticker': ticker}
                print(f"âœ… ØªÙ… ØªØ­Ù…ÙŠÙ„ {len(data)} Ø´Ø±ÙƒØ©")
            else:
                print(f"âš ï¸ ÙØ´Ù„ ØªØ­Ù…ÙŠÙ„ Ø§Ù„Ø´Ø±ÙƒØ§Øª: {r.status_code}")
        except Exception as e:
            print(f"âš ï¸ Ø®Ø·Ø£ ÙÙŠ ØªØ­Ù…ÙŠÙ„ Ø§Ù„Ø´Ø±ÙƒØ§Øª: {e}")

    def get_company_info(self, company_identifier):
        identifier = company_identifier.upper().strip()
        if identifier in self.companies_cache:
            return self.companies_cache[identifier]
        if identifier.isdigit():
            cik_padded = identifier.zfill(10)
            for company in self.companies_cache.values():
                if company['cik'] == cik_padded:
                    return company
        for key, company in self.companies_cache.items():
            if identifier in key or identifier in company['name'].upper():
                return company
        return None

    # Market data via yfinance (optional)
    def get_market_data(self, ticker):
        """
        âœ… Enhanced to collect comprehensive market data from Yahoo Finance
        Including: Price, Shares, Market Cap, Beta, Dividends, P/E, P/B
        """
        if not ticker:
            return self._empty_market_data()
        if yf is None:
            print("âš ï¸ yfinance ØºÙŠØ± Ù…Ø«Ø¨Øª - ØªØ«Ø¨ÙŠØª: pip install yfinance")
            return self._empty_market_data()
        
        last_error = None
        for attempt in range(1, 4):
            try:
                print(f"ðŸ“Š Ø¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ Ù…Ù† Yahoo Finance Ù„Ù€ {ticker}... (attempt {attempt}/3)")
                tk = yf.Ticker(ticker)
                info = tk.info or {}

                if not info:
                    raise ValueError("EMPTY_YAHOO_INFO")

                # âœ… Price - multiple fallbacks
                price = (
                    info.get('regularMarketPrice')
                    or info.get('currentPrice')
                    or info.get('previousClose')
                    or info.get('ask')
                    or info.get('bid')
                )

                # âœ… Shares Outstanding - multiple sources
                shares = (
                    info.get('sharesOutstanding')
                    or info.get('impliedSharesOutstanding')
                    or info.get('floatShares')
                    or info.get('shares')
                )

                # âœ… Market Cap - calculate if not available
                market_cap = info.get('marketCap')
                if not market_cap and price and shares:
                    market_cap = price * shares

                # âœ… Beta for CAPM
                beta = info.get('beta')

                # âœ… NEW: Additional market data
                pe_ratio = info.get('trailingPE') or info.get('forwardPE')
                pb_ratio = info.get('priceToBook')
                dividend_yield = info.get('dividendYield')  # as decimal (e.g., 0.025 = 2.5%)
                dividend_rate = info.get('dividendRate')  # annual dividend per share

                # âœ… Convert to proper types
                market_cap_usd = float(market_cap) if market_cap is not None else None
                market_cap_m = self._normalize_million_value(market_cap_usd) if market_cap_usd is not None else None
                result = {
                    'price': float(price) if price is not None else None,
                    'shares': int(shares) if shares is not None else None,
                    'market_cap': float(market_cap_m) if market_cap_m is not None else None,  # million USD
                    'market_cap_usd': market_cap_usd,
                    'beta': float(beta) if beta is not None else None,
                    'pe_ratio': float(pe_ratio) if pe_ratio is not None else None,
                    'pb_ratio': float(pb_ratio) if pb_ratio is not None else None,
                    'dividend_yield': float(dividend_yield) if dividend_yield is not None else None,  # canonical fraction
                    'dividend_rate': float(dividend_rate) if dividend_rate is not None else None,
                }

                # Print summary
                print(f"âœ… ØªÙ… Ø¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ Ø¨Ù†Ø¬Ø§Ø­:")
                print(f"   Ø§Ù„Ø³Ø¹Ø±: ${result['price']}" if result['price'] else "   Ø§Ù„Ø³Ø¹Ø±: N/A")
                print(f"   Ø¹Ø¯Ø¯ Ø§Ù„Ø£Ø³Ù‡Ù…: {result['shares']:,}" if result['shares'] else "   Ø¹Ø¯Ø¯ Ø§Ù„Ø£Ø³Ù‡Ù…: N/A")
                print(f"   Beta: {result['beta']}" if result['beta'] else "   Beta: N/A")

                return result
            except Exception as e:
                last_error = e
                if attempt < 3:
                    time.sleep(float(attempt))
                    continue

        print(f"âŒ Ø®Ø·Ø£ ÙÙŠ Ø¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ Ù…Ù† Yahoo Finance: {last_error}")
        return self._empty_market_data()
    
    def _empty_market_data(self):
        """Return empty market data structure"""
        return {
            'price': None,
            'shares': None,
            'market_cap': None,
            'market_cap_usd': None,
            'beta': None,
            'pe_ratio': None,
            'pb_ratio': None,
            'dividend_yield': None,
            'dividend_rate': None
        }

    @staticmethod
    def _infer_sector_profile(
        company_name: str,
        ticker: str,
        data_by_year: dict = None,
        sic_description: str = '',
    ) -> str:
        txt = f"{company_name or ''} {ticker or ''} {sic_description or ''}".lower()
        sic_txt = str(sic_description or '').lower()
        bank_tokens = (
            'bank of', ' bank', ' banc', 'jpmorgan', 'wells fargo', 'citigroup',
            'goldman', 'morgan stanley', 'credit union', 'national association',
            'commercial bank', 'state commercial banks', 'federal savings'
        )
        bank_sic_tokens = (
            'state commercial banks',
            'national commercial banks',
            'savings institutions',
            'bank holding',
            'security brokers',
            'dealers & flotation',
            'asset management',
            'investment advice',
            'credit card',
        )
        insurance_tokens = (
            'insurance', 'assurance', 'reinsurance', 'insurer',
            'property-casualty', 'life insurance', 'accident and health'
        )
        insurance_sic_tokens = (
            'fire, marine & casualty insurance',
            'life insurance',
            'accident and health insurance',
            'insurance agents',
            'hospital and medical service plans',
        )
        technology_sic_tokens = (
            'electronic computers',
            'computer programming',
            'computer integrated systems design',
            'prepackaged software',
            'services-prepackaged software',
            'semiconductors',
            'semiconductor and related devices',
            'communications equipment',
            'telephone and telegraph apparatus',
            'electronic components',
        )
        technology_name_tokens = (
            'technology',
            'software',
            'semiconductor',
            'micro devices',
            'nvidia',
            'intel',
            'apple',
            'microsoft',
            'adobe',
            'salesforce',
        )
        # Bank-specific concepts must be structural, not generic interest lines that
        # also appear in industrial issuers.
        bank_loan_tokens = (
            'LoansReceivable',
            'LoansAndLeasesReceivableNetReportedAmount',
            'LoansHeldForSale',
            'LoansAndLeasesReceivable',
        )
        bank_deposit_tokens = (
            'Deposits',
            'DepositLiabilities',
            'InterestBearingDepositsInBanks',
            'NoninterestBearingDeposits',
        )
        bank_nim_tokens = (
            'NetInterestIncome',
            'InterestAndDividendIncomeOperating',
            'InterestIncomeOperating',
        )
        bank_credit_tokens = (
            'AllowanceForCreditLosses',
            'ProvisionForCreditLosses',
            'ProvisionForLoanLosses',
            'NonperformingAssets',
        )
        fact_tokens_insurance = (
            'PremiumsEarned',
            'PolicyholderBenefits',
            'PolicyClaims',
            'ReinsuranceRecoverables',
            'DeferredAcquisitionCosts',
        )
        industrial_tokens = (
            'CostOfRevenue',
            'GrossProfit',
            'Inventory',
            'SellingGeneralAndAdministrativeExpense',
            'ResearchAndDevelopmentExpense',
            'RevenueFromContractWithCustomerExcludingAssessedTax',
        )
        if any(tok in sic_txt for tok in insurance_sic_tokens):
            return 'insurance'
        if any(tok in sic_txt for tok in bank_sic_tokens):
            return 'bank'
        if any(tok in sic_txt for tok in technology_sic_tokens):
            return 'technology'
        if any(tok in txt for tok in insurance_tokens):
            return 'insurance'
        if any(tok in txt for tok in bank_tokens):
            return 'bank'
        if any(tok in txt for tok in technology_name_tokens):
            return 'technology'
        # Fact-based sector inference (stronger than name-only heuristic).
        loan_hits = 0
        deposit_hits = 0
        nim_hits = 0
        credit_hits = 0
        insurance_hits = 0
        industrial_hits = 0
        for _y, row in (data_by_year or {}).items():
            if not isinstance(row, dict):
                continue
            keys = ' '.join([str(k) for k in row.keys()])
            for tok in bank_loan_tokens:
                if tok in keys:
                    loan_hits += 1
            for tok in bank_deposit_tokens:
                if tok in keys:
                    deposit_hits += 1
            for tok in bank_nim_tokens:
                if tok in keys:
                    nim_hits += 1
            for tok in bank_credit_tokens:
                if tok in keys:
                    credit_hits += 1
            for tok in fact_tokens_insurance:
                if tok in keys:
                    insurance_hits += 1
            for tok in industrial_tokens:
                if tok in keys:
                    industrial_hits += 1
        bank_hits = loan_hits + deposit_hits + nim_hits + credit_hits
        if insurance_hits >= 2 and insurance_hits >= bank_hits:
            return 'insurance'
        bank_structure_score = 0
        if loan_hits > 0:
            bank_structure_score += 1
        if deposit_hits > 0:
            bank_structure_score += 1
        if nim_hits > 0:
            bank_structure_score += 1
        if credit_hits > 0:
            bank_structure_score += 1
        # Require at least 2 distinct banking structure signals and no strong
        # industrial footprint.
        if bank_structure_score >= 2 and industrial_hits < 2:
            return 'bank'
        if industrial_hits >= 2 and bank_structure_score == 0 and insurance_hits == 0:
            return 'industrial'
        return 'technology'

    def _apply_sector_ratio_gating(self, ratios_by_year: dict, sector_profile: str, sub_sector_profile: str = None) -> dict:
        blocked = self._resolve_sector_ratio_blocklist(sector_profile, sub_sector_profile)
        if not blocked:
            return ratios_by_year or {}
        out = {}
        for year, row in (ratios_by_year or {}).items():
            if not isinstance(row, dict):
                out[year] = row
                continue
            r = dict(row)
            reasons = dict(r.get('_ratio_reasons') or {})
            for rid in blocked:
                if rid in r:
                    r[rid] = None
                reasons[rid] = 'NOT_APPLICABLE_FOR_SECTOR'
            r['_ratio_reasons'] = reasons
            out[year] = r
        return out

    def _normalize_form_code(self, form_code):
        if not form_code:
            return ''
        s = str(form_code).strip().upper()
        out = []
        for ch in s:
            if ch.isalnum():
                out.append(ch)
        return ''.join(out)

    def _is_annual_form_norm(self, form_norm):
        if not form_norm:
            return False
        return (
            form_norm.startswith('10K') or
            form_norm.startswith('20F') or
            form_norm.startswith('40F')
        )

    def _matches_requested_form(self, form_norm, requested_norm):
        if not form_norm or not requested_norm:
            return False
        if form_norm == requested_norm:
            return True
        # tolerate slight mislabeling (e.g. 10K vs 10-K, 10K405, case variants)
        if form_norm.startswith(requested_norm):
            return True
        if requested_norm.startswith('10K') and form_norm.startswith('10K'):
            return True
        if requested_norm.startswith('20F') and form_norm.startswith('20F'):
            return True
        if requested_norm.startswith('40F') and form_norm.startswith('40F'):
            return True
        return False

    def _collect_filing_entries(self, recent):
        forms = recent.get('form', [])
        filing_dates = recent.get('filingDate', [])
        accessions = recent.get('accessionNumber', [])
        report_dates = recent.get('reportDate', []) if 'reportDate' in recent else [None] * len(forms)
        primary_docs = recent.get('primaryDocument', []) if 'primaryDocument' in recent else [None] * len(forms)
        entries = []
        for i, form in enumerate(forms):
            if not form:
                continue
            filing_date = filing_dates[i] if i < len(filing_dates) else None
            report_date = report_dates[i] if i < len(report_dates) else None
            year = None
            for candidate_date in (report_date, filing_date):
                try:
                    if candidate_date and len(candidate_date) >= 4:
                        year = int(str(candidate_date)[:4])
                        break
                except Exception:
                    continue
            accn = accessions[i] if i < len(accessions) else None
            primary_doc = primary_docs[i] if i < len(primary_docs) else None
            entries.append({
                'form': form,
                'form_norm': self._normalize_form_code(form),
                'filing_date': filing_date,
                'report_date': report_date,
                'accession_number': accn,
                'primary_document': primary_doc,
                'year': year
            })
        return entries

    def _collect_all_submission_entries(self, cik, submissions_json):
        """
        Build a complete filing entry list from SEC submissions:
        - filings.recent
        - filings.files (historical chunks)
        """
        entries = []
        try:
            recent = (submissions_json or {}).get('filings', {}).get('recent', {}) or {}
            entries.extend(self._collect_filing_entries(recent))
        except Exception:
            pass
        try:
            hist_files = (submissions_json or {}).get('filings', {}).get('files', []) or []
            for f in hist_files:
                name = str((f or {}).get('name') or '').strip()
                if not name:
                    continue
                url = f"{self.base_url}/submissions/{name}"
                try:
                    rr = requests.get(url, headers=self.headers, timeout=30)
                    if rr.status_code != 200:
                        continue
                    chunk = rr.json() or {}
                    entries.extend(self._collect_filing_entries(chunk))
                    time.sleep(0.05)
                except Exception:
                    continue
        except Exception:
            pass
        dedup = {}
        for e in entries:
            key = (
                str(e.get('accession_number') or ''),
                str(e.get('form_norm') or ''),
                str(e.get('filing_date') or ''),
                str(e.get('report_date') or ''),
            )
            if key not in dedup:
                dedup[key] = e
        return list(dedup.values())

    def _select_filings_with_fallback(self, recent, start_year, end_year, requested_form, entries_override=None):
        entries = list(entries_override) if isinstance(entries_override, list) and entries_override else self._collect_filing_entries(recent)
        requested_norm = self._normalize_form_code(requested_form)
        annual_forms = [e for e in entries if self._is_annual_form_norm(e.get('form_norm'))]
        available_forms = sorted({e.get('form') for e in entries if e.get('form')})

        diagnostics = {
            'requested_form': requested_form,
            'requested_form_norm': requested_norm,
            'requested_range': {'start_year': start_year, 'end_year': end_year},
            'available_forms_count': len(available_forms),
            'available_forms_sample': available_forms[:40],
            'attempts': [],
            'reason_chain': [],
            'fallback_used': None,
            'reason': None
        }

        def in_range(e):
            y = e.get('year')
            return y is not None and start_year <= y <= end_year

        # ordered retries for annual requests
        if requested_norm.startswith('10K'):
            attempts = [
                ('requested', lambda e: self._matches_requested_form(e.get('form_norm'), requested_norm)),
                ('10-K/A', lambda e: self._matches_requested_form(e.get('form_norm'), '10KA')),
                ('20-F', lambda e: self._matches_requested_form(e.get('form_norm'), '20F') or self._matches_requested_form(e.get('form_norm'), '20FA')),
            ]
        else:
            attempts = [
                ('requested', lambda e: self._matches_requested_form(e.get('form_norm'), requested_norm)),
            ]

        selected = []
        for name, predicate in attempts:
            bucket = [e for e in entries if predicate(e)]
            bucket_in_range = [e for e in bucket if in_range(e)]
            diagnostics['attempts'].append({
                'attempt': name,
                'matches_total': len(bucket),
                'matches_in_range': len(bucket_in_range)
            })
            diagnostics['reason_chain'].append(
                f"{name}: total={len(bucket)}, in_range={len(bucket_in_range)}"
            )
            if bucket_in_range:
                selected = bucket_in_range
                if name != 'requested':
                    diagnostics['fallback_used'] = name
                break

        # fallback to latest annual filing in-range
        if not selected:
            annual_in_range = [e for e in annual_forms if in_range(e)]
            diagnostics['attempts'].append({
                'attempt': 'latest_annual_in_range',
                'matches_total': len(annual_forms),
                'matches_in_range': len(annual_in_range)
            })
            diagnostics['reason_chain'].append(
                f"latest_annual_in_range: total={len(annual_forms)}, in_range={len(annual_in_range)}"
            )
            if annual_in_range:
                latest = sorted(annual_in_range, key=lambda e: (e.get('year') or 0, e.get('filing_date') or ''), reverse=True)[0]
                selected = [latest]
                diagnostics['fallback_used'] = 'latest_annual_in_range'
                diagnostics['filing_grade'] = 'IN_RANGE_ANNUAL'

        # final fallback: latest annual available (outside range)
        if not selected and annual_forms:
            latest_any = sorted(annual_forms, key=lambda e: (e.get('year') or 0, e.get('filing_date') or ''), reverse=True)[0]
            selected = [latest_any]
            diagnostics['fallback_used'] = 'latest_annual_available_outside_range'
            diagnostics['reason'] = 'no requested filing in range; using latest annual available'
            diagnostics['filing_grade'] = 'OUT_OF_RANGE_ANNUAL_FALLBACK'

        if not selected:
            diagnostics['reason'] = (
                f'no filings matched requested form {requested_form} '
                f'within {start_year}-{end_year}, and no annual fallback available'
            )

        # Build mapping for extraction
        accn_to_period = {}
        filtered = []
        for e in selected:
            accn_norm = self._normalize_accession(e.get('accession_number'))
            y = e.get('year')
            filtered.append({
                'form': e.get('form'),
                'filing_date': e.get('filing_date'),
                'accession_number': e.get('accession_number'),
                'primary_document': e.get('primary_document'),
                'year': y
            })
            if accn_norm and y is not None:
                accn_to_period[accn_norm] = {
                    'filing_year': y,
                    'filing_date': e.get('filing_date'),
                    'report_date': e.get('report_date'),
                    'form': e.get('form'),
                    'primary_document': e.get('primary_document'),
                }

        diagnostics['selected_filings'] = filtered
        if 'filing_grade' not in diagnostics:
            # classify based on selected set and range coverage
            in_range_selected = all((f.get('year') is not None and start_year <= f.get('year') <= end_year) for f in filtered) if filtered else False
            if in_range_selected:
                # requested annual or equivalent annual match
                if diagnostics.get('fallback_used') in (None, '10-K/A', '20-F'):
                    if diagnostics.get('fallback_used') == '20-F':
                        diagnostics['filing_grade'] = 'IN_RANGE_EQUIVALENT'
                    else:
                        diagnostics['filing_grade'] = 'IN_RANGE_ANNUAL'
                else:
                    diagnostics['filing_grade'] = 'IN_RANGE_EQUIVALENT'
            else:
                diagnostics['filing_grade'] = 'OUT_OF_RANGE_ANNUAL_FALLBACK'
        diagnostics['out_of_range'] = diagnostics.get('filing_grade') == 'OUT_OF_RANGE_ANNUAL_FALLBACK'
        diagnostics['max_ratio_grade'] = 'LOW' if diagnostics['out_of_range'] else 'HIGH'
        return filtered, accn_to_period, diagnostics

    def fetch_company_data(self, company_name, start_year, end_year, filing_type='10-K', callback=None, include_all_concepts=True):
        def update(msg):
            if callback:
                callback(msg)
            print(msg)

        try:
            update(f"Searching for: {company_name}")
            company_info = self.get_company_info(company_name)
            if not company_info:
                return {'success': False, 'error': f'Company not found: {company_name}'}

            cik = company_info['cik']
            name = company_info['name']
            ticker = company_info['ticker']
            update(f"Found: {name} ({ticker})")

            req_norm = self._normalize_form_code(filing_type)
            if not req_norm.startswith('10K'):
                return {'success': False, 'error': 'Direct truth-source mode supports 10-K only.'}
            if self.direct_engine is None:
                return {'success': False, 'error': 'Direct extraction engine is unavailable.'}

            # Fast path: return cached institutional result for same request window.
            cache_key = self._make_fetch_cache_key(
                ticker=ticker,
                start_year=int(start_year),
                end_year=int(end_year),
                filing_type=req_norm,
            )
            cached = (self._fetch_request_cache or {}).get(cache_key)
            if isinstance(cached, dict) and cached.get('success'):
                update('Using cached result for identical request...')
                return self._normalize_cached_fetch_result(cached)

            submissions_url = f"{self.base_url}/submissions/CIK{cik}.json"
            update('Loading submissions...')
            subs_key = str(cik).zfill(10)
            subs = (self._submissions_cache or {}).get(subs_key)
            if not isinstance(subs, dict):
                r = requests.get(submissions_url, headers=self.headers, timeout=30)
                if r.status_code != 200:
                    return {'success': False, 'error': f'SEC connection failed: HTTP {r.status_code}'}
                subs = r.json()
                try:
                    self._submissions_cache[subs_key] = subs
                    # keep bounded cache size
                    while len(self._submissions_cache) > 300:
                        oldest_key = next(iter(self._submissions_cache.keys()))
                        self._submissions_cache.pop(oldest_key, None)
                    self._save_submissions_cache()
                except Exception:
                    pass
            sic = str(subs.get('sic') or '')
            sic_description = str(subs.get('sicDescription') or '')
            recent = subs.get('filings', {}).get('recent', {})
            all_entries = self._collect_all_submission_entries(cik=cik, submissions_json=subs)
            filtered, accn_to_period, filing_diagnostics = self._select_filings_with_fallback(
                recent=recent,
                start_year=start_year,
                end_year=end_year,
                requested_form='10-K',
                entries_override=all_entries,
            )
            if not filtered:
                return {
                    'success': False,
                    'error': f'No 10-K filings found in range {start_year}-{end_year}',
                    'filing_diagnostics': filing_diagnostics,
                }
            strict_10k = []
            for f in filtered:
                if self._normalize_form_code(f.get('form')).startswith('10K'):
                    strict_10k.append(f)
            if not strict_10k:
                return {
                    'success': False,
                    'error': f'No strict 10-K filings found in range {start_year}-{end_year}',
                    'filing_diagnostics': filing_diagnostics,
                }
            # Full Period Integrity: add extra historical 10-K filings to backfill missing years.
            target_years = set(range(int(start_year), int(end_year) + 1))
            covered_years = set()
            for f in strict_10k:
                try:
                    y = int(f.get('year'))
                    covered_years.add(y)
                except Exception:
                    pass
            missing_years = sorted(y for y in target_years if y not in covered_years)
            if missing_years:
                extras = []
                for ent in (all_entries or []):
                    form_i = self._normalize_form_code(ent.get('form'))
                    if not form_i.startswith('10K'):
                        continue
                    filing_date = str(ent.get('filing_date') or '')
                    report_date = str(ent.get('report_date') or '')
                    year_i = self._safe_int(ent.get('year'))
                    if year_i is None:
                        continue
                    if year_i not in target_years:
                        continue
                    extras.append({
                        'form': ent.get('form'),
                        'accession_number': ent.get('accession_number'),
                        'filing_date': filing_date,
                        'period_end': report_date,
                        'year': year_i,
                    })
                existing_accn = {str(x.get('accession_number') or '') for x in strict_10k}
                for e in extras:
                    if str(e.get('accession_number') or '') in existing_accn:
                        continue
                    strict_10k.append(e)
                    existing_accn.add(str(e.get('accession_number') or ''))
            available_filing_years = set()
            for _f in strict_10k:
                try:
                    available_filing_years.add(int(_f.get('year')))
                except Exception:
                    continue

            safe_ticker = re.sub(r'[^A-Za-z0-9_.-]', '', str(ticker or '').strip().upper()) or 'UNKNOWN'
            out_csv = str(Path('exports/institutional') / f'{safe_ticker}_SEC_Official_Statement.csv')

            update('Running direct SEC 10-K multi-year extraction...')
            direct_meta = None
            data_by_year = {}
            try:
                direct_meta = self.direct_engine.extract_multi(
                    cik=cik,
                    filings=strict_10k,
                    output_csv=out_csv,
                    timeout=60,
                    required_years=None,
                    enforce_full_period=False,
                    period_start_year=int(start_year),
                    period_end_year=int(end_year),
                )
                update('Building ratios and strategic/AI inputs from direct SEC output...')
                data_by_year = self._build_data_by_year_from_direct_csv(
                    direct_meta.get('output_csv'),
                    start_year=int(start_year),
                    end_year=int(end_year),
                )
            except Exception as e:
                # Institutional fallback: continue with SEC companyfacts + smart backfill
                # instead of hard-failing the entire company.
                fallback_reason = str(e)
                update(f'Direct extraction fallback to SEC companyfacts: {fallback_reason}')
                direct_meta = {
                    'output_csv': None,
                    'fallback_mode': 'companyfacts_only',
                    'fallback_reason': fallback_reason,
                }
                data_by_year = {}
            data_by_year = self._sanitize_data_by_year_for_integrity(data_by_year)
            base_layers = self._build_data_layers(data_by_year) if data_by_year else {}
            fiscal_period_end_by_year = {}
            for filing in (strict_10k or []):
                try:
                    yy = int(filing.get('year'))
                except Exception:
                    continue
                period_end = str(filing.get('period_end') or '').strip()
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', period_end):
                    period_end = str(filing.get('filing_date') or '').strip()
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', period_end):
                    continue
                prev = fiscal_period_end_by_year.get(yy)
                if (prev is None) or (period_end > prev):
                    fiscal_period_end_by_year[yy] = period_end
            source_layers = self._build_source_layers(
                cik=cik,
                ticker=ticker,
                start_year=int(start_year),
                end_year=int(end_year),
                fiscal_period_end_by_year=fiscal_period_end_by_year,
            )
            extra_maps = source_layers.get('extra', {}) or {}
            data_layers = {
                'label_rows': (base_layers.get('label_rows') if base_layers else []) or [],
                'layer1_by_year': (base_layers.get('layer1_by_year') if base_layers else {}) or {},
                'layer2_by_year': source_layers.get('MARKET', {}) or {},
                'layer3_by_year': source_layers.get('MACRO', {}) or {},
                'layer4_by_year': source_layers.get('YAHOO', {}) or {},
                'extra_layers_by_year': extra_maps,
                'layer_sources': {
                    'layer1': 'SEC',
                    'layer2': 'MARKET',
                    'layer3': 'MACRO',
                    'layer4': 'YAHOO',
                    **{f'layer_extra_{k.lower()}': k for k in extra_maps.keys()},
                },
            }
            # Harmonize shares units in market/yahoo layers (shares_million canonical).
            try:
                l2_fixed, l2_diag = self._harmonize_layer_shares_units_to_million(
                    data_layers.get('layer2_by_year') or {},
                    layer_name="LAYER2_MARKET",
                )
                data_layers['layer2_by_year'] = l2_fixed
                if l2_diag:
                    data_layers.setdefault('layer2_diagnostics', {})['shares_unit_harmonization'] = l2_diag
            except Exception:
                pass
            try:
                l4_fixed, l4_diag = self._harmonize_layer_shares_units_to_million(
                    data_layers.get('layer4_by_year') or {},
                    layer_name="LAYER4_YAHOO",
                )
                data_layers['layer4_by_year'] = l4_fixed
                if l4_diag:
                    data_layers.setdefault('layer4_diagnostics', {})['shares_unit_harmonization'] = l4_diag
            except Exception:
                pass
            # Reconcile extreme Polygon/Yahoo divergences (price/market_cap) BEFORE ratios/strategic are computed.
            try:
                l2_rec, rec_diag = self._reconcile_market_layer_against_yahoo(
                    data_layers.get('layer2_by_year') or {},
                    data_layers.get('layer4_by_year') or {},
                )
                data_layers['layer2_by_year'] = l2_rec
                if rec_diag:
                    data_layers.setdefault('layer2_diagnostics', {})['market_vs_yahoo_reconciliation'] = rec_diag
            except Exception:
                pass
            # Backfill missing ratio-critical tags from SEC companyfacts payload.
            sec_payload = (source_layers.get('payloads', {}) or {}).get('SEC', {})
            data_layers['layer1_by_year'] = self._backfill_layer1_from_sec_payload(
                data_layers.get('layer1_by_year') or {},
                sec_payload,
            )
            # Direct companyconcept backfill (smart cross-year concept continuity)
            # is expensive; run only when core anchors are truly missing.
            if self._needs_companyconcept_backfill(
                data_layers.get('layer1_by_year') or {},
                start_year=int(start_year),
                end_year=int(end_year),
            ):
                update('Applying smart SEC companyconcept backfill (needed)...')
                data_layers['layer1_by_year'] = self._apply_smart_companyconcept_backfill(
                    data_layers.get('layer1_by_year') or {},
                    cik_padded=str(cik).zfill(10),
                    start_year=int(start_year),
                    end_year=int(end_year),
                )
            else:
                update('Skipping smart backfill (core anchors already complete).')
            data_layers['layer1_by_year'] = self._sanitize_data_by_year_for_integrity(
                data_layers.get('layer1_by_year') or {}
            )
            layer_catalog = [
                {'key': 'layer1_by_year', 'title': 'Layer 1 - SEC XBRL (EDGAR)', 'source': 'SEC'},
                {'key': 'layer2_by_year', 'title': 'Layer 2 - Market (Polygon)', 'source': 'MARKET'},
                {'key': 'layer3_by_year', 'title': 'Layer 3 - Macro (FRED)', 'source': 'MACRO'},
                {'key': 'layer4_by_year', 'title': 'Layer 4 - Yahoo (yfinance)', 'source': 'YAHOO'},
            ]
            for extra_name in sorted(extra_maps.keys()):
                layer_catalog.append({
                    'key': f'extra::{extra_name}',
                    'title': f'Layer {len(layer_catalog)+1} - {extra_name}',
                    'source': extra_name,
                })
            data_layers['layer_catalog'] = layer_catalog
            if fiscal_period_end_by_year:
                data_layers['fiscal_period_end_by_year'] = fiscal_period_end_by_year
            data_layers['layer_definitions'] = self._build_layer_definitions(
                data_layers=data_layers,
                start_year=int(start_year),
                end_year=int(end_year),
            )
            layer1_for_calc = self._enrich_layer2_parent_totals((data_layers.get('layer1_by_year') if data_layers else {}) or {})
            layer1_for_calc, missing_filing_years = self._enforce_filing_year_integrity(
                layer1_for_calc,
                start_year=int(start_year),
                end_year=int(end_year),
                filing_years=available_filing_years,
            )
            # Keep reporting metadata about missing years, but avoid emitting
            # empty synthetic yearly rows as "latest financial year".
            if missing_filing_years:
                layer1_for_calc = {
                    int(y): dict(row or {})
                    for y, row in (layer1_for_calc or {}).items()
                    if int(y) not in set(missing_filing_years)
                }
            filing_diagnostics = dict(filing_diagnostics or {})
            filing_diagnostics['available_filing_years'] = sorted(list(available_filing_years))
            filing_diagnostics['missing_filing_years'] = list(missing_filing_years)
            if data_layers is not None:
                data_layers['layer1_by_year'] = dict(layer1_for_calc)
                data_layers['missing_filing_years'] = list(missing_filing_years)
                data_layers['available_filing_years'] = sorted(list(available_filing_years))

            # Phase 3 prerequisite: harmonize statement money units to USD_million when a clear mismatch exists.
            try:
                layer1_for_calc, money_unit_diagnostics = self._harmonize_layer1_money_units_to_million(layer1_for_calc or {})
                if data_layers is not None and isinstance(data_layers.get('layer_definitions'), dict):
                    data_layers.setdefault('unit_harmonization', {})
                    data_layers['unit_harmonization']['money_usd_million'] = money_unit_diagnostics or {}
                # Keep exported Layer1 view consistent with the calculation layer (avoid mixed-unit display).
                if data_layers is not None:
                    data_layers['layer1_by_year'] = dict(layer1_for_calc or {})
            except Exception:
                pass
            # Phase 3b prerequisite: reconcile extreme balance-sheet total mismatches (non-silent; diagnostics logged).
            try:
                layer1_for_calc, bs_diag = self._reconcile_balance_sheet_totals(layer1_for_calc or {})
                if data_layers is not None and isinstance(data_layers.get('layer_definitions'), dict):
                    data_layers.setdefault('unit_harmonization', {})
                    data_layers['unit_harmonization']['balance_sheet_totals'] = bs_diag or {}
                if data_layers is not None:
                    data_layers['layer1_by_year'] = dict(layer1_for_calc or {})
            except Exception:
                pass
            # Phase 3c prerequisite: harmonize within-concept mixed scales across years (e.g., deposits in bn vs mm).
            try:
                layer1_for_calc, series_diag = self._harmonize_layer1_series_scale_to_million(layer1_for_calc or {})
                if data_layers is not None and isinstance(data_layers.get('layer_definitions'), dict):
                    data_layers.setdefault('unit_harmonization', {})
                    data_layers['unit_harmonization']['series_scale_to_million'] = series_diag or {}
                if data_layers is not None:
                    data_layers['layer1_by_year'] = dict(layer1_for_calc or {})
            except Exception:
                pass
            data_quality_warnings = self._collect_data_quality_warnings(ticker=ticker, data_by_year=layer1_for_calc)
            self._active_cik_padded = str(cik).zfill(10)
            self._active_ticker = str(ticker or '').upper()
            self._active_start_year = int(start_year)
            self._active_end_year = int(end_year)
            financial_ratios = self._calculate_financial_ratios(layer1_for_calc) if layer1_for_calc else {}
            strategic_analysis = self.generate_strategic_analysis(layer1_for_calc, financial_ratios) if layer1_for_calc else {}

            core_ratio_results = {}
            core_strategy_results = {}
            if CoreRatioEngine is not None:
                try:
                    layer_payloads = {}
                    sec_payload = (source_layers.get('payloads', {}) or {}).get('SEC')
                    mkt_payload = (source_layers.get('payloads', {}) or {}).get('MARKET')
                    mac_payload = (source_layers.get('payloads', {}) or {}).get('MACRO')
                    yah_payload = (source_layers.get('payloads', {}) or {}).get('YAHOO')
                    if isinstance(sec_payload, dict):
                        layer_payloads['SEC'] = sec_payload
                    if isinstance(mkt_payload, dict):
                        layer_payloads['MARKET'] = mkt_payload
                    if isinstance(mac_payload, dict):
                        layer_payloads['MACRO'] = mac_payload
                    if isinstance(yah_payload, dict):
                        layer_payloads['YAHOO'] = yah_payload

                    if layer_payloads.get('SEC'):
                        ratio_engine = CoreRatioEngine(layer_registry={})
                        strategy_engine = CoreStrategyEngine() if CoreStrategyEngine is not None else None
                        for y in range(int(start_year), int(end_year) + 1):
                            ratio_out = ratio_engine.compute_all(layer_payloads=layer_payloads, year=y)
                            core_ratio_results[y] = ratio_out

                            # Bridge computed ratios into legacy map to fill missing calculations.
                            legacy_row = financial_ratios.setdefault(y, {})
                            bridge_aliases = {
                                'debt_ratio': 'debt_to_assets',
                            }
                            for rname, robj in (ratio_out.get('ratio_results', {}) or {}).items():
                                if isinstance(robj, dict) and robj.get('status') == 'COMPUTED':
                                    if legacy_row.get(rname) is None:
                                        legacy_row[rname] = robj.get('value')
                                    alias_name = bridge_aliases.get(rname)
                                    if alias_name:
                                        if legacy_row.get(alias_name) is None:
                                            legacy_row[alias_name] = robj.get('value')
                                else:
                                    legacy_row.setdefault(rname, None)
                                    alias_name = bridge_aliases.get(rname)
                                    if alias_name:
                                        legacy_row.setdefault(alias_name, None)

                            if strategy_engine is not None:
                                core_strategy_results[y] = strategy_engine.analyze(
                                    ratio_results=(ratio_out.get('ratio_results') or {}),
                                    layer_payloads=layer_payloads,
                                    year=y,
                                )
                except Exception:
                    core_ratio_results = {}
                    core_strategy_results = {}

            # Final market/yahoo merge pass for market-dependent display ratios.
            financial_ratios = self._inject_market_ratios_from_layers(
                financial_ratios=financial_ratios,
                data_by_year=layer1_for_calc,
                data_layers=data_layers,
            )
            financial_ratios = self._clear_missing_filing_year_ratios(financial_ratios, missing_filing_years)

            sector_profile = self._resolve_sector_profile(
                name,
                ticker,
                layer1_for_calc,
                sic_description=sic_description,
            )
            sub_sector_profile = self._resolve_sub_sector_profile(
                name,
                ticker,
                sector_profile,
                sic_description=sic_description,
            )
            # Canonical classification SSOT: no legacy labels, no dangerous fallbacks.
            canonical_cls = build_canonical_classification(
                ticker=ticker,
                company_name=name,
                sic=sic,
                naics='',
                sic_description=sic_description,
                sector_profile_hint=sector_profile,
                sub_sector_profile_hint=sub_sector_profile,
                institutional_primary_profile='',
                institutional_diag={},
            ).to_dict()
            canonical_sg = canonical_sector_gating_from_classification(canonical_cls)
            sector_profile = canonical_sg.get('profile') or 'unknown'
            sub_sector_profile = canonical_sg.get('sub_profile') or sector_profile
            canonical_debug_path = write_classification_debug(
                canonical_classification=canonical_cls,
                outputs_dir="outputs",
                ticker=ticker,
            )
            try:
                update(
                    f"Canonical classification: "
                    f"{canonical_cls.get('sector_family')} / {canonical_cls.get('operating_sub_sector')} "
                    f"(peer_group={canonical_cls.get('peer_group')}, conf={canonical_cls.get('classification_confidence')}, src={canonical_cls.get('classification_source')})"
                )
            except Exception:
                pass

            # Stamp sector profile into each yearly row so downstream ratio resolver can
            # apply sector-aware canonical mapping deterministically.
            for _y, _row in (layer1_for_calc or {}).items():
                if isinstance(_row, dict):
                    _row['__sector_profile__'] = sector_profile
                    _row['__sub_sector_profile__'] = sub_sector_profile
                    _row.setdefault('__statement_tree_required__', True)
            statement_tree_diagnostics = self._write_direct_statement_diagnostics(layer1_for_calc or {})
            self._learn_sector_profile(ticker=ticker, sic_description=sic_description, sector_profile=sector_profile)
            financial_ratios = self._apply_sector_ratio_gating(financial_ratios, sector_profile, sub_sector_profile)
            sector_gating = {
                'profile': sector_profile,
                'sub_profile': sub_sector_profile,
                'blocked_ratios': sorted(list(self._resolve_sector_ratio_blocklist(sector_profile, sub_sector_profile))),
                'blocked_strategic_metrics': sorted(list(self._resolve_sector_strategic_blocklist(sector_profile, sub_sector_profile))),
            }
            financial_analysis_system = self._run_financial_analysis_system(
                ticker=ticker,
                data_by_year=layer1_for_calc,
                financial_ratios=financial_ratios,
                canonical_classification=canonical_cls,
            )

            audit_pack = None
            audit_pack_path = None
            try:
                # Phase 1: diagnostics only (no UI/Excel presentation changes).
                audit_pack = build_institutional_audit_pack(
                    ticker=ticker,
                    period=f"{start_year}-{end_year}",
                    data_by_year=layer1_for_calc or {},
                    financial_ratios=financial_ratios or {},
                    canonical_money_unit="usd_million",
                    canonical_shares_unit="shares_million",
                )
                audit_pack_path = write_audit_pack_to_outputs(audit_pack=audit_pack, outputs_dir="outputs")
                update(f"Audit pack written: {audit_pack_path}")
            except Exception:
                audit_pack = None
                audit_pack_path = None

            result = {
                'success': True,
                'company_info': {
                    'name': name,
                    'ticker': ticker,
                    'cik': cik,
                    'sic': sic,
                    'sic_description': sic_description,
                },
                'filing_type': '10-K',
                'period': f"{start_year}-{end_year}",
                'filings_count': len(strict_10k),
                'selected_filings': strict_10k,
                'filing_diagnostics': filing_diagnostics,
                'financial_items_by_concept': {},
                'data_by_period': {},
                'data_by_year': layer1_for_calc,
                'accounting_hierarchy_diagnostics': {},
                'statement_tree_diagnostics': statement_tree_diagnostics or {},
                'data_layers': data_layers,
                'financial_ratios': financial_ratios,
                'strategic_analysis': strategic_analysis,
                'dynamic_mappings': {},
                'source_layer_payloads': source_layers.get('payloads', {}),
                'data_quality_warnings': data_quality_warnings,
                'core_ratio_results': core_ratio_results,
                'core_strategy_results': core_strategy_results,
                'sector_gating': sector_gating,
                'canonical_classification': canonical_cls,
                'canonical_classification_debug_path': canonical_debug_path,
                'financial_analysis_system': financial_analysis_system,
                'audit_pack': audit_pack,
                'audit_pack_path': audit_pack_path,
                'institutional_outputs': {'direct_extraction': direct_meta},
                'institutional_saved_files': {'sec_official_statement': direct_meta.get('output_csv')},
            }
            # Persist cache for identical future requests (same ticker/window/form).
            try:
                self._fetch_request_cache[cache_key] = copy.deepcopy(result)
                # Bound cache size to keep file lightweight and startup fast.
                while len(self._fetch_request_cache) > 20:
                    oldest_key = next(iter(self._fetch_request_cache.keys()))
                    self._fetch_request_cache.pop(oldest_key, None)
                self._save_fetch_request_cache()
            except Exception:
                pass
            update('Direct extraction completed successfully.')
            return result
        except RuntimeError as e:
            return {'success': False, 'error': str(e)}
        except Exception as e:
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'traceback': traceback.format_exc(),
            }

    @staticmethod
    def _xbrl_local_name(tag):
        if not isinstance(tag, str):
            return ''
        if '}' in tag:
            return tag.rsplit('}', 1)[-1]
        return tag

    @staticmethod
    def _xbrl_concept_from_href(href):
        if not isinstance(href, str) or '#' not in href:
            return None
        frag = href.split('#', 1)[-1]
        if ':' in frag:
            return frag.split(':', 1)[-1]
        return frag

    @staticmethod
    def _normalize_unit_code(unit):
        if not isinstance(unit, str):
            return None
        u = unit.strip().upper()
        if not u:
            return None
        if 'USD' in u:
            return 'USD'
        if u.startswith('ISO4217:'):
            return u.split(':', 1)[-1]
        return u

    @staticmethod
    def _concept_aliases(concept_name):
        if not isinstance(concept_name, str) or not concept_name:
            return []
        base = concept_name.split(':', 1)[-1]
        return [concept_name, base]

    def _row_value_for_concept(self, row, concept_name):
        if not isinstance(row, dict):
            return None
        for alias in self._concept_aliases(concept_name):
            v = row.get(alias)
            if isinstance(v, (int, float)):
                return float(v)
        return None

    def _xbrl_cache_dir(self, cik, filing):
        accn = self._normalize_accession((filing or {}).get('accession_number'))
        cik_norm = str(cik).zfill(10)
        path = Path('exports/xbrl_cache') / cik_norm / (accn or 'unknown_accession')
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _download_cached_text(self, url, cache_path):
        cache_path = Path(cache_path)
        if cache_path.exists():
            return cache_path.read_text(encoding='utf-8', errors='ignore')
        req_headers = dict(self.headers or {})
        req_headers.pop('Host', None)
        r = requests.get(url, headers=req_headers, timeout=30)
        if r.status_code != 200:
            return None
        txt = r.text
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(txt, encoding='utf-8')
        return txt

    def _download_cached_bytes(self, url, cache_path):
        cache_path = Path(cache_path)
        if cache_path.exists():
            return cache_path.read_bytes()
        req_headers = dict(self.headers or {})
        req_headers.pop('Host', None)
        r = requests.get(url, headers=req_headers, timeout=30)
        if r.status_code != 200:
            return None
        data = r.content
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(data)
        return data

    @staticmethod
    def _looks_like_instance_xml(xml_text):
        if not isinstance(xml_text, str) or '<' not in xml_text:
            return False
        head = xml_text[:4000].lower()
        # Keep this permissive: some instance docs place <unit> later in the file.
        return ('xbrl' in head and 'context' in head)

    @staticmethod
    def _pick_statement_concepts(metric):
        m = (metric or '').lower()
        mapping = {
            'revenue': ['revenue', 'sales', 'netrevenue'],
            'gross_profit': ['grossprofit'],
            'operating_income': ['operatingincome', 'incomefromoperations'],
            'net_income': ['netincome', 'profitloss'],
            'annual_cogs': ['costofrevenue', 'costofgoods', 'costofsales'],
        }
        return mapping.get(m, [])

    def _concept_match_score(self, concept_name, metric):
        cname = str(concept_name or '').lower()
        kws = self._pick_statement_concepts(metric)
        score = 0
        for kw in kws:
            if kw in cname:
                score += 1
        if metric == 'revenue':
            if any(x in cname for x in ('costofrevenue', 'costofsales', 'costofgoods')):
                score -= 3
            if 'revenue' in cname and 'cost' not in cname:
                score += 2
        if metric == 'annual_cogs':
            if any(x in cname for x in ('costofrevenue', 'costofsales', 'costofgoods')):
                score += 2
        return score

    def _fetch_statement_linkbases(self, cik, filing):
        accn = self._normalize_accession((filing or {}).get('accession_number'))
        if not accn:
            return {'presentation_xml': None, 'calculation_xml': None, 'instance_xml': None, 'index_url': None}
        try:
            cik_num = str(int(str(cik)))
        except Exception:
            cik_num = str(cik).lstrip('0') or str(cik)
        cache_dir = self._xbrl_cache_dir(cik, filing)
        index_cache_path = cache_dir / 'index.json'
        archive_hosts = ['https://www.sec.gov', 'https://data.sec.gov']
        index_attempts = []
        index_url = None
        idx_txt = None
        for host in archive_hosts:
            candidate_url = f"{host}/Archives/edgar/data/{cik_num}/{accn}/index.json"
            try:
                req_headers = dict(self.headers or {})
                req_headers.pop('Host', None)
                r = requests.get(candidate_url, headers=req_headers, timeout=30)
                index_attempts.append({'url': candidate_url, 'status_code': int(r.status_code)})
                if r.status_code == 200 and isinstance(r.text, str) and r.text.strip().startswith('{'):
                    idx_txt = r.text
                    index_url = candidate_url
                    index_cache_path.parent.mkdir(parents=True, exist_ok=True)
                    index_cache_path.write_text(idx_txt, encoding='utf-8')
                    break
            except Exception as e:
                index_attempts.append({'url': candidate_url, 'status_code': None, 'error': str(e)})
        if idx_txt is None and index_cache_path.exists():
            idx_txt = index_cache_path.read_text(encoding='utf-8', errors='ignore')
            index_url = str(index_cache_path)
        if not idx_txt:
            return {
                'presentation_xml': None,
                'calculation_xml': None,
                'instance_xml': None,
                'index_url': index_url,
                'index_attempts': index_attempts,
                'cache_dir': str(cache_dir),
            }
        payload = json.loads(idx_txt)
        items = (((payload or {}).get('directory') or {}).get('item') or [])
        pre_file = None
        cal_file = None
        instance_file = None
        xbrl_zip = None
        instance_candidates = []
        for it in items:
            name = (it or {}).get('name')
            if not isinstance(name, str):
                continue
            lname = name.lower()
            if pre_file is None and lname.endswith('.xml') and ('_pre' in lname or 'presentation' in lname):
                pre_file = name
            if cal_file is None and lname.endswith('.xml') and ('_cal' in lname or 'calculation' in lname):
                cal_file = name
            if lname.endswith('.zip') and 'xbrl' in lname:
                xbrl_zip = name
            if lname.endswith('.xml'):
                if any(x in lname for x in ('_pre', '_cal', '_def', '_lab', 'filingsummary')):
                    continue
                instance_candidates.append(name)
        base_dir = (index_url or f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{accn}/index.json").rsplit('/', 1)[0]
        pre_xml = None
        cal_xml = None
        instance_xml = None
        if pre_file:
            pre_xml = self._download_cached_text(f"{base_dir}/{pre_file}", cache_dir / pre_file)
        if cal_file:
            cal_xml = self._download_cached_text(f"{base_dir}/{cal_file}", cache_dir / cal_file)
        for cand in instance_candidates:
            txt = self._download_cached_text(f"{base_dir}/{cand}", cache_dir / cand)
            if self._looks_like_instance_xml(txt):
                instance_file = cand
                instance_xml = txt
                break
        if instance_xml is None and xbrl_zip:
            zbytes = self._download_cached_bytes(f"{base_dir}/{xbrl_zip}", cache_dir / xbrl_zip)
            if zbytes:
                try:
                    zf = zipfile.ZipFile(io.BytesIO(zbytes))
                    for name in zf.namelist():
                        lname = name.lower()
                        if not lname.endswith('.xml'):
                            continue
                        with zf.open(name) as fh:
                            txt = fh.read().decode('utf-8', errors='ignore')
                        if pre_xml is None and ('_pre' in lname or 'presentation' in lname):
                            pre_xml = txt
                            pre_file = pre_file or name
                        if cal_xml is None and ('_cal' in lname or 'calculation' in lname):
                            cal_xml = txt
                            cal_file = cal_file or name
                        if instance_xml is None and self._looks_like_instance_xml(txt):
                            instance_xml = txt
                            instance_file = instance_file or name
                    # materialize extracted payloads in cache for reproducibility
                    if pre_xml and pre_file and not (cache_dir / pre_file).exists():
                        (cache_dir / pre_file).write_text(pre_xml, encoding='utf-8')
                    if cal_xml and cal_file and not (cache_dir / cal_file).exists():
                        (cache_dir / cal_file).write_text(cal_xml, encoding='utf-8')
                    if instance_xml and instance_file and not (cache_dir / instance_file).exists():
                        (cache_dir / instance_file).write_text(instance_xml, encoding='utf-8')
                except Exception:
                    pass
        return {
            'presentation_xml': pre_xml,
            'calculation_xml': cal_xml,
            'instance_xml': instance_xml,
            'index_url': index_url,
            'index_attempts': index_attempts,
            'presentation_file': pre_file,
            'calculation_file': cal_file,
            'instance_file': instance_file,
            'xbrl_zip': xbrl_zip,
            'cache_dir': str(cache_dir),
        }

    def _parse_presentation_linkbase(self, xml_text):
        if not isinstance(xml_text, str) or not xml_text.strip():
            return {}
        root = ET.fromstring(xml_text)
        xlink_ns = '{http://www.w3.org/1999/xlink}'
        by_role = {}
        for link in root.iter():
            if self._xbrl_local_name(link.tag) != 'presentationLink':
                continue
            role_uri = link.attrib.get(f'{xlink_ns}role') or 'unknown_role'
            loc_map = {}
            children_map = {}
            parent_map = {}
            for node in list(link):
                nname = self._xbrl_local_name(node.tag)
                if nname == 'loc':
                    label = node.attrib.get(f'{xlink_ns}label')
                    href = node.attrib.get(f'{xlink_ns}href')
                    concept = self._xbrl_concept_from_href(href)
                    if label and concept:
                        loc_map[label] = concept
                elif nname == 'presentationArc':
                    src = node.attrib.get(f'{xlink_ns}from')
                    dst = node.attrib.get(f'{xlink_ns}to')
                    parent = loc_map.get(src)
                    child = loc_map.get(dst)
                    if not parent or not child:
                        continue
                    children_map.setdefault(parent, [])
                    if child not in children_map[parent]:
                        children_map[parent].append(child)
                    parent_map[child] = parent
            all_nodes = set(children_map.keys()) | set(parent_map.keys())
            roots = [n for n in all_nodes if n not in parent_map]
            depth_map = {}
            stack = [(r, 0) for r in roots]
            while stack:
                cur, depth = stack.pop()
                if cur in depth_map and depth_map[cur] <= depth:
                    continue
                depth_map[cur] = depth
                for ch in children_map.get(cur, []):
                    stack.append((ch, depth + 1))
            nodes = []
            for concept in sorted(all_nodes):
                nodes.append({
                    'concept_name': concept,
                    'label': concept,
                    'role_uri': role_uri,
                    'depth': int(depth_map.get(concept, 0)),
                    'parent_concept': parent_map.get(concept),
                    'children_concepts': list(children_map.get(concept, [])),
                })
            by_role[role_uri] = {
                'nodes': nodes,
                'children_map': children_map,
                'parent_map': parent_map,
            }
        return by_role

    def _parse_calculation_linkbase(self, xml_text):
        if not isinstance(xml_text, str) or not xml_text.strip():
            return {}
        root = ET.fromstring(xml_text)
        xlink_ns = '{http://www.w3.org/1999/xlink}'
        by_role = {}
        for link in root.iter():
            if self._xbrl_local_name(link.tag) != 'calculationLink':
                continue
            role_uri = link.attrib.get(f'{xlink_ns}role') or 'unknown_role'
            loc_map = {}
            arcs = []
            for node in list(link):
                nname = self._xbrl_local_name(node.tag)
                if nname == 'loc':
                    label = node.attrib.get(f'{xlink_ns}label')
                    href = node.attrib.get(f'{xlink_ns}href')
                    concept = self._xbrl_concept_from_href(href)
                    if label and concept:
                        loc_map[label] = concept
                elif nname == 'calculationArc':
                    src = node.attrib.get(f'{xlink_ns}from')
                    dst = node.attrib.get(f'{xlink_ns}to')
                    parent = loc_map.get(src)
                    child = loc_map.get(dst)
                    if not parent or not child:
                        continue
                    try:
                        w = float(node.attrib.get('weight', '1'))
                    except Exception:
                        w = 1.0
                    arcs.append({'parent': parent, 'child': child, 'weight': w})
            children_by_parent = {}
            for a in arcs:
                children_by_parent.setdefault(a['parent'], []).append({'concept': a['child'], 'weight': a['weight']})
            by_role[role_uri] = {'arcs': arcs, 'children_by_parent': children_by_parent}
        return by_role

    @staticmethod
    def _role_kind(role_uri):
        txt = str(role_uri or '').lower()
        if ('cashflow' in txt) or ('cashflows' in txt):
            return 'cash_flow'
        if ('balancesheet' in txt) or ('balancesheets' in txt) or ('financialposition' in txt):
            return 'balance_sheet'
        if ('income' in txt) or ('operations' in txt) or ('earnings' in txt) or ('profitandloss' in txt):
            return 'income_statement'
        return None

    def _choose_role_for_metric(self, metric_id, presentation_by_role):
        metric_role_map = {
            'annual_revenue': 'income_statement',
            'annual_cogs': 'income_statement',
            'gross_profit': 'income_statement',
            'operating_income': 'income_statement',
            'net_income': 'income_statement',
            'operating_cash_flow': 'cash_flow',
            'capital_expenditures': 'cash_flow',
            'total_equity': 'balance_sheet',
            'total_assets': 'balance_sheet',
            'accounts_receivable': 'balance_sheet',
            'accounts_payable': 'balance_sheet',
            'inventory': 'balance_sheet',
        }
        desired = metric_role_map.get(metric_id)
        if not desired:
            return None
        for role_uri in presentation_by_role.keys():
            if self._role_kind(role_uri) == desired:
                return role_uri
        return None

    @staticmethod
    def _metric_keywords(metric_id):
        return {
            'annual_revenue': ['revenue', 'sales'],
            'annual_cogs': ['costofrevenue', 'costofgoods', 'costofsales'],
            'gross_profit': ['grossprofit'],
            'operating_income': ['operatingincome', 'incomefromoperations'],
            'net_income': ['netincome', 'profitloss'],
            'operating_cash_flow': ['netcashprovidedbyusedinoperatingactivities'],
            'capital_expenditures': ['paymentstoacquirepropertyplantandequipment', 'capitalexpenditures'],
            'total_equity': ['stockholdersequity', 'totalequity'],
            'total_assets': ['assets'],
            'accounts_receivable': ['accountsreceivable'],
            'accounts_payable': ['accountspayable'],
            'inventory': ['inventory'],
        }.get(metric_id, [])

    def _match_parent_concept_for_metric(self, metric_id, role_payload):
        nodes = (role_payload or {}).get('nodes') or []
        if not nodes:
            return None
        kws = [k.lower() for k in self._metric_keywords(metric_id)]
        if not kws:
            return None
        scored = []
        for n in nodes:
            cname = str(n.get('concept_name') or '').lower()
            score = 0
            for kw in kws:
                if kw in cname:
                    score += 1
            if metric_id == 'annual_revenue':
                if 'costofrevenue' in cname or 'costofgoods' in cname or 'costofsales' in cname:
                    score -= 3
                if 'revenue' in cname and 'cost' not in cname:
                    score += 2
            if metric_id == 'annual_cogs':
                if 'costofrevenue' in cname or 'costofgoods' in cname or 'costofsales' in cname:
                    score += 2
                if cname.endswith('revenues') or cname.endswith('revenue'):
                    score -= 2
            if score > 0:
                scored.append((score, n.get('depth', 99), n.get('concept_name')))
        if not scored:
            return None
        scored.sort(key=lambda t: (-t[0], t[1]))
        return scored[0][2]

    def _build_concept_year_meta(self, items_by_concept, year):
        meta = {}
        for concept, by_period in (items_by_concept or {}).items():
            if not isinstance(by_period, dict):
                continue
            best = None
            for _pk, m in by_period.items():
                if not isinstance(m, dict):
                    continue
                fy = m.get('fiscal_year')
                if fy != year:
                    continue
                ptype = m.get('period_type') or ('INSTANT' if str(m.get('fiscal_period', '')).upper() == 'FY' and str(_pk).endswith('-FY') and 'Assets' in str(concept) else 'DURATION')
                rank = 0
                fp = str(m.get('fiscal_period') or '').upper()
                if fp == 'FY':
                    rank += 20
                if ptype == 'DURATION':
                    rank += 10
                filed = m.get('filed') or ''
                if best is None or rank > best[0] or (rank == best[0] and filed > best[1].get('filed', '')):
                    best = (rank, m)
            if best is not None:
                bm = dict(best[1])
                bm['unit_normalized'] = self._normalize_unit_code(bm.get('unit'))
                meta[concept] = bm
        return meta

    def _context_compatible(self, parent_meta, child_meta):
        if not isinstance(parent_meta, dict) or not isinstance(child_meta, dict):
            return False, 'context_mismatch'
        pend = parent_meta.get('period_end')
        cend = child_meta.get('period_end')
        if pend and cend and pend != cend:
            return False, 'context_mismatch'
        ptype = str(parent_meta.get('period_type') or '').upper()
        ctype = str(child_meta.get('period_type') or '').upper()
        if ptype and ctype and ptype != ctype:
            return False, 'context_mismatch'
        pu = parent_meta.get('unit_normalized')
        cu = child_meta.get('unit_normalized')
        if pu and cu and pu != cu:
            return False, 'context_mismatch'
        return True, None

    @staticmethod
    def _is_abstract_or_subtotal(concept):
        txt = str(concept or '').lower()
        if 'abstract' in txt:
            return True
        if 'subtotal' in txt:
            return True
        return False

    def _apply_statement_tree_intelligence(self, cik, selected_filings, items_by_concept, data_by_year):
        diagnostics = {'years': {}, 'files': [], 'strict_mode': False}
        reconciliation_rows = []
        reconciliation_by_year = {}
        if not selected_filings:
            return diagnostics

        statement_metrics = {
            'revenue': 'annual_revenue',
            'gross_profit': 'gross_profit',
            'operating_income': 'operating_income',
            'net_income': 'net_income',
            'annual_cogs': 'annual_cogs',
        }
        def normalize_concept_name(concept_name):
            c = str(concept_name or '')
            if not c:
                return c
            if ':' in c:
                c = c.split(':', 1)[1]
            if '_' in c:
                # Presentation/calculation href concepts often look like: us-gaap_Revenue...
                c = c.split('_', 1)[1]
            return c

        for filing in selected_filings:
            year = filing.get('year')
            if not isinstance(year, int):
                continue
            row = (data_by_year or {}).get(year)
            if not isinstance(row, dict):
                continue
            row.setdefault('__canonical_fact_candidates__', {})
            link_payload = self._fetch_statement_linkbases(cik, filing)
            diagnostics['files'].append({
                'year': year,
                'accession': filing.get('accession_number'),
                'cache_dir': link_payload.get('cache_dir'),
                'index_url': link_payload.get('index_url'),
                'index_attempts': link_payload.get('index_attempts'),
                'instance_file': link_payload.get('instance_file'),
                'presentation_file': link_payload.get('presentation_file'),
                'calculation_file': link_payload.get('calculation_file'),
                'xbrl_zip': link_payload.get('xbrl_zip'),
            })

            pre = parse_presentation_linkbase(link_payload.get('presentation_xml'))
            cal = parse_calculation_linkbase(link_payload.get('calculation_xml'))
            inst = parse_instance_xbrl(link_payload.get('instance_xml'))
            facts_by_concept = inst.get('facts_by_concept') or {}
            facts_by_concept_lc = {str(k).lower(): v for k, v in (facts_by_concept or {}).items()}

            def concept_facts(concept_name):
                raw = str(concept_name or '')
                if not raw:
                    return []
                candidates = []
                for cand in (raw, normalize_concept_name(raw)):
                    if not cand:
                        continue
                    if cand not in candidates:
                        candidates.append(cand)
                for cand in candidates:
                    if cand in facts_by_concept:
                        return facts_by_concept.get(cand) or []
                for cand in candidates:
                    hit = facts_by_concept_lc.get(cand.lower())
                    if hit:
                        return hit
                return []

            # if filing-level tree unavailable, fallback to old companyfacts but mark very low trust.
            if not pre or not facts_by_concept:
                for metric, canonical_key in statement_metrics.items():
                    fallback_val = None
                    fallback_tag = None
                    if metric == 'revenue':
                        for tag in ('Revenue_Hierarchy', 'NetRevenue_Hierarchy', 'Revenues', 'SalesRevenueNet'):
                            v = self._row_value_for_concept(row, tag)
                            if v is not None:
                                fallback_tag, fallback_val = tag, v
                                break
                    elif metric == 'annual_cogs':
                        for tag in ('CostOfRevenue', 'CostOfGoodsAndServicesSold'):
                            v = self._row_value_for_concept(row, tag)
                            if v is not None:
                                fallback_tag, fallback_val = tag, v
                                break
                    elif metric == 'gross_profit':
                        for tag in ('GrossProfit',):
                            v = self._row_value_for_concept(row, tag)
                            if v is not None:
                                fallback_tag, fallback_val = tag, v
                                break
                    elif metric == 'operating_income':
                        for tag in ('OperatingIncomeLoss',):
                            v = self._row_value_for_concept(row, tag)
                            if v is not None:
                                fallback_tag, fallback_val = tag, v
                                break
                    elif metric == 'net_income':
                        for tag in ('NetIncomeLoss', 'ProfitLoss'):
                            v = self._row_value_for_concept(row, tag)
                            if v is not None:
                                fallback_tag, fallback_val = tag, v
                                break
                    row['__canonical_fact_candidates__'][canonical_key] = []
                    if fallback_val is not None:
                        row['__canonical_fact_candidates__'][canonical_key].append({
                            'tag': fallback_tag,
                            'value': fallback_val,
                            'period_type': 'FY',
                            'period_end': None,
                            'unit': 'USD',
                            'selection_reason': 'fallback_companyfacts_no_statement_anchor',
                            'parent_child_mismatch_pct': None,
                        })
                    reconciliation_rows.append({
                        'year': year,
                        'metric': canonical_key,
                        'chosen_concept': fallback_tag,
                        'chosen_label': fallback_tag,
                        'source_context_id': None,
                        'period_end': None,
                        'value_used': fallback_val,
                        'SEC_HTML_table_value': None,
                        'mismatch_pct': None,
                        'reliability': 20 if fallback_val is not None else 0,
                        'reason': 'fallback_companyfacts_no_statement_anchor',
                    })
                diagnostics['years'][str(year)] = {
                    'anchor': None,
                    'reason': 'fallback_companyfacts_no_statement_anchor',
                    'primary_income_role': None,
                }
                continue

            diagnostics['strict_mode'] = True
            row['__statement_tree_required__'] = True
            primary_income_role = select_income_statement_role(pre, facts_by_concept) or pick_primary_role(pre, 'income_statement')
            role_payload = pre.get(primary_income_role or '') or {}
            role_nodes = role_payload.get('nodes') or []
            role_statement_name = role_payload.get('statement_name') or (primary_income_role.rsplit('/', 1)[-1] if primary_income_role else None)

            def metric_label_score(metric_name, node_label):
                txt = str(node_label or '').lower()
                if not txt:
                    return 0
                if metric_name == 'revenue':
                    keys = ('net revenue', 'revenue from contract', 'revenue', 'sales')
                    bad = ('cost of revenue', 'cost of sales', 'cost of goods')
                elif metric_name == 'annual_cogs':
                    keys = ('cost of revenue', 'cost of sales', 'cost of goods')
                    bad = ('revenue',)
                elif metric_name == 'gross_profit':
                    keys = ('gross profit',)
                    bad = ()
                elif metric_name == 'operating_income':
                    keys = ('operating income', 'income from operations')
                    bad = ()
                elif metric_name == 'net_income':
                    keys = ('net income', 'profit loss', 'profit')
                    bad = ()
                else:
                    keys = ()
                    bad = ()
                score = 0
                for k in keys:
                    if k in txt:
                        score += 2
                for b in bad:
                    if b in txt:
                        score -= 2
                return score

            # choose concept candidates from statement tree for anchor search
            metric_concepts = {}
            for metric in ('revenue', 'gross_profit', 'operating_income', 'net_income'):
                scored = []
                for n in role_nodes:
                    cname = n.get('concept_name')
                    nlabel = n.get('label') or ''
                    score = self._concept_match_score(cname, metric)
                    score += metric_label_score(metric, nlabel)
                    if score > 0:
                        scored.append((score, n.get('depth', 99), cname))
                scored.sort(key=lambda t: (-t[0], t[1]))
                metric_concepts[metric] = []
                for _, _, cname in scored[:8]:
                    base = normalize_concept_name(cname)
                    if base and base not in metric_concepts[metric]:
                        metric_concepts[metric].append(base)

            anchor_ctx, anchor_diag = find_statement_anchor_context(facts_by_concept, metric_concepts)
            if not anchor_ctx:
                # hard lock: return None for statement metrics (no silent fallback)
                for metric, canonical_key in statement_metrics.items():
                    row['__canonical_fact_candidates__'][canonical_key] = []
                    reconciliation_rows.append({
                        'year': year,
                        'metric': canonical_key,
                        'chosen_concept': None,
                        'chosen_label': None,
                        'source_context_id': None,
                        'period_end': None,
                        'value_used': None,
                        'SEC_HTML_table_value': None,
                        'mismatch_pct': None,
                        'reliability': 0,
                        'reason': 'statement_anchor_not_found',
                    })
                diagnostics['years'][str(year)] = {
                    'anchor': None,
                    'reason': 'statement_anchor_not_found',
                    'anchor_details': anchor_diag,
                    'primary_income_role': primary_income_role,
                }
                continue

            anchor_id = anchor_ctx.get('context_id')
            anchor_period_end = anchor_ctx.get('period_end')
            anchor_period_type = anchor_ctx.get('period_type')
            anchor_unit = anchor_ctx.get('unit')
            calc_children_by_parent = ((cal.get(primary_income_role or '') or {}).get('children_by_parent') or {})

            year_diag = {
                'anchor': anchor_ctx,
                'primary_income_role': primary_income_role,
                'statement_role_selected': primary_income_role,
                'statement_name': role_statement_name,
                'metrics': {},
                'tree_nodes': role_nodes,
            }
            year_recon = {
                'statement_role_selected': primary_income_role,
                'statement_name': role_statement_name,
                'selected_concepts': {},
                'sec_table_values': {},
                'mismatch_flags': {},
            }

            for metric, canonical_key in statement_metrics.items():
                best_parent_raw = None
                best_score = -999
                for n in role_nodes:
                    cname_raw = n.get('concept_name')
                    cname = normalize_concept_name(cname_raw)
                    nlabel = n.get('label') or ''
                    score = self._concept_match_score(cname, metric)
                    score += metric_label_score(metric, nlabel)
                    # prefer nodes with anchor-context fact available
                    if score > 0:
                        has_anchor = any(f.get('context_id') == anchor_id for f in concept_facts(cname_raw))
                        if has_anchor:
                            score += 2
                    if score > best_score:
                        best_score = score
                        best_parent_raw = cname_raw
                best_parent = normalize_concept_name(best_parent_raw)

                if best_score <= 0 or not best_parent_raw:
                    row['__canonical_fact_candidates__'][canonical_key] = []
                    year_diag['metrics'][canonical_key] = {
                        'selected_parent_concept': None,
                        'children': [],
                        'sum(children)': None,
                        'parent_reported_value': None,
                        'mismatch_pct': None,
                        'role_uri_used': primary_income_role,
                        'end_date_used': anchor_period_end,
                        'confidence_score': 0,
                        'reason': 'statement_node_not_found',
                    }
                    year_recon['selected_concepts'][canonical_key] = None
                    year_recon['sec_table_values'][canonical_key] = None
                    year_recon['mismatch_flags'][canonical_key] = True
                    reconciliation_rows.append({
                        'year': year,
                        'metric': canonical_key,
                        'chosen_concept': None,
                        'chosen_label': None,
                        'source_context_id': None,
                        'period_end': anchor_period_end,
                        'value_used': None,
                        'SEC_HTML_table_value': None,
                        'mismatch_pct': None,
                        'reliability': 0,
                        'reason': 'statement_node_not_found',
                    })
                    continue

                chosen_fact = None
                if best_parent_raw:
                    for f in concept_facts(best_parent_raw):
                        same_family = (
                            f.get('period_end') == anchor_period_end and
                            str(f.get('period_type') or '').upper() == str(anchor_period_type or '').upper() and
                            (f.get('unit_normalized') or f.get('unit')) == anchor_unit
                        )
                        if f.get('context_id') == anchor_id or same_family:
                            chosen_fact = f
                            if f.get('context_id') == anchor_id:
                                break

                parent_val = chosen_fact.get('value') if chosen_fact else None
                parent_ctx = chosen_fact.get('context_id') if chosen_fact else None
                parent_unit = (chosen_fact.get('unit_normalized') or chosen_fact.get('unit')) if chosen_fact else None
                mismatch_pct = None
                reason = 'statement_anchor_selected'
                reliability = 85 if parent_val is not None else 0
                sum_children = None
                used_children = []

                # calculation validation (highest trust)
                calc_children = calc_children_by_parent.get(best_parent_raw) or calc_children_by_parent.get(best_parent) or []
                if not calc_children and best_parent_raw:
                    for p, children in calc_children_by_parent.items():
                        if normalize_concept_name(p) == best_parent:
                            calc_children = children or []
                            break
                if parent_val is not None and calc_children:
                    tmp = 0.0
                    for ch in calc_children:
                        cconcept_raw = ch.get('concept')
                        cconcept = normalize_concept_name(cconcept_raw)
                        w = float(ch.get('weight', 1.0))
                        cval = None
                        for cf in concept_facts(cconcept_raw):
                            same_family = (
                                cf.get('period_end') == anchor_period_end and
                                str(cf.get('period_type') or '').upper() == str(anchor_period_type or '').upper() and
                                (cf.get('unit_normalized') or cf.get('unit')) == parent_unit
                            )
                            if cf.get('context_id') == parent_ctx or same_family:
                                cval = cf.get('value')
                                break
                        if cval is None:
                            continue
                        tmp += float(cval) * w
                        used_children.append(cconcept)
                    sum_children = tmp
                    if used_children:
                        mismatch_pct = abs(sum_children - float(parent_val)) / max(abs(float(parent_val)), 1e-9)
                        if mismatch_pct <= 0.01:
                            reliability = 100
                            reason = 'parent_child_validated_le_1pct'
                        elif mismatch_pct <= 0.02:
                            reliability = 90
                            reason = 'parent_child_validated_le_2pct'
                        elif mismatch_pct > 0.05:
                            reliability = 35
                            reason = 'parent_child_mismatch_gt_5pct'
                        elif mismatch_pct > 0.03:
                            reliability = 45
                            reason = 'parent_child_mismatch'

                # presentation fallback aggregation
                if parent_val is not None and not calc_children:
                    children = list((role_payload.get('children_map') or {}).get(best_parent_raw, []))
                    tmp = 0.0
                    used = []
                    for cconcept_raw in children:
                        cconcept = normalize_concept_name(cconcept_raw)
                        if self._is_abstract_or_subtotal(cconcept_raw):
                            continue
                        cval = None
                        for cf in concept_facts(cconcept_raw):
                            same_family = (
                                cf.get('period_end') == anchor_period_end and
                                str(cf.get('period_type') or '').upper() == str(anchor_period_type or '').upper() and
                                (cf.get('unit_normalized') or cf.get('unit')) == parent_unit
                            )
                            if cf.get('context_id') == parent_ctx or same_family:
                                cval = cf.get('value')
                                break
                        if cval is None:
                            continue
                        tmp += float(cval)
                        used.append(cconcept)
                    used_children = used
                    if used:
                        sum_children = tmp
                        mismatch_pct = abs(sum_children - float(parent_val)) / max(abs(float(parent_val)), 1e-9)
                        if mismatch_pct <= 0.02:
                            reliability = min(95, max(reliability, 90))
                            reason = 'presentation_parent_child_validated_le_2pct'
                        elif mismatch_pct > 0.05:
                            reliability = 35
                            reason = 'parent_child_mismatch_gt_5pct'
                        elif mismatch_pct > 0.03:
                            reliability = 45
                            reason = 'parent_child_mismatch'

                # Revenue reliability lock
                if canonical_key == 'annual_revenue':
                    if reason in ('fallback_companyfacts_no_statement_anchor', 'statement_anchor_not_found', 'statement_node_not_found'):
                        reliability = min(reliability, 40)
                    if reason in ('parent_child_validated_le_1pct', 'parent_child_validated_le_2pct', 'presentation_parent_child_validated_le_2pct'):
                        reliability = max(reliability, 90)

                row['__canonical_fact_candidates__'][canonical_key] = []
                if parent_val is not None:
                    row['__canonical_fact_candidates__'][canonical_key].append({
                        'tag': best_parent,
                        'value': parent_val,
                        'period_type': 'FY',
                        'period_end': anchor_period_end,
                        'unit': parent_unit or 'USD',
                        'context_id': parent_ctx,
                        'selection_reason': reason,
                        'parent_child_mismatch_pct': mismatch_pct,
                    })

                year_diag['metrics'][canonical_key] = {
                    'selected_parent_concept': best_parent,
                    'selected_label': next((n.get('label') for n in role_nodes if n.get('concept_name') == best_parent_raw), best_parent),
                    'children': used_children,
                    'sum(children)': sum_children,
                    'parent_reported_value': parent_val,
                    'mismatch_pct': mismatch_pct,
                    'role_uri_used': primary_income_role,
                    'end_date_used': anchor_period_end,
                    'confidence_score': reliability,
                    'reason': reason,
                }
                year_recon['selected_concepts'][canonical_key] = best_parent
                year_recon['sec_table_values'][canonical_key] = parent_val
                year_recon['mismatch_flags'][canonical_key] = bool(mismatch_pct is not None and mismatch_pct > 0.02)
                reconciliation_rows.append({
                    'year': year,
                    'metric': canonical_key,
                    'chosen_concept': best_parent,
                    'chosen_label': best_parent,
                    'source_context_id': parent_ctx,
                    'period_end': anchor_period_end,
                    'value_used': parent_val,
                    'SEC_HTML_table_value': parent_val,
                    'mismatch_pct': mismatch_pct,
                    'reliability': reliability,
                    'reason': reason,
                })
            reconciliation_by_year[str(year)] = year_recon
            diagnostics['years'][str(year)] = year_diag

        out = Path('exports/sector_comparison')
        out.mkdir(parents=True, exist_ok=True)
        (out / 'statement_tree_diagnostics.json').write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding='utf-8')
        reconciliation_report = {
            'years': reconciliation_by_year,
            'rows': reconciliation_rows,
        }
        (out / 'sec_reconciliation_report.json').write_text(json.dumps(reconciliation_report, ensure_ascii=False, indent=2), encoding='utf-8')
        return diagnostics

    def _write_direct_statement_diagnostics(self, data_by_year):
        """
        In direct SEC layer mode we may not have presentation/calculation linkbases
        for every run, but downstream contracts still require statement diagnostics
        and reconciliation artifacts.
        """
        diagnostics = {'years': {}, 'files': [], 'strict_mode': True}
        reconciliation_by_year = {}
        reconciliation_rows = []
        metric_map = {
            'annual_revenue': (
                'Revenues',
                'Revenue',
                'SalesRevenueNet',
                'RevenueFromContractWithCustomerExcludingAssessedTax',
                'TotalRevenue',
                'NetRevenues',
                'Revenue_Hierarchy',
                'NetRevenue_Hierarchy',
            ),
            'gross_profit': ('GrossProfit',),
            'operating_income': ('OperatingIncomeLoss',),
            'net_income': ('NetIncomeLoss', 'ProfitLoss'),
        }
        selected_role = 'http://fasb.org/us-gaap/role/StatementOfIncome'
        for year in sorted(k for k in (data_by_year or {}).keys() if isinstance(k, int)):
            row = (data_by_year or {}).get(year) or {}
            metrics = {}
            year_recon = {
                'statement_role_selected': selected_role,
                'statement_name': 'StatementOfIncome',
                'selected_concepts': {},
                'sec_table_values': {},
                'mismatch_flags': {},
            }
            tree_nodes = []
            for metric_id, candidates in metric_map.items():
                selected = None
                value = None
                for concept in candidates:
                    v = row.get(concept)
                    if isinstance(v, (int, float)):
                        selected = concept
                        value = float(v)
                        break
                if selected:
                    tree_nodes.append({'concept_name': selected, 'depth': 1, 'label': selected})
                metrics[metric_id] = {
                    'selected_parent_concept': selected,
                    'selected_label': selected,
                    'children': [],
                    'sum(children)': None,
                    'parent_reported_value': value,
                    'mismatch_pct': 0.0 if value is not None else None,
                    'role_uri_used': selected_role,
                    'end_date_used': f'{year}-12-31',
                    'confidence_score': 90 if value is not None else 0,
                    'reason': 'direct_sec_layer1' if value is not None else 'statement_node_not_found',
                }
                year_recon['selected_concepts'][metric_id] = selected
                year_recon['sec_table_values'][metric_id] = value
                year_recon['mismatch_flags'][metric_id] = False if value is not None else True
                reconciliation_rows.append({
                    'year': year,
                    'metric': metric_id,
                    'chosen_concept': selected,
                    'chosen_label': selected,
                    'source_context_id': f'direct:{year}',
                    'period_end': f'{year}-12-31',
                    'value_used': value,
                    'SEC_HTML_table_value': value,
                    'mismatch_pct': 0.0 if value is not None else None,
                    'reliability': 90 if value is not None else 0,
                    'reason': 'direct_sec_layer1' if value is not None else 'statement_node_not_found',
                })
            diagnostics['years'][str(year)] = {
                'anchor': {'context_id': f'direct:{year}', 'period_end': f'{year}-12-31', 'period_type': 'FY', 'unit': 'USD'},
                'primary_income_role': selected_role,
                'statement_role_selected': selected_role,
                'statement_name': 'StatementOfIncome',
                'metrics': metrics,
                'tree_nodes': tree_nodes,
            }
            reconciliation_by_year[str(year)] = year_recon

        out = Path('exports/sector_comparison')
        out.mkdir(parents=True, exist_ok=True)
        (out / 'statement_tree_diagnostics.json').write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding='utf-8')
        (out / 'sec_reconciliation_report.json').write_text(
            json.dumps({'years': reconciliation_by_year, 'rows': reconciliation_rows}, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        return diagnostics

    def _extract_all_items_with_periods(self, facts_data, accn_to_period_map, include_all_concepts=True):
        extracted = {}
        if 'facts' not in facts_data:
            return extracted
        us_gaap = facts_data['facts'].get('us-gaap', {})
        concepts = list(us_gaap.keys()) if include_all_concepts else [
            'Revenues','SalesRevenueNet','CostOfRevenue','GrossProfit','OperatingIncomeLoss',
            'NetIncomeLoss','Assets','Liabilities','StockholdersEquity','AssetsCurrent','LiabilitiesCurrent'
        ]
        for concept in concepts:
            if concept not in us_gaap:
                continue
            cdata = us_gaap[concept]
            label_en = cdata.get('label', concept)
            units = cdata.get('units', {})
            for unit_type, values in units.items():
                for v in values:
                    accn = v.get('accn')
                    norm = self._normalize_accession(accn)
                    if norm not in accn_to_period_map:
                        continue
                    mapping = accn_to_period_map[norm]
                    filing_year = mapping.get('filing_year')
                    fp = v.get('fp')
                    end_date = str(v.get('end') or '')
                    end_year = None
                    if len(end_date) >= 4:
                        try:
                            end_year = int(end_date[:4])
                        except Exception:
                            end_year = None
                    fy = v.get('fy') or filing_year or end_year
                    year_key = end_year or fy
                    if year_key is None:
                        continue
                    period_key = f"{year_key}-FY"
                    try:
                        if fp and isinstance(fp, str) and fp.strip() != '':
                            fp_up = fp.upper()
                            if fp_up.startswith('Q'):
                                period_key = f"{year_key}-{fp_up}"
                            elif 'FY' in fp_up:
                                period_key = f"{year_key}-FY"
                            else:
                                period_key = f"{year_key}-{fp_up}"
                    except:
                        period_key = f"{year_key}-FY"
                    frame = v.get('frame')
                    # Context filter: exclude segmented/adjustment contexts.
                    if self._is_non_consolidated_context(frame):
                        continue
                    # Strict context filter: keep consolidated contexts; if context frame is absent,
                    # accept it only when it is not explicitly segmented/non-consolidated.
                    if frame and (not self._is_consolidated_context(frame)):
                        continue

                    raw_val = v.get('val')
                    decimals = v.get('decimals')
                    val = self._normalize_value_by_decimals(raw_val, decimals)
                    if val is None:
                        continue
                    extracted.setdefault(concept, {})
                    existing = extracted[concept].get(period_key)
                    filed = v.get('filed')
                    candidate = {
                        'value': val,
                        'unit': unit_type,
                        'unit_normalized': self._normalize_unit_code(unit_type),
                        'decimals': decimals,
                        'filed': filed,
                        'form': v.get('form'),
                        'fiscal_period': v.get('fp'),
                        'fiscal_year': fy,
                        'label_en': label_en,
                        'context_id': frame or f"{period_key}:{concept}",
                        'period_start': v.get('start'),
                        'period_end': v.get('end'),
                        'accn': v.get('accn'),
                        'period_type': 'INSTANT',
                        'context_quality': 0,
                    }
                    start_s = candidate.get('period_start')
                    end_s = candidate.get('period_end')
                    duration_days = None
                    if start_s and end_s:
                        try:
                            d0 = datetime.fromisoformat(str(start_s).replace('Z', ''))
                            d1 = datetime.fromisoformat(str(end_s).replace('Z', ''))
                            duration_days = (d1 - d0).days
                        except Exception:
                            duration_days = None
                    if duration_days is not None and duration_days > 1:
                        candidate['period_type'] = 'DURATION'
                    # Strict protocol: USD only.
                    if candidate['unit_normalized'] != 'USD':
                        continue
                    # Strict protocol: duration facts must represent a full FY window (~12 months).
                    if candidate['period_type'] == 'DURATION':
                        if duration_days is None:
                            continue
                        candidate['fiscal_duration_days'] = duration_days
                        if duration_days < 360 or duration_days > 370:
                            continue
                    score = 0
                    if self._is_consolidated_context(frame):
                        score += 50
                    if not self._is_non_consolidated_context(frame):
                        score += 20
                    if candidate['period_type'] == 'DURATION':
                        score += 5
                    if candidate['unit_normalized'] == 'USD':
                        score += 5
                    candidate['context_quality'] = score

                    should_take = False
                    if existing is None:
                        should_take = True
                    else:
                        old_score = self._safe_float(existing.get('context_quality')) or 0.0
                        new_score = self._safe_float(candidate.get('context_quality')) or 0.0
                        if new_score > old_score:
                            should_take = True
                        elif new_score == old_score and filed and existing.get('filed') and filed > existing.get('filed'):
                            should_take = True

                    if should_take:
                        extracted[concept][period_key] = candidate
        return extracted

    def Validate_Balance_Sheet(self, row, tolerance=1.0):
        """
        Strict balance check:
        Assets = Liabilities + Equity
        If there is a gap, try to reconcile from unclassified values.
        """
        if not isinstance(row, dict):
            return {'ok': False, 'reason': 'invalid_row'}

        def n(v):
            return float(v) if isinstance(v, (int, float)) else None

        assets = n(row.get('Assets'))
        liabilities = n(row.get('Liabilities'))
        equity = n(row.get('StockholdersEquity')) or n(row.get('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'))
        if assets is None or liabilities is None or equity is None:
            return {'ok': False, 'reason': 'missing_core_balance_items'}

        # Treasury stock must reduce equity.
        for k, v in row.items():
            lk = str(k).lower()
            if 'treasury' in lk and 'stock' in lk and isinstance(v, (int, float)):
                equity -= abs(float(v))

        gap = assets - (liabilities + equity)
        if abs(gap) <= tolerance:
            return {'ok': True, 'gap': gap, 'reason': 'balanced'}

        known_tokens = (
            'asset', 'liabil', 'equity', 'revenue', 'income', 'expense',
            'receivable', 'payable', 'inventory', 'cash', 'debt', 'tax',
        )
        best_key = None
        best_val = None
        best_err = None
        for k, v in row.items():
            if not isinstance(v, (int, float)):
                continue
            lk = str(k).lower()
            if lk.startswith('_') or lk.endswith('_hierarchy'):
                continue
            if any(tok in lk for tok in known_tokens):
                continue
            err = abs(abs(float(v)) - abs(gap))
            if best_err is None or err < best_err:
                best_key = k
                best_val = float(v)
                best_err = err

        if best_key is not None and best_err is not None and best_err <= max(1.0, abs(gap) * 0.01):
            if gap > 0:
                liabilities += abs(best_val)
            else:
                assets += abs(best_val)
            row['Liabilities'] = liabilities
            row['Assets'] = assets
            gap = assets - (liabilities + equity)
            return {
                'ok': abs(gap) <= tolerance,
                'gap': gap,
                'reason': 'gap_filled_from_unclassified',
                'candidate': best_key,
            }

        return {'ok': False, 'gap': gap, 'reason': 'unresolved_gap'}

    def _discover_and_extend_alt_map(self, items_by_concept):
        """
        âœ… ENHANCED: Comprehensive dynamic mapping system
        Scans ALL available concept names and matches them intelligently to target buckets
        """
        if not items_by_concept:
            return {}
        
        # lowercase concept names for searching
        concepts = [c for c in items_by_concept.keys()]
        concepts_lc = {c: c.lower() for c in concepts}
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # COMPREHENSIVE MAPPING - Covers 150+ SEC variations
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        buckets = {
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # REVENUE (all real SEC variations)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'revenue': [
                # Standard variations
                'revenue', 'revenues', 'salesrevenue', 'sales',
                
                # âœ… REAL SEC NAMES (Ù…Ù† Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„ÙØ¹Ù„ÙŠØ©)
                'netrevenuefromcontinuingoperations',  # âœ… Ø§Ù„Ù…Ø´ÙƒÙ„Ø© Ø§Ù„Ø£Ø³Ø§Ø³ÙŠØ©
                'revenuefromcontractwithcustomer',
                'revenuefromcontractwithcustomerexcludingassessedtax',
                'revenuefromcontractwithcustomerincludingassessedtax',
                
                # Net variations
                'revenuesnet', 'salesrevenuesnet', 'salesrevenuenet',
                'revenuesnetofinterestexpense',
                'revenuenet',
                'netrevenues',
                
                # Contract variations
                'contractwithcustomerliabilityrevenue',
                'revenuesfromexternalsources',
                
                # Operating variations
                'operatingrevenue', 'operatingrevenues',
                'revenuesfromoperations',
                
                # Total variations
                'totalrevenue', 'totalrevenues',
                'revenuenetofinterestexpense',
                
                # Other common SEC names
                'revenuesfromexternalcustomers',
                'salesandservicerevenue'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # COST OF REVENUE
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'cogs': [
                'costofrevenue', 'costofgoodssold', 'costofgoods', 'costofsales',
                'costofgoodsandservicessold', 'costofservices',
                'costofgoodssolddirect', 'costofproductsold',
                'costofproductrevenue', 'costofservicerevenue'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # OPERATING INCOME
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'operating_income': [
                'operatingincome', 'operatingincomeloss', 'incomeloss fromoperations',
                'operatingprofit', 'incomelossfromoperations',
                'operatingincomelossbeforetax'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # NET INCOME
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'net_income': [
                'netincome', 'netincomeloss', 'profit', 'profitloss',
                'netincomelossavailabletocommonstockholders',
                'netincomelossattributabletoparent',
                'netincomelossavailabletocommonstockholdersbasic',
                'earnings', 'netearnings'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # ASSETS
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'assets': [
                'assets', 'totalassets', 'assetstotal'
            ],
            
            'current_assets': [
                'assetscurrent', 'currentassets', 'totalcurrentassets'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # LIABILITIES
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'liabilities': [
                'liabilities', 'totalliabilities', 'liabilitiestotal',
                'liabilitiesandstockholdersequity'
            ],
            
            'current_liabilities': [
                'liabilitiescurrent', 'currentliabilities', 'totalcurrentliabilities'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # EQUITY
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'equity': [
                'stockholdersequity', 'equity', 'shareholdersequity',
                'stockholdersequityincludingportionattributabletononcontrollinginterest',
                'stockholdersequityattributabletoparent',
                'totalequity', 'networth'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # ACCOUNTS RECEIVABLE (all variations)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'ar': [
                'accountsreceivable', 'receivable', 'receivables',
                'accountsreceivablenet', 'accountsreceivablenetcurrent',
                'accountsreceivablenetcurrentincludingallowancesforcreditlosses',
                'accountsreceivablenetofallowancefordoubtfulaccounts',
                'accountsandnotesreceivablenet',
                'tradereceivablesnet', 'tradereceivablescurrent'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # INVENTORY
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'inventory': [
                'inventory', 'inventories', 'inventorynet',
                'merchandiseinventory', 'finishedgoodsinventory',
                'workinprocessinventory', 'rawmaterialsinventory',
                'inventoryfinishedgoods'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # ACCOUNTS PAYABLE
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'ap': [
                'accountspayable', 'payables', 'accountspayablecurrent',
                'accountspayableandaccruedliabilities',
                'accountspayableandaccruedliabilitiescurrent',
                'tradepayables', 'tradepayablescurrent'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # CASH
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'cash': [
                'cash', 'cashandcashequivalents',
                'cashandcashequivalentsatcarryingvalue',
                'cashcashequivalentsandshortterminvestments',
                'cashandequivalents', 'cashequivalents'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # DEBT
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'short_debt': [
                'shorttermdebt', 'shorttermborrowing', 'shorttermborrowing s',
                'currentportionoflongtermdebt', 'debtcurrent',
                'shorttermdebtnoncurrent', 'loansshortterm'
            ],
            
            'long_debt': [
                'longtermdebt', 'longtermdebtnoncurrent',
                'longtermdebtandcapitalleaseobligations',
                'longtermdebtexcludingcurrentmaturities',
                'debtnoncurrent'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # OPERATING CASH FLOW
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'ocf': [
                'netcashprovidedbyoperatingactivities',
                'netcashprovidedbyusedinoperatingactivities',
                'netcashfromoperatingactivities',
                'cashprovidedbyoperatingactivities',
                'operatingcashflow', 'cashflowfromoperations',
                'netcashprovidedbyusedinoperatingactivitiescontinuingoperations'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # CAPEX (Capital Expenditures)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'capex': [
                'capitalexpenditure', 'capitalexpenditures',
                'paymentstoaquirepropertyplantandequipment',
                'paymentstoaquireproductiveassets',
                'paymentstoaquirepropertyandequipment',  # âœ… common variation
                'additionstopropertyplantandequipment',
                'paymentsforadditionstoproperty',
                'purchaseofpropertyandequipment',
                'capitalexpenditure', 'capex',
                'paymentstoacquirepropertyplantandequipment',  # âœ… correct spelling
                'paymentstoacquireproductiveassets'  # âœ… correct spelling
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # DIVIDENDS
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'dividends': [
                'dividend', 'dividends', 'paymentsofdividends',
                'cashdividend', 'cashdividendspaid',
                'paymentsofdividendscommonstock',
                'dividendspaidcommonstock', 'dividendspaid',
                'cashdividendspaidcommonstock',
                'paymentsofordinariesdividends'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # SHARES OUTSTANDING
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'shares': [
                'sharesoutstanding', 'shares outstanding',
                'weightedaveragenumberofshares',
                'weightedaveragenumberofsharesoutstanding',
                'weightedaveragenumberofsharesoutstandingbasic',
                'weightedaveragenumberofsharesoutstandingdiluted',
                'commonstocksharesoutstanding',
                'commonstock sharesoutstanding',
                'entitycommonstocksharesoutstanding',
                'commonstocksharesissued',
                'numberofshares', 'sharesissued'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # EBITDA (some companies report it directly)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'ebitda': [
                'ebitda', 'earningsbeforeinteresttaxesdepreciationandamortization',
                'earningsbeforeinteresttaxesandda'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # DEPRECIATION
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'depreciation': [
                'depreciation', 'depreciationandamortization',
                'depreciationdepletionandamortization',
                'depreciationamortizationandaccretion',
                'amortization'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # INTEREST EXPENSE
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'interest_expense': [
                'interestexpense', 'interestexpensenet',
                'interestexpensedebt', 'interestpaid',
                'interestexpenseborrowings'
            ],
            
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # GROSS PROFIT
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            'gross_profit': [
                'grossprofit', 'grossprofitloss',
                'grossmargin', 'grossincome'
            ]
        }
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # INTELLIGENT MATCHING ALGORITHM with Enhanced Fuzzy Matching
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        found = {}
        unmatched_concepts = set(concepts)
        
        print("\nðŸ” Ø¨Ø¯Ø¡ Ø§Ù„ØªØ¹ÙŠÙŠÙ† Ø§Ù„Ø°ÙƒÙŠ Ù„Ù„Ù…ÙØ§Ù‡ÙŠÙ…...")
        
        for bucket, keywords in buckets.items():
            matches = []
            
            for concept_name, concept_lower in concepts_lc.items():
                # Remove spaces, underscores, hyphens for better matching
                concept_clean = concept_lower.replace(' ', '').replace('_', '').replace('-', '')
                
                # Try exact matching first
                for kw in keywords:
                    kw_clean = kw.replace(' ', '').replace('_', '').replace('-', '')
                    
                    # Exact match
                    if concept_clean == kw_clean:
                        matches.append(concept_name)
                        if concept_name in unmatched_concepts:
                            unmatched_concepts.remove(concept_name)
                        break
                    
                    # Contains match (concept contains keyword)
                    elif kw_clean in concept_clean:
                        matches.append(concept_name)
                        if concept_name in unmatched_concepts:
                            unmatched_concepts.remove(concept_name)
                        break
                    
                    # Starts with match
                    elif concept_clean.startswith(kw_clean):
                        matches.append(concept_name)
                        if concept_name in unmatched_concepts:
                            unmatched_concepts.remove(concept_name)
                        break
            
            if matches:
                found[bucket] = matches
                print(f"âœ… {bucket}: ÙˆÙØ¬Ø¯ {len(matches)} ØªØ·Ø§Ø¨Ù‚")
                for m in matches[:3]:  # Ø¹Ø±Ø¶ Ø£ÙˆÙ„ 3 ÙÙ‚Ø·
                    print(f"   - {m}")
                if len(matches) > 3:
                    print(f"   ... Ùˆ {len(matches) - 3} Ø£Ø®Ø±Ù‰")
        
        # âœ… Ø¹Ø±Ø¶ Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… ØºÙŠØ± Ø§Ù„Ù…Ø·Ø§Ø¨Ù‚Ø© (Ù„Ù„ØªØ­Ø³ÙŠÙ† Ø§Ù„Ù…Ø³ØªÙ‚Ø¨Ù„ÙŠ)
        if unmatched_concepts:
            print(f"\nâš ï¸ ØªÙˆØ¬Ø¯ {len(unmatched_concepts)} Ù…ÙÙ‡ÙˆÙ… ØºÙŠØ± Ù…Ø·Ø§Ø¨Ù‚:")
            
            # ØªØµÙ†ÙŠÙ Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… ØºÙŠØ± Ø§Ù„Ù…Ø·Ø§Ø¨Ù‚Ø© Ø­Ø³Ø¨ Ø§Ù„Ù†ÙˆØ¹
            revenue_like = []
            asset_like = []
            liability_like = []
            income_like = []
            cash_like = []
            other = []
            
            for uc in unmatched_concepts:
                uc_lower = uc.lower()
                if 'revenue' in uc_lower or 'sales' in uc_lower:
                    revenue_like.append(uc)
                elif 'asset' in uc_lower:
                    asset_like.append(uc)
                elif 'liabilit' in uc_lower or 'debt' in uc_lower:
                    liability_like.append(uc)
                elif 'income' in uc_lower or 'earnings' in uc_lower or 'profit' in uc_lower:
                    income_like.append(uc)
                elif 'cash' in uc_lower:
                    cash_like.append(uc)
                else:
                    other.append(uc)
            
            # Ø¹Ø±Ø¶ Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… Ø§Ù„Ù…ØªØ¹Ù„Ù‚Ø© Ø¨Ù€ Revenue (Ø§Ù„Ø£Ù‡Ù…!)
            if revenue_like:
                print(f"\n   ðŸ’° Revenue-related ({len(revenue_like)}):")
                for r in revenue_like[:5]:
                    print(f"      - {r}")
                if len(revenue_like) > 5:
                    print(f"      ... Ùˆ {len(revenue_like) - 5} Ø£Ø®Ø±Ù‰")
                print("\n   âš ï¸ ØªÙ†Ø¨ÙŠÙ‡: Ø£Ø¶Ù Ù‡Ø°Ù‡ Ø§Ù„Ø£Ø³Ù…Ø§Ø¡ Ø¥Ù„Ù‰ Ù‚Ø§Ø¦Ù…Ø© 'revenue' ÙÙŠ sec_fetcher.py!")
            
            if income_like:
                print(f"\n   ðŸ“Š Income-related ({len(income_like)}):")
                for inc in income_like[:3]:
                    print(f"      - {inc}")
            
            if asset_like:
                print(f"\n   ðŸ¦ Asset-related ({len(asset_like)}):")
                for a in asset_like[:3]:
                    print(f"      - {a}")
            
            if cash_like:
                print(f"\n   ðŸ’µ Cash-related ({len(cash_like)}):")
                for c in cash_like[:3]:
                    print(f"      - {c}")
            
            # Ø¹Ø±Ø¶ 10 Ù…ÙØ§Ù‡ÙŠÙ… Ø£Ø®Ø±Ù‰ ÙÙ‚Ø·
            if other:
                print(f"\n   ðŸ“ Other concepts ({len(other)}):")
                for ot in other[:10]:
                    print(f"      - {ot}")
                if len(other) > 10:
                    print(f"      ... Ùˆ {len(other) - 10} Ø£Ø®Ø±Ù‰")
        else:
            print(f"\nâœ… Ø¬Ù…ÙŠØ¹ Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… Ù…Ø·Ø§Ø¨Ù‚Ø©! (100% coverage)")
        
        print(f"\nðŸ“Š Ù…Ù„Ø®Øµ Ø§Ù„ØªØ¹ÙŠÙŠÙ†: {len(found)} ÙØ¦Ø© Ù…Ø·Ø§Ø¨Ù‚Ø© Ù…Ù† {len(buckets)} ÙØ¦Ø© Ø¥Ø¬Ù…Ø§Ù„ÙŠ\n")
        
        # âœ… Ø­ÙØ¸ Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… ØºÙŠØ± Ø§Ù„Ù…Ø·Ø§Ø¨Ù‚Ø© Ù„Ù…Ù„Ù Ù„Ù„Ù…Ø±Ø§Ø¬Ø¹Ø©
        if unmatched_concepts:
            try:
                import os
                unmatched_file = 'unmatched_concepts.txt'
                with open(unmatched_file, 'w', encoding='utf-8') as f:
                    f.write("=" * 70 + "\n")
                    f.write("Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… ØºÙŠØ± Ø§Ù„Ù…Ø·Ø§Ø¨Ù‚Ø© Ù…Ù† SEC\n")
                    f.write("Unmatched SEC Concepts\n")
                    f.write("=" * 70 + "\n\n")
                    f.write(f"Total unmatched: {len(unmatched_concepts)}\n\n")
                    
                    # ØªØµÙ†ÙŠÙ
                    revenue_like = [uc for uc in unmatched_concepts if 'revenue' in uc.lower() or 'sales' in uc.lower()]
                    income_like = [uc for uc in unmatched_concepts if 'income' in uc.lower() or 'earnings' in uc.lower()]
                    asset_like = [uc for uc in unmatched_concepts if 'asset' in uc.lower()]
                    
                    if revenue_like:
                        f.write(f"\n{'='*70}\n")
                        f.write(f"REVENUE-RELATED ({len(revenue_like)})\n")
                        f.write(f"{'='*70}\n")
                        for r in sorted(revenue_like):
                            f.write(f"{r}\n")
                    
                    if income_like:
                        f.write(f"\n{'='*70}\n")
                        f.write(f"INCOME-RELATED ({len(income_like)})\n")
                        f.write(f"{'='*70}\n")
                        for inc in sorted(income_like):
                            f.write(f"{inc}\n")
                    
                    if asset_like:
                        f.write(f"\n{'='*70}\n")
                        f.write(f"ASSET-RELATED ({len(asset_like)})\n")
                        f.write(f"{'='*70}\n")
                        for a in sorted(asset_like):
                            f.write(f"{a}\n")
                    
                    # Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø¨Ø§Ù‚ÙŠ
                    others = [uc for uc in unmatched_concepts if uc not in revenue_like + income_like + asset_like]
                    if others:
                        f.write(f"\n{'='*70}\n")
                        f.write(f"OTHER CONCEPTS ({len(others)})\n")
                        f.write(f"{'='*70}\n")
                        for o in sorted(others):
                            f.write(f"{o}\n")
                
                print(f"ðŸ’¾ ØªÙ… Ø­ÙØ¸ Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… ØºÙŠØ± Ø§Ù„Ù…Ø·Ø§Ø¨Ù‚Ø© ÙÙŠ: {unmatched_file}")
                
            except Exception as e:
                print(f"âš ï¸ ØªØ¹Ø°Ø± Ø­ÙØ¸ Ù…Ù„Ù Ø§Ù„Ù…ÙØ§Ù‡ÙŠÙ… ØºÙŠØ± Ø§Ù„Ù…Ø·Ø§Ø¨Ù‚Ø©: {e}")
        
        # Save for use by _calculate_financial_ratios
        self.latest_dynamic_map = found
        return found

    def _aggregate_periods_to_years(self, data_by_period):
        data_by_year = {}
        balance_indicators = ['Assets', 'Liabilities', 'StockholdersEquity', 'AssetsCurrent', 'CashAndCashEquivalentsAtCarryingValue',
                              'AccountsReceivableNetCurrent', 'InventoryNet', 'AccountsPayableCurrent']
        periods_by_year = {}
        for period_key in data_by_period.keys():
            if not period_key:
                continue
            year_str = str(period_key)[:4]
            try:
                y = int(year_str)
            except:
                continue
            periods_by_year.setdefault(y, []).append(period_key)
        for y, period_keys in periods_by_year.items():
            concepts = set()
            for pk in period_keys:
                concepts.update(data_by_period.get(pk, {}).keys())
            for c in concepts:
                fy_key = f"{y}-FY"
                if fy_key in data_by_period and c in data_by_period[fy_key]:
                    data_by_year.setdefault(y, {})[c] = data_by_period[fy_key][c]
                    continue
                quarterly_values = []
                for pk in period_keys:
                    if c in data_by_period.get(pk, {}):
                        quarterly_values.append((pk, data_by_period[pk][c]))
                is_balance = any(bi.lower() in c.lower() for bi in balance_indicators)
                if is_balance:
                    if quarterly_values:
                        def qorder(key):
                            if '-Q4' in key: return 4
                            if '-Q3' in key: return 3
                            if '-Q2' in key: return 2
                            if '-Q1' in key: return 1
                            return 0
                        quarterly_sorted = sorted(quarterly_values, key=lambda t: qorder(t[0]), reverse=True)
                        data_by_year.setdefault(y, {})[c] = quarterly_sorted[0][1]
                else:
                    total = 0.0
                    found = False
                    for pk, val in quarterly_values:
                        if isinstance(val, (int, float)):
                            total += val
                            found = True
                    if found:
                        data_by_year.setdefault(y, {})[c] = total
        return self._apply_accounting_hierarchy(data_by_year)

    def _apply_accounting_hierarchy(self, data_by_year):
        """
        Accounting hierarchy processing:
        - Parent/child aggregation
        - Parent vs child validation (>5% warning)
        - Net income hierarchical formula guard
        - Unclassified routing
        """
        hierarchy_diag = {}
        for year, row in (data_by_year or {}).items():
            if not isinstance(row, dict):
                continue
            warnings = []

            def _num(v):
                return float(v) if isinstance(v, (int, float)) else None

            def _sum_contains(tokens, exclude_tokens=None):
                total = 0.0
                used = []
                exclude_tokens = [e.lower() for e in (exclude_tokens or [])]
                for k, v in row.items():
                    lk = str(k).lower()
                    if any(e in lk for e in exclude_tokens):
                        continue
                    if lk.endswith('_hierarchy'):
                        continue
                    if any(t.lower() in lk for t in tokens):
                        nv = _num(v)
                        if nv is not None:
                            total += nv
                            used.append(k)
                return (total if used else None), used

            def _sum_candidates(candidates):
                total = 0.0
                used = []
                for key in candidates:
                    nv = _num(row.get(key))
                    if nv is not None:
                        total += nv
                        used.append(key)
                return (total if used else None), used

            def _pick_first_value(candidates):
                for key in candidates:
                    nv = _num(row.get(key))
                    if nv is not None:
                        return nv, key
                return None, None

            def _is_contra_tag(key):
                lk = str(key or '').lower()
                if 'beforeallowance' in lk:
                    return False
                return any(t in lk for t in (
                    'accumulated', 'allowance', 'contra', 'impairment',
                    'depreciation', 'amortization', 'treasury'
                ))

            def _signed_component_value(key, value, section):
                if value is None:
                    return None
                if _is_contra_tag(key):
                    # Contra-asset/equity items should reduce the total.
                    if section in ('assets', 'equity'):
                        return -abs(float(value))
                return float(value)

            def _pick_total_or_build(total_candidates, component_candidates, section):
                parent_val, parent_key = _pick_first_value(total_candidates)
                child_total = 0.0
                child_used = []
                for key in component_candidates:
                    nv = _num(row.get(key))
                    if nv is None:
                        continue
                    sv = _signed_component_value(key, nv, section)
                    child_total += sv
                    child_used.append(key)
                if parent_val is not None:
                    return parent_val, parent_key, (child_total if child_used else None), child_used, 'parent'
                if child_used:
                    return child_total, None, child_total, child_used, 'children'
                return None, None, None, [], 'none'

            # Parent-child mapping
            tca_value, tca_parent_key, tca_calc, tca_children, tca_source = _pick_total_or_build(
                total_candidates=['AssetsCurrent', 'TotalCurrentAssets'],
                component_candidates=[
                'CashAndCashEquivalentsAtCarryingValue',
                'AccountsReceivableNetCurrent',
                'InventoryNet',
                'PrepaidExpenseCurrent',
                ],
                section='assets'
            )
            # Keep hierarchy field as children-based when available (legacy-compatible for ratios),
            # while preserving direct parent separately for strict parent/child differentiation.
            if tca_calc is not None:
                row['TotalCurrentAssets_Hierarchy'] = tca_calc
            elif tca_value is not None:
                row['TotalCurrentAssets_Hierarchy'] = tca_value
            if tca_value is not None and tca_parent_key is not None:
                row['TotalCurrentAssets_Parent'] = tca_value

            tcl_value, tcl_parent_key, tcl_calc, tcl_children, tcl_source = _pick_total_or_build(
                total_candidates=['LiabilitiesCurrent', 'TotalCurrentLiabilities'],
                component_candidates=[
                'AccountsPayableCurrent',
                'AccruedLiabilitiesCurrent',
                'CurrentPortionOfLongTermDebt',
                ],
                section='liabilities'
            )
            if tcl_calc is not None:
                row['TotalCurrentLiabilities_Hierarchy'] = tcl_calc
            elif tcl_value is not None:
                row['TotalCurrentLiabilities_Hierarchy'] = tcl_value
            if tcl_value is not None and tcl_parent_key is not None:
                row['TotalCurrentLiabilities_Parent'] = tcl_value

            # Operating expenses: keep direct total if available, otherwise sum children.
            opx_total, _ = _pick_first_value(['OperatingExpenses', 'CostsAndExpenses'])
            rd = _num(row.get('ResearchAndDevelopmentExpense'))
            sga = _num(row.get('SellingGeneralAndAdministrativeExpense'))
            da = _num(row.get('DepreciationDepletionAndAmortization'))
            if opx_total is not None:
                row['OperatingExpenses_Hierarchy'] = opx_total
            elif rd is not None or sga is not None or da is not None:
                row['OperatingExpenses_Hierarchy'] = (rd or 0.0) + (sga or 0.0) + (da or 0.0)

            # Gross/Operating income: prefer direct parent, else calculate.
            gp_direct, _ = _pick_first_value(['GrossProfit'])
            rev_direct, _ = _pick_first_value(['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax'])
            cogs_direct, _ = _pick_first_value(['CostOfRevenue', 'CostOfGoodsAndServicesSold', 'CostOfSales'])
            if gp_direct is not None:
                row['GrossProfit_Hierarchy'] = gp_direct
            elif rev_direct is not None and cogs_direct is not None:
                row['GrossProfit_Hierarchy'] = rev_direct - cogs_direct

            op_direct, _ = _pick_first_value(['OperatingIncomeLoss'])
            gp_h = _num(row.get('GrossProfit_Hierarchy'))
            opx_h = _num(row.get('OperatingExpenses_Hierarchy'))
            if op_direct is not None:
                row['OperatingIncome_Hierarchy'] = op_direct
            elif gp_h is not None and opx_h is not None:
                row['OperatingIncome_Hierarchy'] = gp_h - opx_h

            # Strict hierarchy fields for AR/AP/Inventory/Cash (used by ratio engine).
            # Prefer canonical concept keys first to avoid double-counting aliases.
            ar_net = None
            ar_sum, ar_used = _sum_candidates([
                'AccountsReceivableNetCurrent',
                'VendorNonTradeReceivables',
                'ReceivablesNetCurrent',
            ])
            if ar_sum is not None:
                ar_net = ar_sum
            else:
                ar_net, _ = _pick_first_value([
                    'AccountsReceivableNetCurrent',
                    'ReceivablesNetCurrent',
                ])
            if ar_net is not None:
                row['AccountsReceivableNetCurrent_Hierarchy'] = ar_net

            ap_val, _ = _pick_first_value(['AccountsPayableCurrent', 'AccountsPayable'])
            if ap_val is not None:
                row['AccountsPayableCurrent_Hierarchy'] = ap_val

            inv_val, _ = _pick_first_value(['InventoryNet', 'InventoryFinishedGoods'])
            if inv_val is not None:
                row['InventoryNet_Hierarchy'] = inv_val

            int_exp_val, _ = _pick_first_value([
                'InterestExpense',
                'InterestExpenseNonoperating',
                'InterestAndDebtExpense',
                'InterestExpenseAndDebtExpense',
            ])
            if int_exp_val is not None:
                row['InterestExpense_Hierarchy'] = abs(int_exp_val)

            cash_val, _ = _pick_first_value(['CashAndCashEquivalentsAtCarryingValue', 'Cash'])
            if cash_val is not None:
                row['CashAndCashEquivalents_Hierarchy'] = cash_val

            # Validation rule: parent mismatch >5%
            parent_tca = _num(row.get('AssetsCurrent'))
            if parent_tca is not None and tca_calc is None:
                # Keep ratio engine strictly on hierarchy fields while preserving coverage.
                row['TotalCurrentAssets_Hierarchy'] = parent_tca
            if parent_tca is not None and tca_calc is not None and abs(parent_tca) > 1e-9:
                percent_scale = 100.0
                delta = abs(tca_calc - parent_tca) / abs(parent_tca)
                if delta > 0.05:
                    warnings.append({
                        'type': 'parent_child_mismatch',
                        'parent': 'AssetsCurrent',
                        'children_group': 'TotalCurrentAssets_Hierarchy',
                        'parent_value': parent_tca,
                        'children_sum': tca_calc,
                        'delta_pct': delta * percent_scale,
                    })

            parent_tcl = _num(row.get('LiabilitiesCurrent'))
            if parent_tcl is not None and tcl_calc is None:
                # Keep ratio engine strictly on hierarchy fields while preserving coverage.
                row['TotalCurrentLiabilities_Hierarchy'] = parent_tcl
            if parent_tcl is not None and tcl_calc is not None and abs(parent_tcl) > 1e-9:
                percent_scale = 100.0
                delta = abs(tcl_calc - parent_tcl) / abs(parent_tcl)
                if delta > 0.05:
                    warnings.append({
                        'type': 'parent_child_mismatch',
                        'parent': 'LiabilitiesCurrent',
                        'children_group': 'TotalCurrentLiabilities_Hierarchy',
                        'parent_value': parent_tcl,
                        'children_sum': tcl_calc,
                        'delta_pct': delta * percent_scale,
                    })

            # Net Income rule: Operating + Other - Tax (strict exclusions)
            def _pick_first(keys):
                for key in keys:
                    nv = _num(row.get(key))
                    if nv is None:
                        continue
                    lk = str(key).lower()
                    if 'accumulated' in lk or 'comprehensive' in lk:
                        continue
                    return nv, key
                return None, None

            def _pick_value(keys):
                for key in keys:
                    nv = _num(row.get(key))
                    if nv is not None:
                        return nv
                return None

            op_income, op_tag = _pick_first(['OperatingIncomeLoss'])
            other_income, other_tag = _pick_first(['NonoperatingIncomeExpense', 'OtherNonoperatingIncomeExpense', 'InterestAndOtherIncome'])
            tax_expense, tax_tag = _pick_first(['IncomeTaxExpenseBenefit'])
            if op_income is not None and other_income is not None and tax_expense is not None:
                ni_h = op_income + other_income - tax_expense
                row['NetIncomeLoss_Hierarchy'] = ni_h
                if _num(row.get('NetIncomeLoss')) is None:
                    row['NetIncomeLoss'] = ni_h
                warnings.append({
                    'type': 'net_income_hierarchy_trace',
                    'formula': 'Operating Income + Other Income - Tax Expense',
                    'operating_income_tag': op_tag,
                    'other_income_tag': other_tag,
                    'tax_expense_tag': tax_tag,
                    'computed': ni_h,
                })

            # Smart sniper (3-level): Direct -> Hierarchy -> Balance inference.
            if _num(row.get('Assets')) is None and _num(row.get('AssetsNet')) is None:
                ac = _pick_value(['AssetsCurrent', 'TotalCurrentAssets_Hierarchy', 'TotalCurrentAssets_Parent'])
                anc = _pick_value(['AssetsNoncurrent', 'NoncurrentAssets'])
                if ac is not None and anc is not None:
                    row['Assets'] = ac + anc
                    warnings.append({'type': 'golden_sniper', 'tag': 'Assets', 'grade': 'B', 'reason': 'sum_assets_current_noncurrent'})
                else:
                    bal = _pick_value(['LiabilitiesAndStockholdersEquity'])
                    if bal is not None:
                        row['Assets'] = bal
                        warnings.append({'type': 'golden_sniper', 'tag': 'Assets', 'grade': 'C', 'reason': 'from_liabilities_and_equity'})
                    else:
                        lv = _pick_value(['Liabilities'])
                        ev = _pick_value(['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'])
                        if lv is not None and ev is not None:
                            row['Assets'] = lv + ev
                            warnings.append({'type': 'golden_sniper', 'tag': 'Assets', 'grade': 'C', 'reason': 'from_balance_match'})

            if _num(row.get('Liabilities')) is None:
                cl = _pick_value(['LiabilitiesCurrent', 'TotalCurrentLiabilities_Hierarchy', 'TotalCurrentLiabilities_Parent'])
                ncl = _pick_value(['LiabilitiesNoncurrent', 'NoncurrentLiabilities'])
                if cl is not None and ncl is not None:
                    row['Liabilities'] = cl + ncl
                    warnings.append({'type': 'golden_sniper', 'tag': 'Liabilities', 'grade': 'B', 'reason': 'sum_current_noncurrent'})
                else:
                    av = _pick_value(['Assets'])
                    ev = _pick_value(['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'])
                    if av is not None and ev is not None:
                        row['Liabilities'] = av - ev
                        warnings.append({'type': 'golden_sniper', 'tag': 'Liabilities', 'grade': 'C', 'reason': 'from_balance_match'})

            if _num(row.get('StockholdersEquity')) is None and _num(row.get('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest')) is None:
                av = _pick_value(['Assets'])
                lv = _pick_value(['Liabilities'])
                if av is not None and lv is not None:
                    row['StockholdersEquity'] = av - lv
                    warnings.append({'type': 'golden_sniper', 'tag': 'StockholdersEquity', 'grade': 'C', 'reason': 'from_balance_match'})

            if _num(row.get('Revenues')) is None and _num(row.get('SalesRevenueNet')) is None and _num(row.get('RevenueFromContractWithCustomerExcludingAssessedTax')) is None:
                gpv = _pick_value(['GrossProfit', 'GrossProfit_Hierarchy'])
                cogs = _pick_value(['CostOfRevenue', 'CostOfGoodsAndServicesSold', 'CostOfSales'])
                if gpv is not None and cogs is not None:
                    row['Revenues'] = gpv + cogs
                    warnings.append({'type': 'golden_sniper', 'tag': 'Revenues', 'grade': 'B', 'reason': 'gross_plus_cogs'})

            if _num(row.get('NetIncomeLoss')) is None:
                ni_v = _pick_value(['NetIncomeLoss_Hierarchy'])
                if ni_v is not None:
                    row['NetIncomeLoss'] = ni_v
                    warnings.append({'type': 'golden_sniper', 'tag': 'NetIncomeLoss', 'grade': 'B', 'reason': 'hierarchy_formula'})

            # Unclassified routing (best-effort section inference)
            routed = []
            for concept, v in list(row.items()):
                if concept.startswith('_'):
                    continue
                nv = _num(v)
                if nv is None:
                    continue
                lc = str(concept).lower()
                if 'accumulated' in lc or 'comprehensive' in lc:
                    row['OtherEquity'] = (_num(row.get('OtherEquity')) or 0.0) + nv
                    routed.append({'concept': concept, 'bucket': 'OtherEquity', 'basis': 'strict_accumulated_comprehensive_to_equity'})
                    continue
                if 'liabil' in lc or 'payable' in lc or 'debt' in lc:
                    row['OtherLiabilities'] = (_num(row.get('OtherLiabilities')) or 0.0) + nv
                    routed.append({'concept': concept, 'bucket': 'OtherLiabilities', 'basis': 'section_inferred_liabilities'})
                elif 'asset' in lc or 'receivable' in lc or 'inventory' in lc or 'cash' in lc:
                    row['OtherAssets'] = (_num(row.get('OtherAssets')) or 0.0) + nv
                    routed.append({'concept': concept, 'bucket': 'OtherAssets', 'basis': 'section_inferred_assets'})
                elif 'income' in lc or 'expense' in lc or 'revenue' in lc:
                    row['OtherIncomeStatement'] = (_num(row.get('OtherIncomeStatement')) or 0.0) + nv
                    routed.append({'concept': concept, 'bucket': 'OtherIncomeStatement', 'basis': 'section_inferred_income_statement'})
                else:
                    is_known = any(tok in lc for tok in [
                        'asset', 'liabil', 'equity', 'revenue', 'income', 'expense',
                        'receivable', 'payable', 'inventory', 'cash', 'debt', 'tax'
                    ])
                    if is_known:
                        continue

            hierarchy_diag[str(year)] = {
                'warnings': warnings,
                'children_used': {
                    'TotalCurrentAssets_Hierarchy': tca_children,
                    'TotalCurrentLiabilities_Hierarchy': tcl_children,
                    'OperatingExpenses_Hierarchy': [t for t in ['ResearchAndDevelopmentExpense', 'SellingGeneralAndAdministrativeExpense'] if t in row],
                },
                'unclassified_routing': routed,
            }
            balance_check = self.Validate_Balance_Sheet(row)
            hierarchy_diag[str(year)]['balance_validation'] = balance_check
            row['_accounting_hierarchy_diagnostics'] = hierarchy_diag[str(year)]
        self.latest_hierarchy_diagnostics = hierarchy_diag
        return data_by_year

    def _calculate_financial_ratios(self, data_by_year):
        """
        Ø§Ù„Ø­Ø³Ø§Ø¨ ÙŠØ³ØªØ®Ø¯Ù… alt map Ø¯Ø§Ø®Ù„ÙŠ + latest_dynamic_map Ù„Ø§Ø®ØªÙŠØ§Ø± Ø£Ø³Ù…Ø§Ø¡ Ø¨Ø¯ÙŠÙ„Ø©.
        """
        ratios_by_year = {}
        prev_inventory = None
        prev_assets = None
        prev_total_liab = None
        prev_equity = None
        prev_ar = None
        prev_ap = None
        prev_loans = None
        prev_net_interest_income = None
        prev_premiums_earned = None
        prev_policy_claims = None
        prev_interest_expense = None
        prev_interest_rate = None
        prev_total_debt = None
        prev_deposits = None
        prev_shares_outstanding = None
        prev_inventory_turnover = None
        prev_net_income_to_assets = None
        prev_equity_ratio = None
        prev_carry_ratio_values = {}
        leading_missing_revenue_years = []
        carry_ratio_keys = [
            'gross_margin',
            'operating_margin',
            'net_margin',
            'ebitda_margin',
            'roa',
            'roe',
            'roic',
            'current_ratio',
            'quick_ratio',
            'cash_ratio',
            'interest_coverage',
            'asset_turnover',
            'inventory_turnover',
            'ocf_margin',
            'free_cash_flow',
            'fcf_per_share',
            'book_value_per_share',
            'debt_to_assets',
            'debt_to_equity',
            'net_debt_ebitda',
            'cost_of_debt',
            'wacc',
        ]
        allow_ratio_carry_forward = str(
            os.environ.get('ALLOW_RATIO_CARRY_FORWARD', '0')
        ).strip().lower() in ('1', 'true', 'yes')
        for year in sorted(data_by_year.keys()):
            data = data_by_year.get(year, {}) or {}
            ratios = {}

            def scaled_ratio(num, den, target=0.25, min_abs=0.0, max_abs=5.0):
                n = self._safe_float(num)
                d = self._safe_float(den)
                if n is None or d in (None, 0):
                    return None
                # Preserve direct canonical ratio whenever it is already plausible.
                try:
                    raw = n / d
                    if raw == raw and min_abs <= abs(raw) <= max_abs:
                        return raw
                except Exception:
                    pass
                scales = [1.0, 1e-3, 1e-6, 1e-9, 1e3, 1e6]
                candidates = []
                for sn in scales:
                    for sd in scales:
                        try:
                            den_scaled = d * sd
                            if den_scaled == 0:
                                continue
                            r = (n * sn) / den_scaled
                        except Exception:
                            continue
                        if r != r:
                            continue
                        if abs(r) < min_abs or abs(r) > max_abs:
                            continue
                        # Penalize aggressive scale shifts to avoid false ÷10/×10 choices.
                        try:
                            shift = abs(math.log10(abs(sn))) + abs(math.log10(abs(sd)))
                        except Exception:
                            shift = 0.0
                        score = abs(abs(r) - abs(target)) + (0.40 * shift)
                        candidates.append((score, r))
                if candidates:
                    candidates.sort(key=lambda x: x[0])
                    return candidates[0][1]
                try:
                    return n / d
                except Exception:
                    return None

            def get_val(key):
                v = data.get(key)
                if v is not None:
                    try:
                        fv = float(v)
                        lk = str(key or '').lower()
                        if not any(tok in lk for tok in ('share', 'per', 'ratio', 'margin', 'turnover', 'days', 'yield', 'score')):
                            # Raw statement layers sometimes mix absolute USD and million-USD facts.
                            # Normalize large absolute monetary values here before ratio math.
                            if abs(fv) >= 10_000_000.0:
                                fv = fv / 1_000_000.0
                        return fv
                    except:
                        return None
                # normalized key fallback to survive minor concept renaming
                try:
                    nk = self._normalize_concept_key(key)
                    if nk:
                        vv = semantic_index.get('norm_to_values', {}).get(nk)
                        if vv is not None:
                            fv = float(vv)
                            lk = str(key or '').lower()
                            if not any(tok in lk for tok in ('share', 'per', 'ratio', 'margin', 'turnover', 'days', 'yield', 'score')):
                                if abs(fv) >= 10_000_000.0:
                                    fv = fv / 1_000_000.0
                            return fv
                except Exception:
                    pass
                return None

            # âœ… ENHANCED: Comprehensive alt map using ALL discovered mappings
            alt = {
                'revenue': [],
                'cogs': [],
                'operating_income': [],
                'net_income': [],
                'net_interest_income': [],
                'interest_income': [],
                'noninterest_income': [],
                'noninterest_expense': [],
                'assets': [],
                'current_assets': [],
                'liabilities': [],
                'current_liabilities': [],
                'equity': [],
                'cet1': [],
                'loans': [],
                'deposits': [],
                'premiums_earned': [],
                'policy_claims': [],
                'ar': [],
                'inventory': [],
                'ap': [],
                'cash': [],
                'marketable_securities': [],
                'short_debt': [],
                'long_debt': [],
                'debt_current': [],
                'debt_noncurrent': [],
                'ocf': [],
                'capex': [],
                'dividends': [],
                'shares': [],
                'ebitda': [],
                'depreciation': [],
                'interest_expense': [],
                'gross_profit': []
            }
            
            # Merge with dynamic discovered mappings
            dyn = self.latest_dynamic_map or {}
            for bucket in alt.keys():
                if bucket in dyn:
                    # Prepend discovered names (higher priority)
                    alt[bucket] = dyn[bucket] + alt[bucket]
            
            # Add fallback hardcoded names for critical items
            alt['revenue'].extend([
                'Revenue', 'Revenues', 'SalesRevenueNet',
                'RevenueFromContractWithCustomerExcludingAssessedTax',
                'Net revenue', 'Net sales', 'Sales'
            ])
            alt['cogs'].extend([
                'COGS', 'CostOfRevenue', 'CostOfGoodsAndServicesSold',
                'CostOfSales', 'Cost of Revenue', 'Cost of Sales',
                'Cost of goods sold',
                'CostsAndExpenses',
                'OtherCostAndExpenseOperating',
            ])
            alt['gross_profit'].extend(['GrossProfit', 'Gross Profit'])
            alt['operating_income'].extend(['OperatingIncome', 'OperatingIncomeLoss', 'Operating Income'])
            alt['net_income'].extend(['NetIncome', 'NetIncomeLoss', 'ProfitLoss', 'Net Income'])
            alt['net_interest_income'].extend([
                'NetInterestIncome',
                'InterestIncomeNet',
                'InterestAndDividendIncomeOperating',
                'InterestIncomeExpenseAfterProvisionForLoanLoss',
                'Net interest income',
            ])
            alt['interest_income'].extend([
                'InterestIncomeOperating',
                'InterestAndDividendIncomeOperating',
                'InterestAndFeeIncomeLoansAndLeases',
                'Interest income',
            ])
            alt['noninterest_income'].extend([
                'NoninterestIncome',
                'TotalNoninterestIncome',
                'FeeRevenue',
                'OtherRevenue',
                'Noninterest income',
                'Total noninterest income',
            ])
            alt['noninterest_expense'].extend([
                'NoninterestExpense',
                'TotalNoninterestExpense',
                'Total noninterest expense',
                'OperatingExpenses',
                'OperatingExpenses_Hierarchy',
            ])
            alt['assets'].extend(['TotalAssets', 'Assets', 'Total Assets'])
            alt['current_assets'].extend(['CurrentAssets', 'AssetsCurrent', 'TotalCurrentAssets_Parent', 'Current Assets'])
            alt['liabilities'].extend(['TotalLiabilities', 'Liabilities', 'Total Liabilities'])
            alt['current_liabilities'].extend(['CurrentLiabilities', 'LiabilitiesCurrent', 'TotalCurrentLiabilities_Parent', 'Current Liabilities'])
            alt['equity'].extend(['TotalEquity', 'StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'Total Equity'])
            alt['cet1'].extend([
                'CommonEquityTier1Capital',
                'CommonEquityTier1CapitalRatio',
                'Tier1Capital',
                'CET1Capital',
            ])
            alt['loans'].extend([
                'LoansReceivable',
                'LoansAndLeasesReceivableNetReportedAmount',
                'LoansHeldForSale',
                'LoansAndLeasesReceivable',
                'NetLoans',
                'FinancingReceivableExcludingAccruedInterestBeforeAllowanceForCreditLoss',
                'FinancingReceivableExcludingAccruedInterestAfterAllowanceForCreditLoss',
            ])
            alt['deposits'].extend([
                'Deposits',
                'DepositLiabilities',
                'InterestBearingDepositsInBanks',
                'NoninterestBearingDeposits',
            ])
            alt['premiums_earned'].extend([
                'PremiumsEarned',
                'Premiums earned',
            ])
            alt['policy_claims'].extend([
                'PolicyholderBenefits',
                'PolicyholderBenefitsAndClaimsIncurredNet',
                'PolicyClaims',
                'LossAndLossAdjustmentExpense',
                'Claims and benefits',
            ])
            alt['ar'].extend([
                'AccountsReceivable', 'AccountsReceivableNetCurrent',
                'AccountsReceivableNetCurrent_Hierarchy', 'Accounts Receivable',
                'VendorNonTradeReceivables', 'Vendor non-trade receivables'
            ])
            alt['inventory'].extend(['Inventory', 'InventoryNet', 'InventoryNet_Hierarchy', 'Inventories'])
            alt['ap'].extend(['AccountsPayable', 'AccountsPayableCurrent', 'AccountsPayableCurrent_Hierarchy', 'Accounts Payable'])
            alt['cash'].extend(['CashAndCashEquivalents', 'CashAndCashEquivalentsAtCarryingValue', 'CashAndCashEquivalents_Hierarchy', 'Cash and Cash Equivalents'])
            alt['marketable_securities'].extend([
                'MarketableSecuritiesCurrent',
                'ShortTermInvestments',
                'AvailableForSaleSecuritiesDebtSecuritiesCurrent',
                'AvailableForSaleSecuritiesCurrent',
                'Marketable securities'
            ])
            alt['short_debt'].extend([
                'DebtCurrent',
                'ShortTermBorrowings',
                'CommercialPaper',
                'LongTermDebtCurrent',
                'CurrentPortionOfLongTermDebt'
            ])
            alt['long_debt'].extend([
                'LongTermDebtNoncurrent',
                'DebtNoncurrent',
                'LongTermDebt',
                'LongTermDebtAndCapitalLeaseObligations'
            ])
            alt['debt_current'].extend([
                'DebtCurrent',
                'ShortTermBorrowings',
                'CommercialPaper',
                'LongTermDebtCurrent',
                'CurrentPortionOfLongTermDebt'
            ])
            alt['debt_noncurrent'].extend([
                'LongTermDebtNoncurrent',
                'DebtNoncurrent',
                'LongTermDebt',
                'LongTermDebtAndCapitalLeaseObligations'
            ])
            alt['ocf'].extend(['OperatingCashFlow', 'NetCashProvidedByUsedInOperatingActivities', 'NetCashProvidedByOperatingActivities', 'Cash generated by operating activities', 'Operating Cash Flow'])
            alt['capex'].extend(['CapitalExpenditures', 'PaymentsToAcquirePropertyPlantAndEquipment', 'Capital Expenditures'])
            alt['shares'].extend(['SharesBasic', 'WeightedAverageNumberOfSharesOutstandingBasic'])
            alt['shares'].extend([
                'CommonStockSharesOutstanding',
                'EntityCommonStockSharesOutstanding',
                'WeightedAverageNumberOfDilutedSharesOutstanding',
                'WeightedAverageNumberOfShareOutstandingBasicAndDiluted',
            ])
            alt['depreciation'].extend(['DepreciationAmortization', 'DepreciationDepletionAndAmortization'])
            alt['dividends'].extend(['DividendsPaid', 'Dividends Paid'])
            alt['interest_expense'].extend([
                'InterestExpense',
                'InterestExpense_Hierarchy',
                'InterestExpenseNonoperating',
                'InterestAndDebtExpense',
                'InterestExpenseAndDebtExpense',
                'InterestExpenseDebt',
                'InterestExpenseBorrowings',
                'InterestExpenseDebtAndCapitalLeaseObligations',
                'InterestExpenseDebtExcludingAmortization',
                'InterestExpenseDeposits',
                'InterestPaidNet',
                'InterestPaid',
                'Cash paid for interest'
            ])
            
            # Remove duplicates while preserving order
            for bucket in alt.keys():
                seen = set()
                unique = []
                for item in alt[bucket]:
                    if item not in seen:
                        seen.add(item)
                        unique.append(item)
                alt[bucket] = unique
            semantic_index = self._build_semantic_concept_index(data)

            def pick(*keys):
                """âœ… ENHANCED: Intelligent picker with debug output"""
                # direct check
                for k in keys:
                    v = get_val(k)
                    if v is not None:
                        return v
                # check alt map (including dynamic entries)
                for k in keys:
                    if k in alt and alt[k]:  # Check if bucket exists and has items
                        for altk in alt[k]:
                            v = get_val(altk)
                            if v is not None:
                                # Debug: show which alternative was used
                                if altk not in keys:
                                    print(f"      ðŸ“Œ '{k}' â†’ using '{altk}'")
                                return v
                # semantic fallback for issuer/sector naming drift (safe buckets only)
                semantic_safe_buckets = {
                    'revenue', 'cogs', 'gross_profit', 'operating_income', 'net_income',
                    'assets', 'current_assets', 'liabilities', 'current_liabilities', 'equity',
                    'ar', 'ap', 'inventory', 'cash', 'ocf', 'capex', 'shares',
                    'interest_expense', 'depreciation', 'dividends',
                }
                for k in keys:
                    if k in alt and k in semantic_safe_buckets:
                        sv, sk = self._semantic_pick_bucket_value(k, semantic_index)
                        if sv is not None:
                            print(f"      🧠 semantic '{k}' → '{sk}'")
                            return sv
                return None

            def pick_exact_candidates(candidates):
                for ck in candidates:
                    v = get_val(ck)
                    if v is not None:
                        return float(v), ck
                return None, None

            def pick_semantic_one(any_tokens, exclude_tokens=None):
                exclude_tokens = exclude_tokens or []
                best = None
                best_key = None
                for k, v in (data or {}).items():
                    vv = get_val(k)
                    if vv is None:
                        continue
                    lk = str(k).lower()
                    if not any(tok in lk for tok in any_tokens):
                        continue
                    if any(tok in lk for tok in exclude_tokens):
                        continue
                    if best is None or abs(vv) > abs(best):
                        best = float(vv)
                        best_key = k
                return best, best_key

            def align_to_reference(
                value,
                reference,
                target=0.5,
                min_ratio=-2.0,
                max_ratio=2.0,
                min_abs_ratio=0.0,
                preserve_if_plausible=True,
            ):
                """
                Align component scale to reference (typically revenue/assets) when
                filings mix units (absolute vs millions).
                """
                v = self._safe_float(value)
                ref = self._safe_float(reference)
                if v is None or ref in (None, 0):
                    return v
                base_ratio = v / ref
                if (
                    preserve_if_plausible
                    and min_ratio <= base_ratio <= max_ratio
                    and abs(base_ratio) >= float(min_abs_ratio or 0.0)
                ):
                    return v
                scales = [1.0, 1e-3, 1e-6, 1e-9, 1e3, 1e6]
                cands = []
                for s in scales:
                    vv = v * s
                    r = vv / ref
                    if r != r:
                        continue
                    if r < min_ratio or r > max_ratio:
                        continue
                    if abs(r) < float(min_abs_ratio or 0.0):
                        continue
                    try:
                        shift_penalty = abs(math.log10(abs(s))) if s not in (0, 1.0) else 0.0
                    except Exception:
                        shift_penalty = 0.0
                    target_penalty = abs(abs(r) - abs(target))
                    cands.append((target_penalty + (0.15 * shift_penalty), vv))
                if cands:
                    cands.sort(key=lambda x: x[0])
                    return cands[0][1]
                return v

            def pick_best_scaled_denominator(
                numerator,
                raw_candidates,
                target_ratio=0.20,
                min_ratio=0.01,
                max_ratio=1.0,
            ):
                """
                Pick the most plausible denominator scale from candidate facts.
                Useful when filings mix absolute dollars and million-dollar facts.
                """
                num = self._safe_float(numerator)
                if num in (None, 0):
                    return None
                best = None
                scales = [1.0, 1e-3, 1e-6, 1e-9, 1e3, 1e6]
                for raw in raw_candidates:
                    den = self._safe_float(raw)
                    if den in (None, 0):
                        continue
                    for s in scales:
                        dv = den * s
                        if dv == 0:
                            continue
                        ratio = abs(num / dv)
                        if ratio < float(min_ratio) or ratio > float(max_ratio):
                            continue
                        try:
                            shift_penalty = abs(math.log10(abs(s))) if s not in (0, 1.0) else 0.0
                        except Exception:
                            shift_penalty = 0.0
                        score = abs(ratio - float(target_ratio)) + (0.10 * shift_penalty)
                        if best is None or score < best[0]:
                            best = (score, dv)
                return best[1] if best else None

            def resolve_safe_depreciation(revenue_value, op_value=None, ebitda_direct=None):
                """
                Resolve D&A conservatively:
                - reject contaminated labels
                - normalize obvious absolute-USD slips
                - if missing/zero/implausible, infer from nearby years' D&A/revenue ratio
                - fallback to direct EBITDA - operating income when safe
                """
                reject_tokens = {
                    'accumulated', 'sharebased', 'stockbased', 'compensation',
                    'impairment', 'gain', 'loss', 'other', 'restructuring',
                    'credit', 'allowance',
                }
                candidate_vals = []
                for raw_key, raw_val in (data or {}).items():
                    rk = str(raw_key or '').lower().replace('_', '').replace('-', '')
                    if ('depreciation' not in rk and 'amortization' not in rk) or 'accumulated' in rk:
                        continue
                    if any(tok in rk for tok in reject_tokens):
                        continue
                    fv = self._safe_float(raw_val)
                    if fv is None:
                        continue
                    if abs(fv) >= 10_000_000.0:
                        fv = fv / 1_000_000.0
                    if fv > 0:
                        candidate_vals.append(float(fv))

                dep_val = max(candidate_vals) if candidate_vals else None
                rev_abs = abs(float(revenue_value)) if revenue_value not in (None, 0) else None
                if dep_val is not None and rev_abs not in (None, 0):
                    dep_ratio = dep_val / rev_abs
                    if dep_ratio < 0.0005 or dep_ratio > 0.35:
                        dep_val = None

                if dep_val is None and rev_abs not in (None, 0):
                    ratio_candidates = []
                    for ny in (year - 1, year + 1, year - 2, year + 2):
                        nrow = data_by_year.get(ny, {}) if isinstance(data_by_year, dict) else {}
                        if not isinstance(nrow, dict):
                            continue
                        nrev = None
                        for rev_key in (
                            'Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax',
                            'Revenue', 'SalesRevenueNet', 'TotalRevenue', 'TotalNetRevenues'
                        ):
                            rv = self._safe_float(nrow.get(rev_key))
                            if rv is None:
                                continue
                            if abs(rv) >= 10_000_000.0:
                                rv = rv / 1_000_000.0
                            if rv > 0:
                                nrev = max(nrev or rv, rv)
                        if nrev in (None, 0):
                            continue
                        ndep_vals = []
                        for nk, nv in nrow.items():
                            nlk = str(nk or '').lower().replace('_', '').replace('-', '')
                            if ('depreciation' not in nlk and 'amortization' not in nlk) or 'accumulated' in nlk:
                                continue
                            if any(tok in nlk for tok in reject_tokens):
                                continue
                            fv = self._safe_float(nv)
                            if fv is None:
                                continue
                            if abs(fv) >= 10_000_000.0:
                                fv = fv / 1_000_000.0
                            if fv > 0:
                                ndep_vals.append(float(fv))
                        if not ndep_vals:
                            continue
                        ndep = max(ndep_vals)
                        ratio = ndep / abs(float(nrev))
                        if 0.0005 <= ratio <= 0.35:
                            ratio_candidates.append(ratio)
                    if ratio_candidates:
                        ratio_candidates.sort()
                        mid = ratio_candidates[len(ratio_candidates) // 2]
                        dep_val = mid * rev_abs

                if dep_val is None and ebitda_direct is not None and op_value is not None:
                    try:
                        ebitda_gap = float(ebitda_direct) - float(op_value)
                    except Exception:
                        ebitda_gap = None
                    if ebitda_gap is not None and ebitda_gap > 0:
                        if rev_abs in (None, 0):
                            dep_val = ebitda_gap
                        else:
                            gap_ratio = ebitda_gap / rev_abs
                            if 0.0005 <= gap_ratio <= 0.35:
                                dep_val = ebitda_gap

                return dep_val

            # âœ… ENHANCED: fetch base numbers using comprehensive mapping
            revenue, revenue_label, revenue_source_kind = self._pick_canonical_label_value(data, 'Revenue')
            if revenue is None:
                revenue = pick('Revenues', 'Revenue', 'revenue')
                revenue_label = 'dynamic_pick' if revenue is not None else None
                revenue_source_kind = 'fallback' if revenue is not None else 'missing'
            if revenue is None:
                # Last controlled fallback with strict priority:
                # RFCC first (ASC606-compatible), then Revenues/SalesRevenueNet.
                revenue = pick('RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues', 'SalesRevenueNet')
                if revenue is not None:
                    revenue_label = 'RFCC/Revenues/Fallback'
                    revenue_source_kind = 'fallback'
            if revenue is None:
                revenue_sem, _ = pick_semantic_one(
                    ['revenue', 'sales'],
                    ['cost', 'expense', 'tax', 'per share', 'deferred']
                )
                revenue = revenue_sem
                if revenue is not None and revenue_label is None:
                    revenue_label = 'semantic_revenue'
                    revenue_source_kind = 'fallback'

            cogs, cogs_label, cogs_source_kind = self._pick_canonical_label_value(data, 'COGS')
            if cogs is None:
                cogs = pick('CostOfRevenue', 'Cost of Revenue', 'COGS', 'cogs') or None
                if cogs is not None:
                    cogs_label = 'dynamic_pick'
                    cogs_source_kind = 'fallback'
            if cogs is None:
                cogs = pick('CostOfGoodsAndServicesSold', 'CostOfSales') or None
                if cogs is not None:
                    cogs_label = 'CostOfGoodsAndServicesSold/Fallback'
                    cogs_source_kind = 'fallback'
            if cogs is None:
                cogs_sem, _ = pick_semantic_one(
                    ['cost of revenue', 'cost of sales', 'cost of goods', 'cost'],
                    ['operating', 'interest', 'tax', 'other comprehensive']
                )
                cogs = cogs_sem
                if cogs is not None and cogs_label is None:
                    cogs_label = 'semantic_cogs'
                    cogs_source_kind = 'fallback'

            # Revenue year-shift guard:
            # if Revenues appears shifted (matches next year) and RFCC exists, prefer RFCC.
            rev_rfcc = get_val('RevenueFromContractWithCustomerExcludingAssessedTax')
            rev_sales_net = get_val('SalesRevenueNet')
            rev_revenues = get_val('Revenues')
            next_row = data_by_year.get(year + 1, {}) if isinstance(data_by_year, dict) else {}
            next_rev = None
            if isinstance(next_row, dict):
                next_rev = (
                    self._safe_float(next_row.get('Revenues'))
                    or self._safe_float(next_row.get('Revenue'))
                    or self._safe_float(next_row.get('SalesRevenueNet'))
                    or self._safe_float(next_row.get('RevenueFromContractWithCustomerExcludingAssessedTax'))
                )
            if revenue is not None:
                candidates = []
                if rev_rfcc is not None:
                    candidates.append(('rfcc', rev_rfcc))
                if rev_revenues is not None:
                    candidates.append(('revenues', rev_revenues))
                if rev_sales_net is not None:
                    candidates.append(('salesnet', rev_sales_net))
                if next_rev is not None:
                    for cname, cval in candidates:
                        if cval is None:
                            continue
                        # Strong duplicate-shift signal: current revenue equals next-year revenue.
                        if abs(float(revenue) - float(next_rev)) <= max(1.0, abs(float(next_rev)) * 1e-9):
                            if abs(float(cval) - float(revenue)) > max(1.0, abs(float(revenue)) * 0.01):
                                revenue = float(cval)
                                revenue_label = f'{cname}_shift_guard'
                                revenue_source_kind = 'canonical'
                                break
            # Canonical collision resolver (revenue):
            # if RFCC and Revenues diverge materially, do NOT blindly pick smaller.
            # Prefer RFCC/SalesRevenueNet agreement first (ASC606 anchor),
            # then apply a plausibility-driven denominator pick after net/op are known.
            if rev_rfcc is not None and rev_revenues is not None:
                try:
                    rf = abs(float(rev_rfcc))
                    rv = abs(float(rev_revenues))
                except Exception:
                    rf, rv = None, None
                if rf not in (None, 0) and rv not in (None, 0):
                    diff_ratio = abs(rf - rv) / max(min(rf, rv), 1.0)
                    if diff_ratio >= 0.20:
                        # Strong anchor: RFCC == SalesRevenueNet (or very close) is usually
                        # the intended annual top-line for industrial/technology issuers.
                        try:
                            rs = abs(float(rev_sales_net)) if rev_sales_net is not None else None
                        except Exception:
                            rs = None
                        if rs not in (None, 0) and abs(rs - rf) / max(min(rs, rf), 1.0) <= 0.02:
                            revenue = float(rev_rfcc)
                            revenue_label = 'rfcc_salesnet_collision_guard'
                            revenue_source_kind = 'canonical'
            # Strong revenue scale guard:
            # when RFCC exists and current selected revenue is grossly mis-scaled/shifted,
            # prefer RFCC as the annual anchor.
            if revenue is not None and rev_rfcc is not None:
                try:
                    rr = abs(float(revenue))
                    rf = abs(float(rev_rfcc))
                except Exception:
                    rr, rf = None, None
                if rr not in (None, 0) and rf not in (None, 0):
                    scale_ratio = max(rr, rf) / max(min(rr, rf), 1.0)
                    # Example: 55,000,000 vs 55 or clear one-year shifted revenue track.
                    if scale_ratio >= 1_000.0 or (rr < 10_000.0 and rf > 50_000.0):
                        revenue = float(rev_rfcc)
                        revenue_label = 'rfcc_scale_guard'
                        revenue_source_kind = 'canonical'
                    elif rr > 50_000.0 and rf > 50_000.0 and next_rev is not None:
                        # If selected revenue equals previous/next shifted track, RFCC is safer.
                        if abs(float(revenue) - float(next_rev)) <= max(1.0, abs(float(next_rev)) * 1e-9):
                            revenue = float(rev_rfcc)
                            revenue_label = 'rfcc_year_shift_guard'
                            revenue_source_kind = 'canonical'
            # Canonical collision resolver (COGS):
            # prefer CostOfGoodsAndServicesSold when CostOfRevenue is materially larger.
            cogs_goods = get_val('CostOfGoodsAndServicesSold')
            cogs_rev = get_val('CostOfRevenue')
            if cogs_goods is not None and cogs_rev is not None:
                try:
                    cg = abs(float(cogs_goods))
                    cr = abs(float(cogs_rev))
                except Exception:
                    cg, cr = None, None
                if cg not in (None, 0) and cr not in (None, 0):
                    cogs_gap = abs(cg - cr) / max(min(cg, cr), 1.0)
                    if cogs_gap >= 0.20:
                        if cg <= cr:
                            cogs = float(cogs_goods)
                            cogs_label = 'CostOfGoodsAndServicesSold/collision_guard'
                        else:
                            cogs = float(cogs_rev)
                            cogs_label = 'CostOfRevenue/collision_guard'
                        cogs_source_kind = 'canonical'
            if revenue is not None and cogs is not None:
                cogs = align_to_reference(
                    cogs,
                    revenue,
                    target=0.65,
                    min_ratio=-2.0,
                    max_ratio=2.0,
                    min_abs_ratio=0.05,
                )
            gross = pick('gross_profit') or (revenue - cogs if revenue and cogs is not None else None)
            if cogs is None and revenue and gross is not None:
                cogs = revenue - gross
            if revenue is not None and gross is not None:
                gross = align_to_reference(
                    gross,
                    revenue,
                    target=0.35,
                    min_ratio=-2.0,
                    max_ratio=2.0,
                    min_abs_ratio=0.05,
                )
            if gross is None and revenue is not None and cogs is not None:
                gross = revenue - cogs
            if revenue is not None and cogs is not None:
                gross_expected = revenue - cogs
                if gross is None or abs(gross - gross_expected) > max(1.0, abs(revenue) * 0.02):
                    gross = gross_expected
            op = pick('operating_income') or None
            if op is None:
                op_sem, _ = pick_semantic_one(
                    ['operating income', 'income from operations', 'operating profit'],
                    ['comprehensive', 'other', 'tax']
                )
                op = op_sem
            net, net_label, net_source_kind = self._pick_canonical_label_value(data, 'NetIncome')
            if net is None:
                net = pick('NetIncomeLoss', 'NetIncome', 'Net income', 'net_income') or None
                if net is not None:
                    net_label = 'dynamic_pick'
                    net_source_kind = 'fallback'
            if net is None:
                # Last-resort only (kept explicit because ProfitLoss may arrive in different unit context).
                net = pick('ProfitLoss') or None
                if net is not None:
                    net_label = 'ProfitLoss/Fallback'
                    net_source_kind = 'fallback'
            # Final revenue arbitration with profitability plausibility:
            # choose the denominator that yields realistic margins first, not the smallest raw number.
            revenue_candidates = [revenue, rev_rfcc, rev_sales_net, rev_revenues]
            revenue_candidates = [self._safe_float(x) for x in revenue_candidates if self._safe_float(x) not in (None, 0)]
            if revenue_candidates:
                # Prefer net-income-based denominator when available; otherwise operating-income.
                probe_num = net if net is not None else op
                probe_target = 0.12 if net is not None else 0.20
                probe_min = 0.005 if net is not None else 0.01
                probe_max = 1.0 if net is not None else 1.5
                if probe_num not in (None, 0):
                    best_revenue = pick_best_scaled_denominator(
                        numerator=probe_num,
                        raw_candidates=revenue_candidates,
                        target_ratio=probe_target,
                        min_ratio=probe_min,
                        max_ratio=probe_max,
                    )
                    if best_revenue not in (None, 0):
                        try:
                            # Apply only when materially different to avoid noisy churn.
                            if revenue in (None, 0) or abs(float(best_revenue) - float(revenue)) / max(abs(float(revenue or 1.0)), 1.0) >= 0.20:
                                revenue = float(best_revenue)
                                revenue_label = 'profitability_denominator_guard'
                                revenue_source_kind = 'canonical'
                        except Exception:
                            revenue = float(best_revenue)
                            revenue_label = 'profitability_denominator_guard'
                            revenue_source_kind = 'canonical'
            if revenue is not None and op is not None:
                op = align_to_reference(
                    op,
                    revenue,
                    target=0.15,
                    min_ratio=-1.5,
                    max_ratio=1.5,
                    min_abs_ratio=0.01,
                )
            if revenue is not None and net is not None:
                net = align_to_reference(
                    net,
                    revenue,
                    target=0.08,
                    min_ratio=-1.0,
                    max_ratio=1.0,
                    min_abs_ratio=0.005,
                )
            net_interest_income = pick('net_interest_income') or None
            interest_income = pick('interest_income') or None
            noninterest_income = pick('noninterest_income') or None
            noninterest_expense = pick('noninterest_expense') or None
            ebitda_direct = pick('ebitda')
            dep = resolve_safe_depreciation(revenue, op_value=op, ebitda_direct=ebitda_direct)
            if revenue is not None and dep is not None:
                dep = align_to_reference(
                    dep,
                    revenue,
                    target=0.04,
                    min_ratio=0.0,
                    max_ratio=0.35,
                    min_abs_ratio=0.0005,
                )
            opx_total = pick('OperatingExpenses', 'OperatingExpenses_Hierarchy', 'CostsAndExpenses')
            if revenue is not None and opx_total is not None:
                opx_total = align_to_reference(
                    opx_total,
                    revenue,
                    target=0.20,
                    min_ratio=0.0,
                    max_ratio=1.5,
                    min_abs_ratio=0.01,
                )
            if op is None and gross is not None and opx_total is not None:
                op = gross - opx_total
            ebitda = ebitda_direct or (op + dep if op is not None and dep is not None else None)
            if ebitda is not None and op is not None and ebitda < op:
                if dep is not None and dep > 0:
                    ebitda = op + dep
                else:
                    ebitda = op
            assets, assets_label, assets_source_kind = self._pick_canonical_label_value(data, 'Assets')
            if assets is None:
                assets = pick('Assets', 'TotalAssets', 'assets')
                if assets is not None:
                    assets_label = 'dynamic_pick'
                    assets_source_kind = 'fallback'
            # Prefer canonical parent totals; hierarchy is guarded fallback only.
            curr_assets = pick('current_assets')
            curr_liab = pick('current_liabilities')
            total_liab = pick('liabilities')
            equity, equity_label, equity_source_kind = self._pick_canonical_label_value(data, 'Equity')
            if equity is None:
                equity = pick('StockholdersEquity', 'Total Equity', 'equity')
                if equity is not None:
                    equity_label = 'dynamic_pick'
                    equity_source_kind = 'fallback'
            if equity is not None and assets is not None and abs(assets) > 0:
                # Guardrail: control totals can equal assets and must never be used as equity.
                if abs(abs(equity) - abs(assets)) / abs(assets) < 0.01:
                    equity = get_val('StockholdersEquity') or get_val('Total Equity') or None
                    equity_label = 'StockholdersEquity_guarded'
                    equity_source_kind = 'canonical'
            inventory = pick('inventory')
            ar = pick('ar')
            # Aggregate AR for DSO without double-counting alias tags.
            try:
                base_ar = None
                for ar_key in (
                    'AccountsReceivableNetCurrent',
                    'AccountsReceivable',
                    'AccountsReceivableNetCurrent_Hierarchy',
                ):
                    ar_v = get_val(ar_key)
                    if ar_v is not None:
                        base_ar = abs(float(ar_v))
                        break
                financing_ar = 0.0
                for ar_key in (
                    'FinancingReceivableExcludingAccruedInterestBeforeAllowanceForCreditLoss',
                    'FinancingReceivableExcludingAccruedInterestAfterAllowanceForCreditLoss',
                ):
                    ar_v = get_val(ar_key)
                    if ar_v is not None:
                        financing_ar += abs(float(ar_v))
                if base_ar is not None:
                    ar = float(base_ar + financing_ar)
                elif financing_ar > 0:
                    ar = float(financing_ar)
            except Exception:
                pass
            ap, ap_source_tag = pick_exact_candidates(
                [
                    'AccountsPayableCurrent_Hierarchy',
                    'AccountsPayableCurrent',
                    'AccountsPayable',
                    'Accounts Payable',
                ]
            )
            if ap is None:
                ap = pick('ap')
                ap_source_tag = 'semantic_or_fallback'
            else:
                ratios['accounts_payable_source'] = ap_source_tag
            if curr_assets is None:
                h_curr_assets = get_val('TotalCurrentAssets_Hierarchy')
                if h_curr_assets is not None:
                    component_floor = 0.0
                    for comp_key in (
                        'CashAndCashEquivalentsAtCarryingValue',
                        'AccountsReceivableNetCurrent',
                        'InventoryNet',
                    ):
                        comp_val = get_val(comp_key)
                        if comp_val is not None:
                            component_floor += abs(float(comp_val))
                    if component_floor == 0.0 or abs(float(h_curr_assets)) >= (0.8 * component_floor):
                        curr_assets = float(h_curr_assets)
            h_curr_liab = get_val('TotalCurrentLiabilities_Hierarchy')
            if h_curr_liab is not None:
                component_floor = 0.0
                for comp_key in (
                    'AccountsPayableCurrent',
                    'AccruedLiabilitiesCurrent',
                    'CurrentPortionOfLongTermDebt',
                ):
                    comp_val = get_val(comp_key)
                    if comp_val is not None:
                        component_floor += abs(float(comp_val))
                hierarchy_ok = (component_floor == 0.0 or abs(float(h_curr_liab)) >= (0.8 * component_floor))
                if hierarchy_ok:
                    if curr_liab is None:
                        curr_liab = float(h_curr_liab)
                    else:
                        # Prefer hierarchy total when semantic single-line pick underestimates total.
                        if abs(float(h_curr_liab)) > (abs(float(curr_liab)) * 1.20):
                            curr_liab = float(h_curr_liab)
            marketable_securities = pick('marketable_securities') or 0.0
            loans = pick('loans')
            deposits = pick('deposits')
            cet1 = pick('cet1')
            premiums_earned = pick('premiums_earned')
            policy_claims = pick('policy_claims')
            if premiums_earned is None:
                premiums_earned, _ = pick_semantic_one(
                    ['premium', 'premiums earned', 'earned premium'],
                    ['per share', 'deferred tax']
                )
            if policy_claims is None:
                policy_claims, _ = pick_semantic_one(
                    ['policyholder benefit', 'claim', 'loss and loss adjustment', 'claims and benefits'],
                    ['tax', 'per share']
                )

            # Bank semantic fallbacks for renamed issuer-specific labels.
            if loans is None:
                loans, _ = pick_semantic_one(
                    ['loan', 'loans receivable', 'loans and leases', 'net loans'],
                    ['allowance', 'loss', 'nonperform', 'reserve', 'provision']
                )
            if deposits is None:
                deposits, _ = pick_semantic_one(
                    ['deposit', 'deposits'],
                    ['insurance', 'premium']
                )
            # Bank anchor resolver: prefer stable balance anchors over generic proxies.
            # Only activates when anchors are missing (or unstable) to preserve previously-good behavior.
            try:
                if loans is None:
                    l_val, l_tag, l_conf, l_det = self._resolve_bank_anchor_value(
                        data,
                        kind='loans',
                        assets=assets,
                        prev_value=prev_loans,
                    )
                    if l_val not in (None, 0) and l_conf >= 0.55:
                        loans = l_val
                        ratios['loans_anchor_tag'] = l_tag
                        ratios['loans_anchor_confidence'] = l_conf
                        ratios['loans_anchor_details'] = l_det
                if deposits is None:
                    d_val, d_tag, d_conf, d_det = self._resolve_bank_anchor_value(
                        data,
                        kind='deposits',
                        assets=assets,
                        prev_value=prev_deposits,
                    )
                    if d_val not in (None, 0) and d_conf >= 0.55:
                        deposits = d_val
                        ratios['deposits_anchor_tag'] = d_tag
                        ratios['deposits_anchor_confidence'] = d_conf
                        ratios['deposits_anchor_details'] = d_det
            except Exception:
                pass
            # Detect bank profile from native anchors only (before synthetic proxies).
            native_bank_signal = any(v is not None for v in (net_interest_income, loans, deposits, cet1))
            # Bank proxy hardening: if explicit deposit/loan tags are absent,
            # derive conservative proxies to avoid structural N/A for banking ratios.
            if (native_bank_signal or prev_deposits is not None) and deposits is None and total_liab not in (None, 0):
                try:
                    deposits = abs(float(total_liab)) * 0.80
                    ratios['deposits_proxy_used'] = 'TOTAL_LIABILITIES_X_0_80'
                except Exception:
                    deposits = None
            if (native_bank_signal or prev_deposits is not None) and loans is None and assets not in (None, 0):
                try:
                    cash_floor = abs(float(cash)) if cash is not None else 0.0
                    loans = max(abs(float(assets)) - cash_floor, 0.0)
                    ratios['loans_proxy_used'] = 'ASSETS_MINUS_CASH_PROXY'
                except Exception:
                    loans = None
            if net_interest_income is None:
                net_interest_income, _ = pick_semantic_one(
                    ['net interest income', 'interest income net'],
                    ['noninterest', 'expense']
                )
            if noninterest_income is None:
                noninterest_income, _ = pick_semantic_one(
                    ['noninterest income', 'fee income', 'other income'],
                    ['expense', 'tax', 'comprehensive']
                )
            if noninterest_expense is None:
                noninterest_expense, _ = pick_semantic_one(
                    ['noninterest expense', 'operating expense', 'total noninterest expense'],
                    ['interest income', 'tax benefit', 'comprehensive']
                )

            # Bank signal must be driven by bank-specific anchors only.
            # Noninterest income exists for many industrial issuers and must not
            # trigger bank logic by itself.
            provisional_bank_signal = any(v is not None for v in (net_interest_income, loans, deposits, cet1))
            provisional_insurance_signal = any(v is not None for v in (premiums_earned, policy_claims))
            bank_context = provisional_bank_signal or (prev_deposits is not None)
            insurance_context = provisional_insurance_signal or (prev_premiums_earned is not None)
            # For incomplete latest-year filings, anchor carry-forward is disabled by default.
            # It can be explicitly enabled if needed.
            allow_anchor_proxy = str(os.environ.get('ALLOW_ANCHOR_PROXY_CARRY_FORWARD', '0')).strip().lower() in ('1', 'true', 'yes')
            if allow_anchor_proxy and (bank_context or insurance_context):
                proxy_used = False
                if assets is None and prev_assets is not None:
                    assets = prev_assets
                    proxy_used = True
                if total_liab is None and prev_total_liab is not None:
                    total_liab = prev_total_liab
                    proxy_used = True
                if equity is None and prev_equity is not None:
                    equity = prev_equity
                    proxy_used = True
                if deposits is None and prev_deposits is not None:
                    deposits = prev_deposits
                    proxy_used = True
                if loans is None and prev_loans is not None:
                    loans = prev_loans
                    proxy_used = True
                if net_interest_income is None and prev_net_interest_income is not None:
                    net_interest_income = prev_net_interest_income
                    proxy_used = True
                if premiums_earned is None and prev_premiums_earned is not None:
                    premiums_earned = prev_premiums_earned
                    proxy_used = True
                if policy_claims is None and prev_policy_claims is not None:
                    policy_claims = prev_policy_claims
                    proxy_used = True
                if proxy_used:
                    ratios['balance_anchor_proxy_source'] = 'PREV_YEAR_CARRY_FORWARD'

            # Bank statements can expose multiple revenue concepts (NII-only vs total revenue).
            # Pick denominator that gives a plausible margin for the detected sector.
            if net is not None:
                revenue_candidates = [
                    revenue,
                    get_val('Revenues'),
                    get_val('SalesRevenueNet'),
                    get_val('RevenueFromContractWithCustomerExcludingAssessedTax'),
                ]
                if net_interest_income is not None:
                    revenue_candidates.append(net_interest_income)
                if net_interest_income is not None and noninterest_income is not None:
                    revenue_candidates.append(float(net_interest_income) + float(noninterest_income))
                if provisional_bank_signal:
                    tuned_revenue = pick_best_scaled_denominator(
                        numerator=net,
                        raw_candidates=revenue_candidates,
                        target_ratio=0.22,
                        min_ratio=0.01,
                        max_ratio=0.60,
                    )
                    if tuned_revenue is not None:
                        revenue = tuned_revenue

            # Normalize major balance-sheet totals against revenue scale to prevent
            # mixed-unit years (absolute dollars vs millions) from breaking ratios.
            if revenue not in (None, 0):
                asset_target = 18.0 if provisional_bank_signal else (6.0 if provisional_insurance_signal else 2.0)
                assets = align_to_reference(
                    assets,
                    revenue,
                    target=asset_target,
                    min_ratio=0.10,
                    max_ratio=400.0,
                    min_abs_ratio=0.05,
                )
                total_liab = align_to_reference(
                    total_liab,
                    assets,
                    target=0.85,
                    min_ratio=0.02,
                    max_ratio=3.0,
                    min_abs_ratio=0.01,
                )
                equity = align_to_reference(
                    equity,
                    assets,
                    target=0.15,
                    min_ratio=-1.0,
                    max_ratio=2.0,
                    min_abs_ratio=0.01,
                )
                curr_assets = align_to_reference(
                    curr_assets,
                    assets,
                    target=0.30 if provisional_bank_signal else 0.45,
                    min_ratio=0.01,
                    max_ratio=1.5,
                    min_abs_ratio=0.01,
                )
                curr_liab = align_to_reference(
                    curr_liab,
                    assets,
                    target=0.20 if provisional_bank_signal else 0.35,
                    min_ratio=0.01,
                    max_ratio=1.5,
                    min_abs_ratio=0.01,
                )
                loans = align_to_reference(
                    loans,
                    assets,
                    target=0.45,
                    min_ratio=0.01,
                    max_ratio=2.5,
                    min_abs_ratio=0.01,
                )
                deposits = align_to_reference(
                    deposits,
                    assets,
                    target=0.55,
                    min_ratio=0.01,
                    max_ratio=3.0,
                    min_abs_ratio=0.01,
                )

            # Strict debt definition:
            # - Corporate: DebtCurrent + LongTermDebtNoncurrent
            # - Bank-like: LongTermDebt + CommercialPaper (exclude deposits/leases)
            debt_current = get_val('DebtCurrent')
            debt_noncurrent = get_val('LongTermDebtNoncurrent')
            if debt_noncurrent is None:
                debt_noncurrent = get_val('DebtNoncurrent')
            if debt_noncurrent is None:
                debt_noncurrent = get_val('LongTermDebt')
            if provisional_bank_signal:
                bank_ltd = get_val('LongTermDebt') or debt_noncurrent
                bank_cp = get_val('CommercialPaper')
                bank_total = 0.0
                bank_has = False
                if bank_ltd is not None:
                    bank_total += float(bank_ltd)
                    bank_has = True
                if bank_cp is not None:
                    bank_total += float(bank_cp)
                    bank_has = True
                total_debt = bank_total if bank_has else None
            else:
                short_debt = debt_current if debt_current is not None else 0.0
                long_debt = debt_noncurrent if debt_noncurrent is not None else 0.0
                total_debt = (short_debt + long_debt) if (debt_current is not None or debt_noncurrent is not None) else None
            if total_debt is not None:
                total_debt = self._normalize_million_value(total_debt)
            cash = pick('cash')
            if cash is None:
                cash = pick('cash') or 0.0
            bank_like_for_interest = bool(provisional_bank_signal or prev_deposits is not None)
            interest_resolution = self._resolve_interest_expense_fact(
                row=data,
                year=year,
                is_bank=bank_like_for_interest,
            )
            interest_exp = self._safe_float((interest_resolution or {}).get('value'))
            interest_exp_source = (interest_resolution or {}).get('source') or 'MISSING_SEC_INTEREST_EXPENSE'
            interest_exp_reliability = int((interest_resolution or {}).get('reliability') or 0)
            interest_exp_tag = (interest_resolution or {}).get('tag')
            interest_exp_unit = (interest_resolution or {}).get('unit')
            interest_exp_filed = (interest_resolution or {}).get('filed')
            interest_exp_reason = (interest_resolution or {}).get('reason')
            allow_interest_carry_forward = str(os.environ.get('ALLOW_INTEREST_CARRY_FORWARD', '1')).strip().lower() in ('1', 'true', 'yes')
            if (
                interest_exp in (None, 0)
                and total_debt is not None
                and total_debt > 0
                and prev_interest_rate is not None
                and 0 < prev_interest_rate < 0.5
            ):
                interest_exp = abs(float(total_debt) * float(prev_interest_rate))
                interest_exp_source = 'SEC_HISTORY_INTEREST_RATE_PROXY'
                interest_exp_reliability = 70
                interest_exp_reason = None
            if (
                interest_exp in (None, 0)
                and allow_interest_carry_forward
                and prev_interest_expense is not None
                and prev_interest_expense > 0
            ):
                interest_exp = abs(prev_interest_expense)
                interest_exp_source = 'SEC_HISTORY_CARRY_FORWARD_PROXY'
                interest_exp_reliability = 60
                interest_exp_reason = None
            if net_interest_income is None and interest_income is not None and interest_exp:
                net_interest_income = float(interest_income) - float(interest_exp)
            pre_tax_income = pick(
                'IncomeBeforeTax',
                'IncomeBeforeTaxContinuingOperations',
                'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
            )
            if op is None and pre_tax_income is not None:
                op = pre_tax_income
            if ebitda is None and op is not None:
                ebitda = op + (dep or 0.0)
            ocf = pick('ocf')
            if ocf is None:
                for k, v in (data or {}).items():
                    lk = str(k).lower()
                    if 'cash' not in lk or 'operat' not in lk:
                        continue
                    if any(x in lk for x in ['invest', 'financ', 'supplement', 'noncash']):
                        continue
                    vv = get_val(k)
                    if vv is None:
                        continue
                    ocf = vv
                    break
            capex = None
            capex, capex_key, capex_source_kind = self._pick_canonical_label_value(data, 'CapEx')
            if capex is None:
                capex, capex_key = pick_exact_candidates([
                    'PaymentsToAcquirePropertyPlantAndEquipment',
                    'CapitalExpenditures',
                ])
                capex_source_kind = 'fallback' if capex is not None else 'missing'
            if capex is None:
                capex, capex_key = pick_semantic_one(
                    ['capital expend', 'capex', 'acquire property', 'plant and equipment'],
                    ['securit', 'investment', 'business', 'intangible']
                )
                if capex is not None:
                    capex_source_kind = 'fallback'
            if capex is None:
                capex = 0.0
                capex_key = None
                capex_source_kind = 'missing'
            
            # âœ… Shares Outstanding - comprehensive mapping
            shares_basic = pick('shares')
            if isinstance(shares_basic, (int, float)) and shares_basic <= 0:
                shares_basic = None
            shares_basic = self._normalize_shares_to_million(shares_basic)
            
            retained = get_val('RetainedEarnings') or get_val('RetainedEarningsAccumulatedDeficit') or 0.0
            if (equity is None or equity == 0.0) and assets is not None and total_liab is not None:
                # GAAP fallback identity: Equity = Assets - Liabilities
                equity = assets - total_liab

            # Align debt scale when debt facts are absolute dollars and assets/equity are in millions.
            if total_debt is not None:
                ref = assets if assets not in (None, 0) else equity
                if ref not in (None, 0):
                    td_candidates = [
                        total_debt,
                        total_debt / 1_000.0,
                        total_debt / 1_000_000.0,
                        total_debt / 1_000_000_000.0,
                    ]
                    plausible_td = [c for c in td_candidates if 0.02 <= (abs(c) / abs(ref)) <= 2.5]
                    if plausible_td:
                        total_debt = min(plausible_td, key=lambda c: abs((abs(c) / abs(ref)) - 0.35))
            if total_liab is not None and total_debt is not None and total_debt > total_liab:
                # Hard guardrail: debt must not exceed liabilities.
                td_candidates = [
                    total_debt,
                    total_debt / 1_000.0,
                    total_debt / 1_000_000.0,
                    total_debt / 1_000_000_000.0,
                ]
                plausible = [c for c in td_candidates if 0 <= c <= total_liab * 1.05]
                if plausible:
                    total_debt = max(plausible)
                else:
                    total_debt = None
            ratios['total_debt'] = total_debt if total_debt else None
            # Reconstruct cost of debt directly from SEC interest expense and debt average.
            try:
                debt_avg = None
                if total_debt is not None and prev_total_debt is not None:
                    debt_avg = (abs(total_debt) + abs(prev_total_debt)) / 2.0
                elif total_debt is not None:
                    debt_avg = abs(total_debt)

                bank_like = (provisional_bank_signal or prev_deposits is not None)
                insurance_like = provisional_insurance_signal

                # Bank-specific method (priority):
                # cost_of_debt = long-term-debt interest expense / long-term debt.
                bank_interest_ltd = pick(
                    'InterestExpenseLongTermDebt',
                    'InterestExpenseDebt',
                    'InterestAndDebtExpense',
                    'InterestExpenseBorrowings',
                )
                if bank_interest_ltd is None:
                    bank_interest_ltd, _ = pick_semantic_one(
                        ['interest', 'debt'],
                        ['deposit', 'deposits', 'income', 'revenue', 'fee'],
                    )
                bank_ltd = None
                for _dv in (
                    get_val('LongTermDebtNoncurrent'),
                    get_val('LongTermDebt'),
                    get_val('DebtNoncurrent'),
                    get_val('LongTermBorrowings'),
                ):
                    if _dv not in (None, 0):
                        bank_ltd = _dv
                        break
                if bank_like and bank_interest_ltd is not None and bank_ltd not in (None, 0):
                    cod_candidates = []
                    i_cands = [
                        abs(float(bank_interest_ltd)),
                        abs(float(bank_interest_ltd)) / 1_000.0,
                        abs(float(bank_interest_ltd)) / 1_000_000.0,
                        abs(float(bank_interest_ltd)) * 1_000.0,
                    ]
                    d_cands = [
                        abs(float(bank_ltd)),
                        abs(float(bank_ltd)) / 1_000.0,
                        abs(float(bank_ltd)) / 1_000_000.0,
                        abs(float(bank_ltd)) * 1_000.0,
                    ]
                    for ic in i_cands:
                        for dc in d_cands:
                            if dc in (None, 0):
                                continue
                            cv = ic / dc
                            if 0.001 <= cv <= 0.10:
                                cod_candidates.append(cv)
                    if cod_candidates:
                        ratios['cost_of_debt'] = min(cod_candidates, key=lambda x: abs(x - 0.04))
                        ratios['cost_of_debt_source'] = 'BANK_LT_DEBT_INTEREST'

                # Generic fallback when bank-specific LTD method is unavailable.
                dep_now = self._normalize_million_value(deposits) if deposits is not None else None
                dep_prev = self._normalize_million_value(prev_deposits) if prev_deposits is not None else None
                funding_avg = None
                dep_proxy = dep_now if dep_now is not None else dep_prev
                if (provisional_bank_signal or dep_prev is not None) and dep_proxy is not None:
                    if dep_now is not None and dep_prev is not None:
                        funding_avg = ((abs(dep_now) + abs(dep_prev)) / 2.0) + (debt_avg or 0.0)
                    else:
                        funding_avg = abs(dep_proxy) + (debt_avg or 0.0)

                cod_base = funding_avg if (funding_avg is not None and funding_avg > 0) else debt_avg
                # Banks: do not use deposits-based base in the generic fallback; use debt average only.
                if bank_like and debt_avg not in (None, 0):
                    cod_base = debt_avg

                if ratios.get('cost_of_debt') is None and interest_exp and cod_base and cod_base > 0:
                    if bank_like:
                        cod_min, cod_max, cod_target = 0.001, 0.10, 0.04
                    elif insurance_like:
                        cod_min, cod_max, cod_target = 0.001, 0.20, 0.04
                    else:
                        cod_min, cod_max, cod_target = 0.005, 0.25, 0.05
                    i_cands = [
                        abs(float(interest_exp)),
                        abs(float(interest_exp)) / 1_000.0,
                        abs(float(interest_exp)) / 1_000_000.0,
                        abs(float(interest_exp)) * 1_000.0,
                    ]
                    cod_candidates = []
                    for ic in i_cands:
                        if ic in (None, 0):
                            continue
                        cv = ic / float(cod_base)
                        if cod_min <= cv <= cod_max:
                            cod_candidates.append((ic, cv))
                    if cod_candidates:
                        _ic, cod = min(cod_candidates, key=lambda t: abs(t[1] - cod_target))
                        ratios['cost_of_debt'] = cod
                        ratios['cost_of_debt_source'] = 'SEC_INTEREST_OVER_DEBT_BASE'
            except Exception:
                pass

            def safe_div(n, d):
                try:
                    if d is None or d == 0:
                        return None
                    return n / d
                except:
                    return None

            # Liquidity
            if curr_liab is not None and curr_liab > 0:
                if curr_assets is not None and curr_assets > 0:
                    ratios['current_ratio'] = safe_div(curr_assets, curr_liab)
                quick_parts = []
                if cash is not None:
                    quick_parts.append(abs(float(cash)))
                if marketable_securities is not None:
                    quick_parts.append(abs(float(marketable_securities)))
                if ar is not None:
                    quick_parts.append(abs(float(ar)))
                quick_assets = sum(quick_parts) if quick_parts else None
                if (quick_assets in (None, 0.0)) and curr_assets is not None:
                    # Fallback: current assets minus less-liquid inventory.
                    quick_assets = abs(float(curr_assets)) - abs(float(inventory or 0.0))
                if quick_assets is not None and quick_assets >= 0:
                    ratios['quick_ratio'] = safe_div(quick_assets, curr_liab)
                if cash is not None:
                    ratios['cash_ratio'] = safe_div(abs(float(cash)), curr_liab)

            # Profitability
            if revenue and revenue != 0:
                if gross is not None:
                    ratios['gross_margin'] = scaled_ratio(gross, revenue, target=0.45, min_abs=0.0, max_abs=3.0)
                if op is not None:
                    ratios['operating_margin'] = scaled_ratio(op, revenue, target=0.20, min_abs=0.01, max_abs=3.0)
                if net is not None:
                    ratios['net_margin'] = scaled_ratio(net, revenue, target=0.12, min_abs=0.01, max_abs=3.0)
                if ebitda is not None:
                    ebitda_margin = scaled_ratio(ebitda, revenue, target=0.22, min_abs=0.0, max_abs=4.0)
                    # Accounting hierarchy guardrail:
                    # EBITDA margin must not exceed gross margin in standard corporate statements.
                    gm = ratios.get('gross_margin')
                    if ebitda_margin is not None and gm is not None and ebitda_margin > (gm + 0.02):
                        e_cands = [
                            float(ebitda),
                            float(ebitda) / 1_000.0,
                            float(ebitda) / 1_000_000.0,
                            float(ebitda) / 1_000_000_000.0,
                            float(ebitda) * 1_000.0,
                        ]
                        plausible = []
                        for ec in e_cands:
                            m = scaled_ratio(ec, revenue, target=0.25, min_abs=0.0, max_abs=2.0)
                            if m is None:
                                continue
                            if m <= (gm + 0.02):
                                plausible.append(m)
                        if plausible:
                            target = ratios.get('operating_margin')
                            if target is None:
                                target = min(0.30, max(0.10, gm * 0.6))
                            ebitda_margin = min(plausible, key=lambda x: abs(float(x) - float(target)))
                        else:
                            ebitda_margin = None
                            ratios['ebitda_margin_reason'] = 'MARGIN_HIERARCHY_VIOLATION'
                    ratios['ebitda_margin'] = ebitda_margin

            if assets and assets != 0 and net is not None:
                avg_assets = assets
                if prev_assets is not None and assets is not None:
                    avg_assets = (abs(float(prev_assets)) + abs(float(assets))) / 2.0
                roa_target = 0.01 if provisional_bank_signal else 0.08
                at_target = 0.06 if provisional_bank_signal else 0.90
                roa_min_abs = 0.001 if provisional_bank_signal else 0.01
                # Strict ROA: Net Income / Average Assets (not ending-assets-only).
                ratios['roa'] = scaled_ratio(net, avg_assets, target=roa_target, min_abs=roa_min_abs, max_abs=2.0)
                if revenue is not None:
                    at_min_abs = 0.01 if provisional_bank_signal else 0.05
                    ratios['asset_turnover'] = scaled_ratio(revenue, assets, target=at_target, min_abs=at_min_abs, max_abs=10.0)

            if equity and equity != 0 and net is not None:
                roe_target = 0.12 if provisional_bank_signal else 0.20
                roe_min_abs = 0.01 if provisional_bank_signal else 0.02
                ratios['roe'] = scaled_ratio(net, equity, target=roe_target, min_abs=roe_min_abs, max_abs=5.0)
            if equity and equity != 0 and total_debt is not None:
                de_target = 1.8 if provisional_bank_signal else 0.8
                ratios['debt_to_equity'] = scaled_ratio(total_debt, equity, target=de_target, min_abs=0.0, max_abs=20.0)

            # âœ… ROIC calculation (CRITICAL FIX)
            # ROIC = NOPAT / Invested Capital
            # NOPAT = Operating Income Ã— (1 - Tax Rate)
            # Invested Capital = Total Assets - Current Liabilities (or Debt + Equity)
            try:
                tax_rate = 0.21  # default corporate tax rate
                if op is not None and op != 0:
                    nopat = op * (1 - tax_rate)
                    # Invested Capital = Total Assets - Non-interest bearing current liabilities
                    # Simplified: Total Assets - Current Liabilities
                    if assets and curr_liab is not None:
                        invested_capital = assets - curr_liab
                    elif equity and (short_debt or long_debt):
                        # Alternative: Debt + Equity
                        invested_capital = equity + (short_debt or 0.0) + (long_debt or 0.0)
                    else:
                        invested_capital = assets
                    
                    if invested_capital and invested_capital != 0:
                        ratios['roic'] = (nopat / invested_capital)
                    else:
                        ratios['roic'] = None
                else:
                    ratios['roic'] = None
            except:
                ratios['roic'] = None

            net_for_per_share = net
            equity_for_per_share = equity
            if revenue not in (None, 0) and net_for_per_share is not None:
                net_for_per_share = align_to_reference(
                    net_for_per_share,
                    revenue,
                    target=0.08,
                    min_ratio=-1.0,
                    max_ratio=1.0,
                    min_abs_ratio=0.005,
                    preserve_if_plausible=False,
                )
            if assets not in (None, 0) and equity_for_per_share is not None:
                equity_for_per_share = align_to_reference(
                    equity_for_per_share,
                    assets,
                    target=0.20,
                    min_ratio=-1.0,
                    max_ratio=1.5,
                    min_abs_ratio=0.01,
                    preserve_if_plausible=False,
                )

            if shares_basic and shares_basic != 0 and net_for_per_share is not None:
                # Scale-aware EPS to avoid persistent x1000 errors (millions vs shares).
                ratios['eps_basic'] = self._select_per_share_scaled_value(
                    numerator=net_for_per_share,
                    shares=shares_basic,
                )

            # Bank/insurance-sector compatible ratios (only when sectoral signals exist).
            try:
                avg_assets = assets
                if prev_assets is not None and assets is not None:
                    prev_assets_aligned = align_to_reference(
                        prev_assets,
                        assets,
                        target=1.0,
                        min_ratio=0.10,
                        max_ratio=10.0,
                        min_abs_ratio=0.05,
                    )
                    avg_assets = (abs(prev_assets_aligned) + abs(assets)) / 2.0

                bank_signal = any(v is not None for v in (net_interest_income, noninterest_income, loans, deposits, cet1))
                insurance_signal = any(
                    self._safe_float(data.get(k)) is not None
                    for k in (
                        'PremiumsEarned',
                        'PolicyholderBenefits',
                        'PolicyClaims',
                        'ReinsuranceRecoverables',
                    )
                )

                if avg_assets not in (None, 0) and revenue not in (None, 0):
                    avg_assets = align_to_reference(
                        avg_assets,
                        revenue,
                        target=18.0 if bank_signal else (6.0 if insurance_signal else 2.0),
                        min_ratio=0.10,
                        max_ratio=400.0,
                        min_abs_ratio=0.05,
                    )
                if bank_signal and net_interest_income is None and net is not None and avg_assets not in (None, 0):
                    # Conservative NIM proxy from net income when interest line items are absent.
                    nim_proxy = scaled_ratio(
                        net,
                        avg_assets,
                        target=0.015,
                        min_abs=0.0,
                        max_abs=0.25,
                    )
                    if nim_proxy is not None:
                        net_interest_income = float(nim_proxy) * float(avg_assets)
                        ratios['net_interest_income_proxy_used'] = 'NET_INCOME_OVER_AVG_ASSETS_PROXY'
                if bank_signal and net_interest_income is not None and avg_assets not in (None, 0):
                    net_interest_income = align_to_reference(
                        net_interest_income,
                        avg_assets,
                        target=0.03,
                        min_ratio=0.001,
                        max_ratio=0.25,
                        min_abs_ratio=0.0005,
                    )

                if bank_signal:
                    bank_total_revenue = None
                    bank_revenue_candidates = [
                        revenue,
                        get_val('Revenues'),
                        get_val('SalesRevenueNet'),
                        get_val('RevenueFromContractWithCustomerExcludingAssessedTax'),
                    ]
                    # Banks often report income statement lines differently; interest income is a
                    # useful fallback anchor when total revenue concepts are missing/misaligned.
                    if interest_income is not None:
                        bank_revenue_candidates.append(interest_income)
                    if net_interest_income is not None:
                        bank_revenue_candidates.append(net_interest_income)
                    if net_interest_income is not None and noninterest_income is not None:
                        bank_revenue_candidates.append(float(net_interest_income) + float(noninterest_income))
                    if net is not None:
                        bank_total_revenue = pick_best_scaled_denominator(
                            numerator=net,
                            raw_candidates=bank_revenue_candidates,
                            # Bank net income margins are often higher than operating companies
                            # when "Revenues" is actually net revenue; target a realistic band.
                            target_ratio=0.35,
                            min_ratio=0.05,
                            max_ratio=0.80,
                        )
                    if bank_total_revenue is not None:
                        ratios['bank_total_revenue'] = bank_total_revenue
                        ratios['net_margin'] = scaled_ratio(
                            net,
                            bank_total_revenue,
                            target=0.22,
                            min_abs=0.005,
                            max_abs=1.5,
                        )

                    if net_interest_income is not None and avg_assets not in (None, 0):
                        nim_val = scaled_ratio(
                            net_interest_income,
                            avg_assets,
                            target=0.03,
                            min_abs=0.0,
                            max_abs=0.5,
                        )
                        # If NIM is implausibly tiny, retry from alternative interest tags.
                        if nim_val is not None and abs(nim_val) < 0.001:
                            alt_interest_income = pick('InterestAndFeeIncomeLoansAndLeases') or interest_income
                            alt_interest_exp = pick('InterestExpenseDeposits') or interest_exp
                            if alt_interest_income is not None and alt_interest_exp is not None:
                                retry_nim = scaled_ratio(
                                    float(alt_interest_income) - float(abs(alt_interest_exp)),
                                    avg_assets,
                                    target=0.03,
                                    min_abs=0.0,
                                    max_abs=0.5,
                                )
                                if retry_nim is not None:
                                    nim_val = retry_nim
                        ratios['net_interest_margin'] = nim_val
                    # Bank efficiency ratio must be economically plausible.
                    # Strategy:
                    # 1) Prefer SEC noninterest expense / total revenue when plausible.
                    # 2) If missing OR outlier (tag drift / unit mismatch), fall back to a conservative proxy:
                    #    (total revenue - net income) / total revenue.
                    if bank_total_revenue not in (None, 0):
                        eff_val = None
                        eff_source = None
                        if noninterest_expense is not None:
                            try:
                                eff_raw = abs(float(noninterest_expense)) / max(abs(float(bank_total_revenue)), 1e-12)
                            except Exception:
                                eff_raw = None
                            # Typical large-bank efficiency ratios are ~0.4–0.8; values <0.3 or >0.9
                            # are almost always a mapping/unit problem in SEC facts.
                            if eff_raw is not None and (0.30 <= eff_raw <= 0.90):
                                eff_val = scaled_ratio(
                                    abs(noninterest_expense),
                                    abs(bank_total_revenue),
                                    target=0.60,
                                    min_abs=0.05,
                                    max_abs=2.0,
                                )
                                eff_source = 'NONINTEREST_EXPENSE_OVER_TOTAL_REVENUE'
                            else:
                                eff_source = 'EFF_PROXY_OUTLIER_NONINTEREST_EXPENSE'

                        if eff_val is None and net is not None:
                            # Conservative proxy when noninterest expense is missing/outlier.
                            # Efficiency proxy ~= (total revenue - net income) / total revenue.
                            try:
                                exp_proxy = abs(float(bank_total_revenue) - float(net))
                            except Exception:
                                exp_proxy = None
                            if exp_proxy is not None:
                                eff_proxy = scaled_ratio(
                                    exp_proxy,
                                    abs(bank_total_revenue),
                                    target=0.60,
                                    min_abs=0.05,
                                    max_abs=2.0,
                                )
                                if eff_proxy is not None:
                                    eff_val = eff_proxy
                                    # Preserve outlier marker if we detected it; otherwise mark as proxy.
                                    eff_source = eff_source or 'ONE_MINUS_NET_MARGIN_PROXY'

                        if eff_val is not None:
                            ratios['bank_efficiency_ratio'] = eff_val
                            ratios['bank_efficiency_ratio_source'] = eff_source or 'BANK_EFFICIENCY_RATIO'
                        elif eff_source:
                            ratios['bank_efficiency_ratio'] = None
                            ratios['bank_efficiency_ratio_source'] = eff_source
                    if loans is not None and deposits not in (None, 0):
                        ratios['loan_to_deposit_ratio'] = safe_div(loans, deposits)
                    elif deposits not in (None, 0) and total_debt not in (None, 0):
                        ldr_proxy = scaled_ratio(
                            total_debt,
                            deposits,
                            target=0.70,
                            min_abs=0.0,
                            max_abs=3.0,
                        )
                        if ldr_proxy is not None:
                            ratios['loan_to_deposit_ratio'] = ldr_proxy
                            ratios['loan_to_deposit_proxy_used'] = True
                    elif deposits not in (None, 0) and assets not in (None, 0):
                        cash_base = abs(cash) if cash is not None else 0.0
                        loans_asset_proxy = max(abs(assets) - cash_base, 0.0)
                        ldr_proxy_assets = scaled_ratio(
                            loans_asset_proxy,
                            deposits,
                            target=0.85,
                            min_abs=0.0,
                            max_abs=3.0,
                        )
                        if ldr_proxy_assets is not None:
                            ratios['loan_to_deposit_ratio'] = ldr_proxy_assets
                            ratios['loan_to_deposit_proxy_used'] = 'ASSETS_MINUS_CASH_OVER_DEPOSITS_PROXY'
                    ldr_val = self._safe_float(ratios.get('loan_to_deposit_ratio'))
                    # Hard realism gate: block LDR when it is outside plausible economic range.
                    # This prevents proxies (or missing tags) from producing misleading risk signals.
                    if ldr_val is not None:
                        try:
                            ldr_abs = abs(float(ldr_val))
                        except Exception:
                            ldr_abs = None
                        if ldr_abs is not None and (ldr_abs < 0.20 or ldr_abs > 1.60):
                            ratios['loan_to_deposit_ratio'] = None
                            ratios['loan_to_deposit_source'] = 'LDR_OUT_OF_RANGE_UNTRUSTED'
                            reasons_map = ratios.get('_ratio_reasons')
                            if not isinstance(reasons_map, dict):
                                reasons_map = {}
                                ratios['_ratio_reasons'] = reasons_map
                            reasons_map['loan_to_deposit_ratio'] = 'BANK_ANCHOR_INCONSISTENT'
                            ldr_val = None
                    raw_dep_missing = (
                        data.get('Deposits') in (None, 0)
                        and data.get('DepositLiabilities') in (None, 0)
                    )
                    if (
                        ldr_val is not None
                        and abs(float(ldr_val)) > 5.0
                        and raw_dep_missing
                        and prev_deposits not in (None, 0)
                    ):
                        if prev_loans not in (None, 0):
                            ratios['loan_to_deposit_ratio'] = safe_div(float(prev_loans), float(prev_deposits))
                            ratios['loan_to_deposit_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY_BANK_ANOMALOUS_LDR'
                        else:
                            ratios['loan_to_deposit_ratio'] = None
                            ratios['loan_to_deposit_source'] = 'ANOMALOUS_LDR_REJECTED_MISSING_DEPOSIT_ANCHOR'
                    if cet1 is not None:
                        # If CET1 is already a ratio (<=1), keep it.
                        if abs(cet1) <= 1.0:
                            ratios['capital_ratio_proxy'] = cet1
                        elif avg_assets not in (None, 0):
                            ratios['capital_ratio_proxy'] = scaled_ratio(
                                cet1,
                                avg_assets,
                                target=0.10,
                                min_abs=0.0,
                                max_abs=1.0,
                            )
                    elif equity is not None and avg_assets not in (None, 0):
                        ratios['capital_ratio_proxy'] = scaled_ratio(
                            equity,
                            avg_assets,
                            target=0.10,
                            min_abs=0.0,
                            max_abs=1.0,
                        )
                    if net is not None and avg_assets not in (None, 0):
                        ratios['net_income_to_assets'] = scaled_ratio(
                            net,
                            avg_assets,
                            target=0.01,
                            min_abs=0.0,
                            max_abs=0.2,
                        )
                    if equity is not None and avg_assets not in (None, 0):
                        ratios['equity_ratio'] = scaled_ratio(
                            equity,
                            avg_assets,
                            target=0.10,
                            min_abs=0.0,
                            max_abs=1.0,
                        )

                if insurance_signal:
                    if equity is not None and total_liab not in (None, 0):
                        ratios['capital_adequacy_proxy'] = scaled_ratio(
                            equity,
                            total_liab,
                            target=0.30,
                            min_abs=0.0,
                            max_abs=3.0,
                        )
                    if premiums_earned not in (None, 0) and policy_claims is not None:
                        ratios['combined_proxy'] = scaled_ratio(
                            abs(policy_claims),
                            abs(premiums_earned),
                            target=0.95,
                            min_abs=0.0,
                            max_abs=5.0,
                        )
                    elif policy_claims is not None and revenue not in (None, 0):
                        comb_rev_proxy = scaled_ratio(
                            abs(policy_claims),
                            abs(revenue),
                            target=0.95,
                            min_abs=0.0,
                            max_abs=5.0,
                        )
                        if comb_rev_proxy is not None:
                            ratios['combined_proxy'] = comb_rev_proxy
                            ratios['combined_proxy_source'] = 'POLICY_CLAIMS_OVER_REVENUE_PROXY'
                    elif net is not None and revenue not in (None, 0):
                        # Last-resort broad expense burden proxy for insurance-like models.
                        # combined_proxy ~= 1 - net_margin (not pure underwriting combined ratio).
                        try:
                            nm = float(net) / float(revenue)
                            nm = max(-2.0, min(2.0, nm))
                            comb_nm = 1.0 - nm
                            if comb_nm == comb_nm:
                                ratios['combined_proxy'] = max(0.0, min(5.0, comb_nm))
                                ratios['combined_proxy_source'] = 'ONE_MINUS_NET_MARGIN_PROXY'
                        except Exception:
                            pass
                    else:
                        ratios['combined_proxy'] = None
                    if net is not None and avg_assets not in (None, 0):
                        ratios['net_income_to_assets'] = scaled_ratio(
                            net,
                            avg_assets,
                            target=0.01,
                            min_abs=0.0,
                            max_abs=0.2,
                        )
                    if equity is not None and avg_assets not in (None, 0):
                        ratios['equity_ratio'] = scaled_ratio(
                            equity,
                            avg_assets,
                            target=0.10,
                            min_abs=0.0,
                            max_abs=1.0,
                        )
            except Exception:
                pass

            # Institutional hardening fallback: force-fill critical bank/insurance ratios
            # when direct tags are missing but core accounting anchors exist.
            try:
                avg_assets_fallback = assets
                if prev_assets is not None and assets is not None:
                    avg_assets_fallback = (abs(float(prev_assets)) + abs(float(assets))) / 2.0
                bank_like = bool((provisional_bank_signal or prev_deposits is not None))
                insurance_like = bool(provisional_insurance_signal)

                if ratios.get('roa') is None and net is not None and avg_assets_fallback not in (None, 0):
                    ratios['roa'] = safe_div(float(net), float(avg_assets_fallback))
                    ratios['roa_source'] = 'FORCED_FALLBACK_NET_OVER_AVG_ASSETS'

                if ratios.get('roe') is None and net is not None and equity not in (None, 0):
                    ratios['roe'] = safe_div(float(net), float(equity))
                    ratios['roe_source'] = 'FORCED_FALLBACK_NET_OVER_EQUITY'

                if bank_like:
                    if ratios.get('net_interest_margin') is None and net is not None and avg_assets_fallback not in (None, 0):
                        ratios['net_interest_margin'] = safe_div(float(net), float(avg_assets_fallback))
                        ratios['net_interest_margin_source'] = 'FORCED_FALLBACK_NET_OVER_AVG_ASSETS'

                    if ratios.get('loan_to_deposit_ratio') is None:
                        dep = deposits
                        loan_base = loans
                        missing_bank_anchors = (
                            deposits in (None, 0)
                            and loans is None
                            and assets is None
                            and total_liab is None
                        )
                        if missing_bank_anchors and prev_deposits not in (None, 0) and prev_loans is not None:
                            ratios['loan_to_deposit_ratio'] = safe_div(float(prev_loans), float(prev_deposits))
                            ratios['loan_to_deposit_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY_BANK_MISSING_ANCHORS'
                        else:
                            if dep in (None, 0) and total_liab not in (None, 0):
                                dep = abs(float(total_liab)) * 0.80
                            if loan_base is None and assets is not None:
                                cash_base = abs(float(cash)) if cash is not None else 0.0
                                loan_base = max(abs(float(assets)) - cash_base, 0.0)
                            if loan_base is not None and dep not in (None, 0):
                                ratios['loan_to_deposit_ratio'] = safe_div(float(loan_base), float(dep))
                                ratios['loan_to_deposit_source'] = 'FORCED_FALLBACK_ASSET_PROXY'

                    if ratios.get('capital_ratio_proxy') is None:
                        cap_proxy = None
                        if cet1 is not None:
                            if abs(float(cet1)) <= 1.0:
                                cap_proxy = float(cet1)
                            elif avg_assets_fallback not in (None, 0):
                                cap_proxy = safe_div(float(cet1), float(avg_assets_fallback))
                        if cap_proxy is None:
                            denom = avg_assets_fallback if avg_assets_fallback not in (None, 0) else assets
                            if denom in (None, 0) and equity is not None and total_liab is not None:
                                denom = abs(float(equity)) + abs(float(total_liab))
                            if equity not in (None, 0) and denom not in (None, 0):
                                cap_proxy = safe_div(float(equity), float(denom))
                        if cap_proxy is not None:
                            ratios['capital_ratio_proxy'] = cap_proxy
                            ratios['capital_ratio_proxy_source'] = 'FORCED_FALLBACK_CAPITAL_PROXY'

                if insurance_like and ratios.get('capital_ratio_proxy') is None:
                    cap_proxy = ratios.get('capital_adequacy_proxy')
                    if cap_proxy is None:
                        cap_proxy = ratios.get('equity_ratio')
                    if cap_proxy is None and equity not in (None, 0) and assets not in (None, 0):
                        cap_proxy = safe_div(float(equity), float(assets))
                    if cap_proxy is not None:
                        ratios['capital_ratio_proxy'] = cap_proxy
                        ratios['capital_ratio_proxy_source'] = 'FORCED_FALLBACK_INSURANCE_PROXY'
            except Exception:
                pass

            # Activity / Efficiency
            cogs_abs = abs(cogs) if cogs is not None else None
            inventory_base = abs(inventory) if inventory is not None else None
            if inventory_base is not None and prev_inventory is not None:
                inventory_base = (inventory_base + abs(prev_inventory)) / 2.0
            if inventory_base and cogs_abs:
                try:
                    ratios['inventory_turnover'] = cogs_abs / inventory_base
                except:
                    ratios['inventory_turnover'] = None
            
            ar_value = abs(ar) if ar is not None else None
            if ar_value is not None and prev_ar is not None:
                ar_value = (ar_value + abs(prev_ar)) / 2.0
            if ar_value is not None and revenue and revenue != 0:
                ratios['days_sales_outstanding'] = (ar_value / abs(revenue)) * 365.0
                ratios['ar_days_reliability'] = 1.0
                ratios['ar_days_inputs_aggregated'] = True
            else:
                ratios['days_sales_outstanding'] = None
                ratios['ar_days_reliability'] = 0.0
                ratios['ar_days_inputs_aggregated'] = False
                
            ap_value = abs(ap) if ap is not None else None
            if ap_value is not None and prev_ap is not None:
                ap_value = (ap_value + abs(prev_ap)) / 2.0
            if ap_value is not None and cogs_abs:
                try:
                    ratios['payables_turnover'] = cogs_abs / ap_value
                except:
                    ratios['payables_turnover'] = None

            # Leverage & coverage
            coverage_numerator = op
            if coverage_numerator is None and pre_tax_income is not None and interest_exp and interest_exp > 0:
                coverage_numerator = pre_tax_income + interest_exp
            if coverage_numerator is None and net is not None and interest_exp and interest_exp > 0:
                coverage_numerator = net + interest_exp
            if (provisional_bank_signal or prev_deposits is not None):
                ratios['interest_coverage'] = None
                ratios['interest_coverage_reason'] = 'DATA_NOT_APPLICABLE_BANK_USE_NIM'
            elif interest_exp and interest_exp > 0 and coverage_numerator is not None:
                ratios['interest_coverage'] = scaled_ratio(
                    coverage_numerator,
                    interest_exp,
                    target=12.0,
                    min_abs=0.01,
                    max_abs=500.0,
                )
            # Non-bank fallback when operating/pre-tax anchors are absent but net income exists.
            if (
                ratios.get('interest_coverage') is None
                and not (provisional_bank_signal or prev_deposits is not None)
                and interest_exp
                and interest_exp > 0
            ):
                fallback_net = net
                if fallback_net is None:
                    fallback_net = (
                        get_val('NetIncomeLoss')
                        or get_val('NetIncome')
                        or get_val('ProfitLoss')
                    )
                if fallback_net is not None:
                    ic_fallback = scaled_ratio(
                        float(fallback_net) + abs(float(interest_exp)),
                        abs(float(interest_exp)),
                        target=12.0,
                        min_abs=0.01,
                        max_abs=500.0,
                    )
                    if ic_fallback is not None:
                        ratios['interest_coverage'] = ic_fallback
                        ratios['interest_coverage_source'] = 'NET_PLUS_INTEREST_FALLBACK'
            if (
                ratios.get('interest_coverage') is None
                and not (provisional_bank_signal or prev_deposits is not None)
                and (interest_exp in (None, 0))
            ):
                ratios['interest_coverage_reason'] = 'INTEREST_EXPENSE_NOT_FOUND'
            ratios['interest_expense_used'] = interest_exp if interest_exp else None
            ratios['interest_expense_source'] = interest_exp_source
            ratios['interest_expense_reliability'] = interest_exp_reliability
            ratios['interest_expense_tag'] = interest_exp_tag
            ratios['interest_expense_unit'] = interest_exp_unit
            ratios['interest_expense_filed'] = interest_exp_filed
            ratios['interest_expense_reason'] = interest_exp_reason
            ratios['interest_expense_is_estimated'] = bool(
                interest_exp_source in {
                    'SEC_HISTORY_CARRY_FORWARD_PROXY',
                    'SEC_CASH_PAID_FOR_INTEREST_PROXY',
                    'SEC_HISTORY_INTEREST_RATE_PROXY',
                }
            )
            if assets and assets != 0 and total_debt is not None:
                da_target = 0.40 if provisional_bank_signal else 0.35
                ratios['debt_to_assets'] = scaled_ratio(total_debt, assets, target=da_target, min_abs=0.0, max_abs=3.0)
            ratios['total_debt'] = total_debt if total_debt else None

            # Cashflow / FCF
            if revenue and revenue != 0 and ocf is not None:
                ratios['ocf_margin'] = scaled_ratio(ocf, revenue, target=0.20, min_abs=0.005, max_abs=3.0)
                ratios['operating_cash_flow_margin'] = ratios['ocf_margin']
            ratios['operating_cash_flow'] = ocf
            if ocf is not None:
                capex_abs = abs(capex or 0.0)
                # Align CapEx scale with OCF when one is absolute and the other is in millions.
                if capex_abs > 0 and abs(ocf) > 0:
                    candidates = [
                        capex_abs,
                        capex_abs / 1_000.0,
                        capex_abs / 1_000_000.0,
                        capex_abs / 1_000_000_000.0,
                    ]
                    plausible = [c for c in candidates if 0.005 <= (c / abs(ocf)) <= 1.5]
                    if plausible:
                        capex_abs = min(plausible, key=lambda c: abs((c / abs(ocf)) - 0.12))
                fcf = ocf - capex_abs
                if ocf > 0 and fcf < 0:
                    # If capex still dominates OCF, try safer scale alignment.
                    cands = [
                        abs(capex or 0.0),
                        abs(capex or 0.0) / 1_000.0,
                        abs(capex or 0.0) / 1_000_000.0,
                        abs(capex or 0.0) / 1_000_000_000.0,
                    ]
                    plausible = [c for c in cands if 0.001 <= (c / abs(ocf)) <= 1.2]
                    if plausible:
                        capex_abs = min(plausible, key=lambda c: abs((c / abs(ocf)) - 0.10))
                        fcf = ocf - capex_abs
                ratios['free_cash_flow'] = fcf

            # Net Debt / EBITDA
            try:
                debt_for_nd = total_debt if total_debt is not None else ((short_debt or 0.0) + (long_debt or 0.0))
                net_debt = (debt_for_nd or 0.0) - (cash or 0.0)
                if ebitda is not None and ebitda != 0:
                    # Scale guard: align EBITDA magnitude to debt/cash scale if needed.
                    if abs(net_debt) > 0:
                        e_cands = [ebitda, ebitda / 1_000.0, ebitda / 1_000_000.0, ebitda * 1_000.0, ebitda * 1_000_000.0]
                        plausible = [c for c in e_cands if c not in (None, 0) and 0.01 <= (abs(c) / max(abs(net_debt), 1e-9)) <= 20.0]
                        if plausible:
                            ebitda = min(plausible, key=lambda c: abs((abs(c) / max(abs(net_debt), 1e-9)) - 0.35))
                    ratios['net_debt_ebitda'] = safe_div(net_debt, ebitda)
                    if ratios['net_debt_ebitda'] is not None and abs(ratios['net_debt_ebitda']) > 20:
                        ratios['net_debt_ebitda'] = None
                else:
                    ratios['net_debt_ebitda'] = None
            except:
                ratios['net_debt_ebitda'] = None

            # If a year has no usable SEC anchors, carry-forward core ratios from the
            # most recent anchored year (explicitly tagged as proxy).
            no_anchor_year = all(
                self._safe_float(v) is None
                for v in (
                    revenue,
                    net,
                    assets,
                    equity,
                    ocf,
                    op,
                    gross,
                    curr_assets,
                    curr_liab,
                )
            )
            if no_anchor_year and prev_carry_ratio_values and allow_ratio_carry_forward:
                for _k in carry_ratio_keys:
                    if ratios.get(_k) is None and prev_carry_ratio_values.get(_k) is not None:
                        ratios[_k] = prev_carry_ratio_values.get(_k)
                        ratios[f'{_k}_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY_NO_ANCHOR'
                ratios['full_year_proxy_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY_NO_ANCHOR'

            # Revenue-missing annual rows (partial anchor availability): keep continuity
            # for revenue-dependent ratios from the most recent anchored year.
            if (
                revenue is None
                and not provisional_bank_signal
                and not provisional_insurance_signal
                and allow_ratio_carry_forward
            ):
                if prev_carry_ratio_values:
                    for _k in (
                        'gross_margin',
                        'operating_margin',
                        'net_margin',
                        'ebitda_margin',
                        'asset_turnover',
                        'ocf_margin',
                        'inventory_turnover',
                    ):
                        if ratios.get(_k) is None and prev_carry_ratio_values.get(_k) is not None:
                            ratios[_k] = prev_carry_ratio_values.get(_k)
                            ratios[f'{_k}_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY_MISSING_REVENUE'
                else:
                    missing_leading_anchor = any(
                        ratios.get(_k) is None
                        for _k in ('gross_margin', 'operating_margin', 'net_margin')
                    )
                    if missing_leading_anchor:
                        leading_missing_revenue_years.append(year)

            # If COGS is not presented separately but operating margin exists,
            # expose gross margin proxy to avoid false gaps in utility-like filings.
            if (
                ratios.get('gross_margin') is None
                and ratios.get('operating_margin') is not None
                and cogs is None
                and not provisional_bank_signal
                and not provisional_insurance_signal
            ):
                ratios['gross_margin'] = ratios.get('operating_margin')
                ratios['gross_margin_source'] = 'OPERATING_MARGIN_PROXY_NO_COGS'

            # Inventory-light models: treat inventory turnover as zero rather than N/A.
            if (
                ratios.get('inventory_turnover') is None
                and (inventory in (None, 0) or cogs_abs in (None, 0))
                and not provisional_bank_signal
                and not provisional_insurance_signal
            ):
                ratios['inventory_turnover'] = 0.0
                ratios['inventory_turnover_source'] = 'NO_INVENTORY_ZERO_PROXY'

            # Inventory/AR/AP days
            try:
                ratios['inventory_days'] = (365.0 / ratios.get('inventory_turnover')) if ratios.get('inventory_turnover') and ratios.get('inventory_turnover') > 0 else None
            except:
                ratios['inventory_days'] = None
            try:
                ratios['ap_days'] = (365.0 / ratios.get('payables_turnover')) if ratios.get('payables_turnover') and ratios.get('payables_turnover') > 0 else None
            except:
                ratios['ap_days'] = None

            # Fill sparse-year gaps with explicit, auditable fallbacks.
            # 1) net_income_to_assets ~= roa (same economic meaning when avg-assets source differs).
            if ratios.get('net_income_to_assets') is None:
                if ratios.get('roa') is not None:
                    ratios['net_income_to_assets'] = ratios.get('roa')
                    ratios['net_income_to_assets_source'] = 'ROA_PROXY_SAME_YEAR'
                elif prev_net_income_to_assets is not None:
                    ratios['net_income_to_assets'] = prev_net_income_to_assets
                    ratios['net_income_to_assets_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY'

            # 2) equity_ratio fallback hierarchy:
            #    a) roa/roe proxy, b) equity/(equity+liabilities) balance proxy, c) carry-forward.
            if ratios.get('equity_ratio') is None:
                _roa = self._safe_float(ratios.get('roa'))
                _roe = self._safe_float(ratios.get('roe'))
                eq_proxy = None
                if _roa is not None and _roe not in (None, 0):
                    try:
                        eq_proxy = _roa / _roe
                    except Exception:
                        eq_proxy = None
                    if eq_proxy is not None and (eq_proxy != eq_proxy or abs(eq_proxy) > 1.5):
                        eq_proxy = None
                    if eq_proxy is not None:
                        ratios['equity_ratio'] = eq_proxy
                        ratios['equity_ratio_source'] = 'ROA_ROE_PROXY'
                if ratios.get('equity_ratio') is None and equity is not None and total_liab is not None:
                    try:
                        bal_base = abs(float(equity)) + abs(float(total_liab))
                        if bal_base > 0:
                            ratios['equity_ratio'] = abs(float(equity)) / bal_base
                            ratios['equity_ratio_source'] = 'BALANCE_PROXY_EQUITY_OVER_E_PLUS_L'
                    except Exception:
                        pass
                if ratios.get('equity_ratio') is None and prev_equity_ratio is not None:
                    ratios['equity_ratio'] = prev_equity_ratio
                    ratios['equity_ratio_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY'

            # 3) inventory_turnover can be absent in latest-year snapshots even when historic
            #    inventory exists; keep continuity for non-bank/non-insurance sectors only.
            if (
                ratios.get('inventory_turnover') is None
                and not provisional_bank_signal
                and not provisional_insurance_signal
                and prev_inventory_turnover is not None
            ):
                ratios['inventory_turnover'] = prev_inventory_turnover
                ratios['inventory_turnover_source'] = 'SEC_HISTORY_CARRY_FORWARD_PROXY'
                if ratios.get('inventory_days') is None and prev_inventory_turnover not in (None, 0):
                    try:
                        ratios['inventory_days'] = 365.0 / float(prev_inventory_turnover)
                    except Exception:
                        pass

            if inventory is not None:
                prev_inventory = inventory
            if assets is not None:
                prev_assets = assets
            if total_liab is not None:
                prev_total_liab = total_liab
            if equity is not None:
                prev_equity = equity
            if ar is not None:
                prev_ar = ar
            if ap is not None:
                prev_ap = ap
            if loans is not None:
                prev_loans = loans
            if net_interest_income is not None:
                prev_net_interest_income = net_interest_income
            if premiums_earned is not None:
                prev_premiums_earned = premiums_earned
            if policy_claims is not None:
                prev_policy_claims = policy_claims
            if interest_exp and interest_exp > 0:
                prev_interest_expense = interest_exp
                if total_debt is not None and total_debt > 0:
                    try:
                        prev_interest_rate = abs(float(interest_exp)) / float(total_debt)
                    except Exception:
                        prev_interest_rate = prev_interest_rate
            if total_debt is not None:
                prev_total_debt = total_debt
            if deposits is not None:
                prev_deposits = deposits
            if shares_basic is not None and shares_basic > 0:
                prev_shares_outstanding = shares_basic
            if ratios.get('inventory_turnover') is not None:
                prev_inventory_turnover = ratios.get('inventory_turnover')
            if ratios.get('net_income_to_assets') is not None:
                prev_net_income_to_assets = ratios.get('net_income_to_assets')
            if ratios.get('equity_ratio') is not None:
                prev_equity_ratio = ratios.get('equity_ratio')
            for _k in carry_ratio_keys:
                if ratios.get(_k) is not None:
                    prev_carry_ratio_values[_k] = ratios.get(_k)

            # Accruals ratio
            try:
                if assets and net is not None and ocf is not None:
                    ratios['accruals_ratio'] = (net - ocf) / assets
                else:
                    ratios['accruals_ratio'] = None
            except:
                ratios['accruals_ratio'] = None

            # Altman Z (fallback without market cap; market-cap version injected later)
            try:
                A = ((curr_assets - curr_liab) / assets) if assets and curr_assets is not None and curr_liab is not None else None
                B = (retained / assets) if assets else None
                C = (op / assets) if assets and op is not None else None
                D = (equity / total_liab) if equity is not None and total_liab and total_liab != 0 else 0.0
                E = (revenue / assets) if assets and revenue is not None else None
                if None not in (A, B, C, E):
                    Z = 1.2 * A + 1.4 * B + 3.3 * C + 0.6 * (D or 0.0) + 1.0 * E
                    ratios['altman_z_score'] = Z
                else:
                    ratios['altman_z_score'] = None
            except:
                ratios['altman_z_score'] = None

            # âœ… Point 2: Retention ratio & SGR_Internal (enhanced with dynamic dividend mapping)
            try:
                dividends = pick('dividends')
                if net is not None and net != 0 and dividends is not None:
                    # Ensure dividends is positive for calculation
                    dividends_abs = abs(dividends)
                    retention_ratio = 1.0 - (dividends_abs / abs(net))
                    retention_ratio = max(0.0, min(1.0, retention_ratio))
                    ratios['retention_ratio'] = retention_ratio
                else:
                    # If no dividends found, assume full retention
                    ratios['retention_ratio'] = None
                
                roe_val = ratios.get('roe')
                if ratios.get('retention_ratio') is not None and roe_val is not None:
                    ratios['sgr_internal'] = ratios['retention_ratio'] * roe_val
                else:
                    ratios['sgr_internal'] = None
            except Exception:
                ratios['retention_ratio'] = None
                ratios['sgr_internal'] = None

            # âœ… Point 5: FCF per Share calculation
            try:
                fcf = ratios.get('free_cash_flow')
                existing_fcf_per_share = ratios.get('fcf_per_share')
                shares_for_fcfps = None
                share_candidates = [
                    pick('WeightedAverageNumberOfDilutedSharesOutstanding'),
                    pick('WeightedAverageNumberOfShareOutstandingBasicAndDiluted'),
                    shares_basic,
                    pick(
                        'WeightedAverageNumberOfSharesOutstandingBasic',
                        'CommonStockSharesOutstanding',
                        'SharesOutstanding',
                        'Basic (shares)',
                    ),
                    ratios.get('shares_outstanding'),
                ]
                normalized_candidates = []
                for sv in share_candidates:
                    nv = self._normalize_shares_to_million(sv)
                    if nv is not None and nv > 0:
                        normalized_candidates.append(float(nv))
                if normalized_candidates:
                    if prev_shares_outstanding not in (None, 0):
                        shares_for_fcfps = min(
                            normalized_candidates,
                            key=lambda s: abs(float(s) - float(prev_shares_outstanding)),
                        )
                    else:
                        shares_for_fcfps = normalized_candidates[0]
                if fcf is not None and shares_for_fcfps and shares_for_fcfps != 0:
                    ratios['fcf_per_share'] = self._select_per_share_scaled_value(
                        numerator=fcf,
                        shares=shares_for_fcfps,
                    )
                elif existing_fcf_per_share is None:
                    ratios['fcf_per_share'] = None
                else:
                    ratios['fcf_per_share'] = existing_fcf_per_share
            except Exception:
                ratios['fcf_per_share'] = None

            # placeholders for market data (will be filled by main.py)
            ratios['pe_ratio'] = None
            ratios['pb_ratio'] = None  # Price-to-Book
            ratios['dividend_yield'] = None
            ratios['market_cap'] = None
            ratios['fcf_yield'] = None
            ratios['ebitda'] = ebitda
            if equity_for_per_share and shares_basic:
                ratios['book_value_per_share'] = self._select_per_share_scaled_value(
                    numerator=equity_for_per_share,
                    shares=shares_basic,
                )
            else:
                ratios['book_value_per_share'] = None
            ratios['shares_outstanding'] = shares_basic
            
            # Store dividends and other metrics for reference
            ratios['dividends_paid'] = pick('dividends')
            ratios['operating_income'] = op
            ratios['gross_profit'] = gross
            ratios['capex_source'] = f"{capex_source_kind}:{capex_key}" if capex_key else capex_source_kind
            ratios['canonical_revenue_source'] = f"{revenue_source_kind}:{revenue_label}" if revenue_label else revenue_source_kind
            ratios['canonical_cogs_source'] = f"{cogs_source_kind}:{cogs_label}" if cogs_label else cogs_source_kind
            ratios['canonical_net_income_source'] = f"{net_source_kind}:{net_label}" if net_label else net_source_kind
            ratios['canonical_assets_source'] = f"{assets_source_kind}:{assets_label}" if assets_label else assets_source_kind
            ratios['canonical_equity_source'] = f"{equity_source_kind}:{equity_label}" if equity_label else equity_source_kind

            # Refresh carry-forward cache after all ratio computations for this year.
            for _k in carry_ratio_keys:
                if ratios.get(_k) is not None:
                    prev_carry_ratio_values[_k] = ratios.get(_k)

            # Mandatory validation gate against unit mismatch outliers.
            for _rid in (
                'roe',
                'gross_margin',
                'net_margin',
                'asset_turnover',
                'debt_to_equity',
                'debt_to_assets',
                'interest_coverage',
                'net_debt_ebitda',
            ):
                _raw = ratios.get(_rid)
                if _raw is None:
                    continue
                try:
                    _validated = self._validate_ratio_gate(_rid, _raw)
                    if _validated is None:
                        ratios[_rid] = None
                        ratios[f'{_rid}_source'] = 'UNIT_GATE_FLAGGED_OUT_OF_RANGE'
                    else:
                        ratios[_rid] = _validated
                except Exception:
                    ratios[_rid] = None
                    ratios[f'{_rid}_source'] = 'UNIT_GATE_HALTED_OUT_OF_RANGE'

            # Canonicalize + attach ratio formatting metadata + debug diagnostics.
            ratio_meta = {}
            ratio_debug = {}
            for rid in list(ratios.keys()):
                val = ratios.get(rid)
                if rid.endswith('_source'):
                    ratio_meta[rid] = {'ratio_format': 'text', 'ratio_unit': 'label', 'ratio_display_multiplier': 1}
                    ratio_debug[rid] = {
                        'stored_ratio_value_canonical': val,
                        'formatted_display_value': val,
                        'formatted_display_text': str(val) if val is not None else 'N/A',
                        'display_suffix': '',
                        'formatter_path_used': 'text_passthrough',
                        'format_rejection_reason': None,
                    }
                    continue
                cval = canonicalize_ratio_value(rid, val)
                ratios[rid] = cval
                meta = get_ratio_metadata(rid)
                fmt = format_ratio_value(rid, cval)
                ratio_meta[rid] = meta
                ratio_debug[rid] = {
                    'stored_ratio_value_canonical': cval,
                    'formatted_display_value': fmt.get('display_value'),
                    'formatted_display_text': fmt.get('display_text'),
                    'display_suffix': fmt.get('display_suffix'),
                    'formatter_path_used': fmt.get('formatter_path'),
                    'format_rejection_reason': fmt.get('format_rejection_reason'),
                }
            ratios['_ratio_metadata'] = ratio_meta
            ratios['_ratio_debug'] = ratio_debug

            ratios_by_year[year] = ratios

            if prev_carry_ratio_values and leading_missing_revenue_years:
                for _leading_year in list(leading_missing_revenue_years):
                    _leading_ratios = ratios_by_year.get(_leading_year, {}) or {}
                    for _k in (
                        'gross_margin',
                        'operating_margin',
                        'net_margin',
                        'ebitda_margin',
                        'asset_turnover',
                        'ocf_margin',
                        'inventory_turnover',
                    ):
                        if _leading_ratios.get(_k) is None and prev_carry_ratio_values.get(_k) is not None:
                            _leading_ratios[_k] = prev_carry_ratio_values.get(_k)
                            _leading_ratios[f'{_k}_source'] = 'SEC_HISTORY_BACKFILL_PROXY_LEADING_MISSING_REVENUE'
                    ratios_by_year[_leading_year] = _leading_ratios
                leading_missing_revenue_years = []

        # Company-level dividend profile normalization:
        # if no positive dividend marker exists across all years, set dividend_yield=0 explicitly.
        try:
            has_positive_dividend = False
            for _y, _r in (ratios_by_year or {}).items():
                dy = _r.get('dividend_yield')
                dp = _r.get('dividends_paid')
                if isinstance(dy, (int, float)) and abs(float(dy)) > 0:
                    has_positive_dividend = True
                    break
                if isinstance(dp, (int, float)) and abs(float(dp)) > 0:
                    has_positive_dividend = True
                    break
            if not has_positive_dividend:
                for _y, _r in (ratios_by_year or {}).items():
                    if _r.get('dividend_yield') is None:
                        _r['dividend_yield'] = 0.0
                        _r['dividend_yield_source'] = 'NO_DIVIDEND_PROFILE_ZERO'
        except Exception:
            pass

        # Backfill leading orphan annual years that have profit lines but no revenue anchor.
        try:
            years_sorted = sorted(ratios_by_year.keys())
            first_valid_margin_year = next(
                (
                    yy for yy in years_sorted
                    if any((ratios_by_year.get(yy, {}) or {}).get(k) is not None for k in ('gross_margin', 'operating_margin', 'net_margin'))
                ),
                None,
            )
            if first_valid_margin_year is not None:
                template = ratios_by_year.get(first_valid_margin_year, {}) or {}
                for yy in years_sorted:
                    if yy >= first_valid_margin_year:
                        break
                    src_row = data_by_year.get(yy, {}) or {}
                    if any(src_row.get(k) not in (None, 0) for k in ('Revenues', 'Revenue', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax')):
                        continue
                    target_row = ratios_by_year.get(yy, {}) or {}
                    for k in ('gross_margin', 'operating_margin', 'net_margin', 'ebitda_margin'):
                        if target_row.get(k) is None and template.get(k) is not None:
                            target_row[k] = template.get(k)
                            target_row[f'{k}_source'] = 'SEC_HISTORY_BACKFILL_PROXY_FIRST_VALID_YEAR'
                    ratios_by_year[yy] = target_row
        except Exception:
            pass

        return ratios_by_year

    def explain_ratio(self, name, value):
        if value is None:
            return "ØºÙŠØ± Ù…ØªÙˆÙØ±"
        try:
            v = float(value)
        except:
            return "ØºÙŠØ± Ù…ØªÙˆÙØ±"
        if name == 'altman_z_score':
            if v > 2.99: return "Ø®Ø·Ø± Ø¥ÙÙ„Ø§Ø³ Ù…Ù†Ø®ÙØ¶."
            if v > 1.8: return "Ù…Ù†Ø·Ù‚Ø© Ø±Ù…Ø§Ø¯ÙŠØ©."
            return "Ø®Ø·Ø± Ø¥ÙÙ„Ø§Ø³ Ù…Ø±ØªÙØ¹."
        if name == 'accruals_ratio':
            if abs(v) > 0.05: return "ØªØ­Ø°ÙŠØ±: ÙØ±ÙˆÙ‚Ø§Øª ÙƒØ¨ÙŠØ±Ø© Ø¨ÙŠÙ† Ø§Ù„Ø£Ø±Ø¨Ø§Ø­ ÙˆØ§Ù„ØªØ¯ÙÙ‚Ø§Øª."
            return "Ù„Ø§ ÙŠÙˆØ¬Ø¯ Ø§Ø®ØªÙ„Ø§Ù ÙƒØ¨ÙŠØ±"
        return f"Ù‚ÙŠÙ…Ø©: {round(v,2)}"

    def generate_forecast(self, data_by_year, metric='Revenues', years_forward=10):
        """
        Generate a conservative multi-year forecast from historical annual values.
        Uses a 3-stage fade model instead of fixed CAGR:
        - Stage 1: near-term growth anchored to recent history
        - Stage 2: transition fade
        - Stage 3: terminal growth anchor
        """
        metric_aliases = {
            'Revenues': ['Revenues', 'Revenue', 'SalesRevenueNet', 'TotalRevenue'],
            'NetIncomeLoss': ['NetIncomeLoss', 'NetIncome', 'ProfitLoss'],
        }
        aliases = metric_aliases.get(metric, [metric])
        hist = []
        for yk, row in (data_by_year or {}).items():
            try:
                y = int(yk)
            except Exception:
                continue
            row = row or {}
            val = None
            for key in aliases:
                v = row.get(key)
                if isinstance(v, (int, float)):
                    val = float(v)
                    break
            if val is not None:
                hist.append((y, val))
        hist.sort(key=lambda x: x[0])
        out = {'metric': metric, 'forecast': {}, 'method': 'insufficient_history'}
        if len(hist) < 2:
            return out

        years = [x[0] for x in hist]
        vals = [x[1] for x in hist]
        y0, y1 = years[0], years[-1]
        v0, v1 = vals[0], vals[-1]
        n = max(1, y1 - y0)

        # Build YoY growth history for robust anchoring.
        growth_hist = []
        for i in range(1, len(vals)):
            prev = vals[i - 1]
            cur = vals[i]
            if prev is None or cur is None or abs(prev) < 1e-9:
                continue
            g = (cur - prev) / abs(prev)
            if isinstance(g, (int, float)):
                growth_hist.append(float(g))

        # Per-metric conservative bounds.
        if metric == 'Revenues':
            g_floor, g_cap = -0.20, 0.30
            g_terminal = 0.03
        else:
            g_floor, g_cap = -0.35, 0.35
            g_terminal = 0.025

        # CAGR baseline (long history).
        cagr_long = None
        if v0 > 0 and v1 > 0:
            cagr_long = (v1 / v0) ** (1.0 / n) - 1.0

        # Recent trend (up to last 3 YoY points).
        recent = growth_hist[-3:] if growth_hist else []
        g_recent = (sum(recent) / len(recent)) if recent else None

        # Volatility-aware anchor.
        if growth_hist:
            g_avg = sum(growth_hist) / len(growth_hist)
            g_var = sum((g - g_avg) ** 2 for g in growth_hist) / max(1, len(growth_hist))
            g_std = g_var ** 0.5
        else:
            g_std = 0.0

        if g_recent is not None and cagr_long is not None:
            g_start = (0.65 * g_recent) + (0.35 * cagr_long)
        elif g_recent is not None:
            g_start = g_recent
        elif cagr_long is not None:
            g_start = cagr_long
        else:
            # No stable multiplicative history: fallback to linear.
            slope = (v1 - v0) / float(n)
            for i in range(1, int(years_forward) + 1):
                fy = y1 + i
                out['forecast'][fy] = v1 + (slope * i)
            out['method'] = 'linear_no_growth_history'
            return out

        # Volatility penalty: high volatility reduces usable growth anchor.
        vol_penalty = min(0.15, max(0.0, g_std * 0.5))
        g_start = g_start - vol_penalty
        g_start = max(g_floor, min(g_cap, g_start))

        # If latest is non-positive, avoid explosive compounding.
        if v1 <= 0:
            slope = (v1 - v0) / float(n)
            for i in range(1, int(years_forward) + 1):
                fy = y1 + i
                out['forecast'][fy] = v1 + (slope * i)
            out['method'] = 'linear_non_positive_latest'
            return out

        # Three-stage fade growth to terminal.
        # Stronger decay after year 3 to prevent unrealistic long-run explosions.
        v = float(v1)
        for i in range(1, int(years_forward) + 1):
            fy = y1 + i
            if i <= 3:
                decay = 0.22 * (i - 1)
            elif i <= 7:
                decay = 0.22 * 2 + 0.14 * (i - 3)
            else:
                decay = 0.22 * 2 + 0.14 * 4 + 0.10 * (i - 7)
            g_t = g_terminal + (g_start - g_terminal) * max(0.0, (1.0 - decay))
            g_t = max(g_floor, min(g_cap, g_t))
            v = v * (1.0 + g_t)
            out['forecast'][fy] = v

        out['method'] = 'three_stage_fade'
        out['g_start'] = g_start
        out['g_terminal'] = g_terminal
        out['volatility'] = g_std
        return out

    def generate_strategic_analysis(self, data_by_year, ratios_by_year):
        analysis = {}
        years = sorted(data_by_year.keys())
        if not years:
            return analysis
        latest = years[-1]
        prev = years[-2] if len(years) >= 2 else None
        growth = {}
        try:
            rev_latest = data_by_year[latest].get('Revenue') or data_by_year[latest].get('Revenues') or data_by_year[latest].get('SalesRevenueNet') or 0
            rev_prev = (
                data_by_year[prev].get('Revenue')
                or data_by_year[prev].get('Revenues')
                or data_by_year[prev].get('SalesRevenueNet')
                or 0
            ) if prev else None
            if prev and rev_prev and rev_prev != 0:
                growth_rate = ((rev_latest - rev_prev) / abs(rev_prev))
            else:
                growth_rate = None
        except:
            growth_rate = None
        growth['revenue_growth_1yr'] = growth_rate

        prof = {
            'net_margin_latest': ratios_by_year.get(latest, {}).get('net_margin'),
            'roe_latest': ratios_by_year.get(latest, {}).get('roe'),
            'roic_latest': ratios_by_year.get(latest, {}).get('roic')
        }
        liq = {
            'current_ratio': ratios_by_year.get(latest, {}).get('current_ratio'),
            'quick_ratio': ratios_by_year.get(latest, {}).get('quick_ratio')
        }
        lev = {
            'debt_to_equity': ratios_by_year.get(latest, {}).get('debt_to_equity'),
            'net_debt_ebitda': ratios_by_year.get(latest, {}).get('net_debt_ebitda'),
            'interest_coverage': ratios_by_year.get(latest, {}).get('interest_coverage')
        }
        analysis['growth'] = growth
        analysis['profitability'] = prof
        analysis['liquidity'] = liq
        analysis['leverage'] = lev
        return analysis
