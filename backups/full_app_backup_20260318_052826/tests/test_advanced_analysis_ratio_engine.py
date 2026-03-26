import json
import os
import unittest
from pathlib import Path

from main import SECFinancialSystem
from modules.advanced_analysis import AdvancedFinancialAnalysis, generate_ai_insights
from modules.ratio_engine import RatioEngine
from modules.ratio_source import UnifiedRatioSource
from modules.ratio_source import maybe_guard_ratios_by_year
from modules.ratio_formats import format_ratio_value
from modules.sec_fetcher import SECDataFetcher


class AdvancedAnalysisRatioEngineTests(unittest.TestCase):
    def test_roe_is_consumed_from_ratio_engine_not_recomputed(self):
        analyzer = AdvancedFinancialAnalysis()
        data_by_year = {
            2023: {'NetIncomeLoss': 120.0, 'StockholdersEquity': 1_000.0, 'Revenues': 50_000_000_000},
        }
        ratios_by_year = {
            2023: {'roe': 0.12, 'retention_ratio': 0.5, 'sgr_internal': 0.06, 'roic': 0.11},
        }
        analyzer.load_ratio_context(data_by_year, ratios_by_year)
        impact = analyzer._calculate_scenario_impact(
            data_by_year, ratios_by_year, revenue_growth=0.1, retention=0.5, cost_of_debt=0.04
        )
        self.assertAlmostEqual(impact.get('sgr_internal'), 0.06, places=8)

    def test_ratio_engine_dso_ccc_guardrails(self):
        eng = RatioEngine()
        data_by_year = {
            2024: {
                'Revenues': 100_000_000_000,
                'CostOfRevenue': 50_000_000_000,
                'AccountsReceivableNetCurrent': 1_000_000_000_000_000,
                'InventoryNet': 1_000_000_000_000_000,
                'AccountsPayableCurrent': 1_000_000_000_000_000,
                'NetIncomeLoss': 10_000_000,
                'StockholdersEquity': 1_000_000_000,
            },
        }
        ratios_by_year = {2024: {'roe': 0.10}}
        out = eng.build(data_by_year, ratios_by_year)
        c = out['contracts_by_year'][2024]
        self.assertEqual(c['days_sales_outstanding']['value'], None)
        self.assertEqual(c['days_sales_outstanding']['reason'], 'plausibility_violation')
        self.assertEqual(c['ccc_days']['value'], None)
        self.assertIn(c['ccc_days']['reason'], ['plausibility_violation', 'missing_ccc_components'])

    def test_gross_margin_hard_bound_for_industrial(self):
        eng = RatioEngine()
        data_by_year = {
            2024: {
                'Revenues': 100.0,
                'CostOfRevenue': 1.0,  # 99% margin -> reject
                'NetIncomeLoss': 10.0,
                'StockholdersEquity': 100.0,
                'AccountsReceivableNetCurrent': 10.0,
            }
        }
        out = eng.build(data_by_year, {2024: {}})
        gm = out['ratios'][2024]['gross_margin']
        self.assertIsNone(gm['value'])
        self.assertEqual(gm['reason'], 'plausibility_violation')

    def test_ui_performance_tier_roe_matches_base_ratio(self):
        class _Var:
            def __init__(self, v):
                self.v = v
            def get(self):
                return self.v

        class _Dummy:
            pass

        d = _Dummy()
        d.price_var = _Var(0.0)
        d.shares_var = _Var(0.0)
        d.cost_of_debt_var = _Var(4.0)
        d.current_data = {'market_data': {}}
        d._get_selected_years_range = lambda: [2024]
        d._debug_ui_contracts_enabled = lambda: False
        d._assert_no_legacy_ratio_keys = lambda _r: None

        data_by_year = {
            2024: {
                'NetIncomeLoss': 12.0,
                'StockholdersEquity': 100.0,
                'Revenues': 365.0,
                'CostOfRevenue': 200.0,
                'AccountsReceivableNetCurrent': 60.0,
                'AccountsPayableCurrent': 40.0,
                'InventoryNet': 30.0,
            }
        }
        ratios_by_year = {
            2024: {
                'roe': 0.12,
                'roic': 0.10,
                'retention_ratio': 0.5,
                'sgr_internal': 0.06,
                'days_sales_outstanding': 60.0,
                'inventory_days': 30.0,
                'ap_days': 40.0,
                'ccc_days': 50.0,
            }
        }

        per_year = SECFinancialSystem._compute_per_year_metrics(d, data_by_year, ratios_by_year)
        src = UnifiedRatioSource()
        src.load('CURRENT', data_by_year, ratios_by_year)
        roe_c = src.get_ratio_contract('CURRENT', 2024, 'roe').get('value')
        dso_c = src.get_ratio_contract('CURRENT', 2024, 'dso_days').get('value')
        ccc_c = src.get_ratio_contract('CURRENT', 2024, 'ccc_days').get('value')
        self.assertEqual(per_year[2024]['ROE']['source'], 'ratio_engine')
        self.assertAlmostEqual(per_year[2024]['ROE']['value'], roe_c, places=8)
        self.assertAlmostEqual(per_year[2024]['AR_Days']['value'], dso_c, places=8)
        self.assertAlmostEqual(per_year[2024]['CCC_Days']['value'], ccc_c, places=8)
        self.assertEqual(per_year[2024]['ROE']['display'], format_ratio_value('roe', roe_c)['display_text'])

    def test_urs_aliases_normalize_to_single_contract(self):
        src = UnifiedRatioSource()
        data_by_year = {
            2024: {
                'NetIncomeLoss': 12.0,
                'StockholdersEquity': 100.0,
                'Revenues': 1_000.0,
                'CostOfRevenue': 400.0,
                'AccountsReceivableNetCurrent': 100.0,
            }
        }
        ratios_by_year = {2024: {'sgr_internal': 0.1}}
        src.load('INTC', data_by_year, ratios_by_year)
        c1 = src.get_ratio_contract('INTC', 2024, 'ROE')
        c2 = src.get_ratio_contract('INTC', 2024, 'return_on_equity')
        c3 = src.get_ratio_contract('INTC', 2024, 'roe')
        self.assertEqual(c1.get('ratio_id'), 'roe')
        self.assertAlmostEqual(c1.get('value'), c2.get('value'), places=8)
        self.assertAlmostEqual(c2.get('value'), c3.get('value'), places=8)

    def test_debug_guard_blocks_non_normalized_ratio_keys(self):
        os.environ['SEC_DEBUG_RATIO_GUARD'] = '1'
        try:
            guarded = maybe_guard_ratios_by_year({2024: {'roe': 0.1}})
            row = guarded.get(2024, {})
            with self.assertRaises(RuntimeError):
                _ = row['ROE']
        finally:
            os.environ.pop('SEC_DEBUG_RATIO_GUARD', None)

    def test_advanced_analysis_diagnostics_written_and_sourced(self):
        data_by_year = {
            2024: {
                'Revenues': 100_000_000_000,
                'SalesRevenueNet': 100_000_000_000,
                'NetIncomeLoss': 10_000_000_000,
                'Liabilities': 50_000_000_000,
                'AccountsReceivableNetCurrent': 10_000_000_000,
            }
        }
        ratios_by_year = {
            2024: {
                'roe': 0.20,
                'roic': 0.15,
                'sgr_internal': 0.10,
                'retention_ratio': 0.5,
                'days_sales_outstanding': 120.0,
                'inventory_days': 30.0,
                'ap_days': 45.0,
                'net_margin': 0.1,
                'accruals_ratio': 0.01,
                'altman_z_score': 3.2,
                'net_debt_ebitda': 1.0,
                'interest_coverage': 5.0,
            }
        }
        _ = generate_ai_insights(data_by_year, ratios_by_year, 70.0, 0.04, 0.03)
        path = Path('exports/sector_comparison/advanced_analysis_diagnostics.json')
        self.assertTrue(path.exists())
        payload = json.loads(path.read_text(encoding='utf-8'))
        metric = payload['metrics']['2024']['roe']
        self.assertEqual(metric['source'], 'ratio_engine')
        lockdown_path = Path('exports/sector_comparison/data_integrity_lockdown_report.json')
        self.assertTrue(lockdown_path.exists())

    def test_canonical_item_selection_diagnostics_written(self):
        eng = RatioEngine()
        data_by_year = {
            2024: {
                'Revenues': 1000.0,
                'CostOfRevenue': 400.0,
                'StockholdersEquity': 500.0,
                'AccountsReceivableNetCurrent': 80.0,
                'AccountsPayableCurrent': 60.0,
                'InventoryNet': 70.0,
            }
        }
        eng.build(data_by_year, {2024: {}})
        path = Path('exports/sector_comparison/canonical_item_selection_diagnostics.json')
        self.assertTrue(path.exists())
        payload = json.loads(path.read_text(encoding='utf-8'))
        row = payload.get('2024', {})
        self.assertEqual(row.get('canonical_revenue_tag'), 'Revenues')
        self.assertEqual(row.get('canonical_cogs_tag'), 'CostOfRevenue')

    def test_day_ratio_rejected_when_revenue_not_fy(self):
        eng = RatioEngine()
        data_by_year = {
            2024: {
                'SalesRevenueNetQTD': 300.0,
                'CostOfRevenue': 120.0,
                'AccountsReceivableNetCurrent': 30.0,
                'NetIncomeLoss': 10.0,
                'StockholdersEquity': 100.0,
            }
        }
        out = eng.build(data_by_year, {2024: {}})
        dso = out['ratios'][2024]['days_sales_outstanding']
        self.assertIsNone(dso.get('value'))
        self.assertEqual(dso.get('reason'), 'revenue_not_fy')

    def test_mixed_fiscal_end_date_blocks_ratio(self):
        eng = RatioEngine()
        data_by_year = {
            2024: {
                'Revenues': 1_000.0,
                'CostOfRevenue': 400.0,
                'AccountsReceivableNetCurrent': 100.0,
                '__canonical_fact_candidates__': {
                    'annual_revenue': [
                        {
                            'tag': 'Revenues',
                            'value': 1_000.0,
                            'period_type': 'FY',
                            'period_end': '2024-12-31',
                            'unit': 'USD',
                        }
                    ],
                    'accounts_receivable': [
                        {
                            'tag': 'AccountsReceivableNetCurrent',
                            'value': 100.0,
                            'period_type': 'INSTANT',
                            'period_end': '2024-10-01',
                            'unit': 'USD',
                        }
                    ],
                },
            }
        }
        out = eng.build(data_by_year, {2024: {}})
        dso = out['ratios'][2024]['days_sales_outstanding']
        self.assertIsNone(dso.get('value'))
        self.assertEqual(dso.get('reason'), 'fy_mismatch')

    def test_ratio_reliability_and_integrity_reports_written(self):
        eng = RatioEngine()
        data_by_year = {
            2025: {
                'Revenues': 79_000_000_000.0,
                'CostOfRevenue': 44_000_000_000.0,
                'StockholdersEquity': 105_000_000_000.0,
                'AccountsReceivableNetCurrent': 20_000_000_000.0,
                '__html_table_values__': {'gross_margin': 0.443},
            }
        }
        eng.build(data_by_year, {2025: {}})
        self.assertTrue(Path('exports/sector_comparison/data_integrity_diagnostics.json').exists())
        self.assertTrue(Path('exports/sector_comparison/ratio_reliability_report.json').exists())

    def test_intel_2025_revenue_html_match_within_three_percent(self):
        eng = RatioEngine()
        html_revenue = 79_000_000_000.0
        xbrl_revenue = 79_400_000_000.0
        data_by_year = {
            2025: {
                'Revenues': xbrl_revenue,
                'CostOfRevenue': 44_000_000_000.0,
                '__html_table_values__': {'annual_revenue': html_revenue},
            }
        }
        out = eng.build(data_by_year, {2025: {}})
        selected = out['items'][2025]['canonical_revenue']['value']
        mismatch = abs(selected - html_revenue) / html_revenue
        self.assertLessEqual(mismatch, 0.03)

    def test_hierarchy_inputs_produce_100_reliability_for_day_ratios(self):
        eng = RatioEngine()
        data_by_year = {
            2023: {
                'SalesRevenueNet': 730.0,
                'CostOfRevenue': 365.0,
                'AccountsReceivableNetCurrent_Hierarchy': 73.0,
                'AccountsPayableCurrent_Hierarchy': 36.5,
                'InventoryNet_Hierarchy': 18.25,
            },
            2024: {
                'SalesRevenueNet': 730.0,
                'CostOfRevenue': 365.0,
                'AccountsReceivableNetCurrent_Hierarchy': 73.0,
                'AccountsPayableCurrent_Hierarchy': 36.5,
                'InventoryNet_Hierarchy': 18.25,
            },
        }
        out = eng.build(data_by_year, {2024: {}})
        row = out['ratios'][2024]
        self.assertEqual(row['days_sales_outstanding']['reliability'], 100)
        self.assertEqual(row['ap_days']['reliability'], 100)
        self.assertEqual(row['inventory_days']['reliability'], 100)
        self.assertEqual(row['ccc_days']['reliability'], 100)

    def test_statement_tree_lock_blocks_silent_raw_fallback(self):
        eng = RatioEngine()
        data_by_year = {
            2025: {
                'Revenues': 10_000.0,
                'CostOfRevenue': 7_000.0,
                '__statement_tree_required__': True,
                '__canonical_fact_candidates__': {
                    'annual_revenue': [],
                    'annual_cogs': [],
                    'total_equity': [],
                    'accounts_receivable': [],
                    'accounts_payable': [],
                    'inventory': [],
                },
            }
        }
        out = eng.build(data_by_year, {2025: {}})
        rev_item = out['items'][2025]['canonical_revenue']
        gm = out['ratios'][2025]['gross_margin']
        self.assertIsNone(rev_item.get('value'))
        self.assertIn(rev_item.get('selection_reason', ''), ['annual_revenue_missing_or_invalid', 'concept_fallback_used'])
        self.assertIsNone(gm.get('value'))

    def test_parent_child_mismatch_gt_five_percent_forces_zero_reliability(self):
        eng = RatioEngine()
        data_by_year = {
            2025: {
                '__statement_tree_required__': True,
                '__canonical_fact_candidates__': {
                    'annual_revenue': [
                        {
                            'tag': 'Revenues',
                            'value': 1000.0,
                            'period_type': 'FY',
                            'period_end': '2025-12-31',
                            'unit': 'USD',
                            'parent_child_mismatch_pct': 0.06,
                        }
                    ],
                    'annual_cogs': [
                        {
                            'tag': 'CostOfRevenue',
                            'value': 600.0,
                            'period_type': 'FY',
                            'period_end': '2025-12-31',
                            'unit': 'USD',
                            'parent_child_mismatch_pct': 0.06,
                        }
                    ],
                    'total_equity': [],
                    'accounts_receivable': [],
                    'accounts_payable': [],
                    'inventory': [],
                },
            }
        }
        out = eng.build(data_by_year, {2025: {}})
        gm = out['ratios'][2025]['gross_margin']
        self.assertEqual(gm.get('reliability'), 0)
        self.assertEqual(gm.get('reason'), 'parent_child_mismatch')
        self.assertIsNone(gm.get('value'))

    def test_intc_statement_tree_hard_lock_matches_html_tolerances(self):
        fetcher = SECDataFetcher.__new__(SECDataFetcher)

        pre_xml = """<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:presentationLink xlink:role="http://xbrl.sec.gov/role/StatementOfIncome">
    <link:loc xlink:label="loc_rev" xlink:href="intel-20251231.xsd#us-gaap:Revenues"/>
    <link:loc xlink:label="loc_cogs" xlink:href="intel-20251231.xsd#us-gaap:CostOfRevenue"/>
    <link:loc xlink:label="loc_gp" xlink:href="intel-20251231.xsd#us-gaap:GrossProfit"/>
    <link:loc xlink:label="loc_opex" xlink:href="intel-20251231.xsd#us-gaap:OperatingExpenses"/>
    <link:loc xlink:label="loc_op" xlink:href="intel-20251231.xsd#us-gaap:OperatingIncomeLoss"/>
    <link:presentationArc xlink:from="loc_gp" xlink:to="loc_rev"/>
    <link:presentationArc xlink:from="loc_gp" xlink:to="loc_cogs"/>
    <link:presentationArc xlink:from="loc_op" xlink:to="loc_gp"/>
    <link:presentationArc xlink:from="loc_op" xlink:to="loc_opex"/>
  </link:presentationLink>
</link:linkbase>
"""
        cal_xml = """<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:calculationLink xlink:role="http://xbrl.sec.gov/role/StatementOfIncome">
    <link:loc xlink:label="loc_rev" xlink:href="intel-20251231.xsd#us-gaap:Revenues"/>
    <link:loc xlink:label="loc_cogs" xlink:href="intel-20251231.xsd#us-gaap:CostOfRevenue"/>
    <link:loc xlink:label="loc_gp" xlink:href="intel-20251231.xsd#us-gaap:GrossProfit"/>
    <link:loc xlink:label="loc_opex" xlink:href="intel-20251231.xsd#us-gaap:OperatingExpenses"/>
    <link:loc xlink:label="loc_op" xlink:href="intel-20251231.xsd#us-gaap:OperatingIncomeLoss"/>
    <link:calculationArc xlink:from="loc_gp" xlink:to="loc_rev" weight="1"/>
    <link:calculationArc xlink:from="loc_gp" xlink:to="loc_cogs" weight="-1"/>
    <link:calculationArc xlink:from="loc_op" xlink:to="loc_gp" weight="1"/>
    <link:calculationArc xlink:from="loc_op" xlink:to="loc_opex" weight="-1"/>
  </link:calculationLink>
</link:linkbase>
"""
        instance_xml = """<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:us-gaap="http://fasb.org/us-gaap/2025">
  <xbrli:context id="ctx_2025">
    <xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000050863</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2025-01-01</xbrli:startDate><xbrli:endDate>2025-12-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:unit id="u_usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
  <us-gaap:Revenues contextRef="ctx_2025" unitRef="u_usd">1000</us-gaap:Revenues>
  <us-gaap:CostOfRevenue contextRef="ctx_2025" unitRef="u_usd">620</us-gaap:CostOfRevenue>
  <us-gaap:GrossProfit contextRef="ctx_2025" unitRef="u_usd">380</us-gaap:GrossProfit>
  <us-gaap:OperatingExpenses contextRef="ctx_2025" unitRef="u_usd">170</us-gaap:OperatingExpenses>
  <us-gaap:OperatingIncomeLoss contextRef="ctx_2025" unitRef="u_usd">210</us-gaap:OperatingIncomeLoss>
  <us-gaap:NetIncomeLoss contextRef="ctx_2025" unitRef="u_usd">180</us-gaap:NetIncomeLoss>
</xbrli:xbrl>
"""

        fetcher._fetch_statement_linkbases = lambda cik, filing: {
            'presentation_xml': pre_xml,
            'calculation_xml': cal_xml,
            'instance_xml': instance_xml,
            'index_url': 'mock://index',
            'presentation_file': 'mock_pre.xml',
            'calculation_file': 'mock_cal.xml',
        }

        data_by_year = {
            2025: {
                'Revenues': 1000.0,
                'CostOfRevenue': 620.0,
                'GrossProfit': 380.0,
                'OperatingExpenses': 170.0,
                'OperatingIncomeLoss': 210.0,
            }
        }
        items_by_concept = {
            'Revenues': {'2025-FY': {'fiscal_year': 2025, 'fiscal_period': 'FY', 'period_end': '2025-12-31', 'unit': 'USD', 'period_type': 'DURATION'}},
            'CostOfRevenue': {'2025-FY': {'fiscal_year': 2025, 'fiscal_period': 'FY', 'period_end': '2025-12-31', 'unit': 'USD', 'period_type': 'DURATION'}},
            'GrossProfit': {'2025-FY': {'fiscal_year': 2025, 'fiscal_period': 'FY', 'period_end': '2025-12-31', 'unit': 'USD', 'period_type': 'DURATION'}},
            'OperatingExpenses': {'2025-FY': {'fiscal_year': 2025, 'fiscal_period': 'FY', 'period_end': '2025-12-31', 'unit': 'USD', 'period_type': 'DURATION'}},
            'OperatingIncomeLoss': {'2025-FY': {'fiscal_year': 2025, 'fiscal_period': 'FY', 'period_end': '2025-12-31', 'unit': 'USD', 'period_type': 'DURATION'}},
        }
        selected_filings = [{'accession_number': '0000050863-26-000011', 'primary_document': 'intc-20251231x10k.htm', 'year': 2025}]

        diag = fetcher._apply_statement_tree_intelligence('0000050863', selected_filings, items_by_concept, data_by_year)
        self.assertTrue(diag.get('strict_mode'))

        year_diag = (diag.get('years') or {}).get('2025', {})
        metrics_diag = year_diag.get('metrics', {})
        rev_d = metrics_diag.get('annual_revenue', {})
        gp_d = metrics_diag.get('gross_profit', {})
        op_d = metrics_diag.get('operating_income', {})

        html_revenue = 1000.0
        html_gross_profit = 380.0
        html_operating_income = 210.0

        rev_mismatch = abs((rev_d.get('parent_reported_value') or 0) - html_revenue) / html_revenue
        gp_mismatch = abs((gp_d.get('parent_reported_value') or 0) - html_gross_profit) / html_gross_profit
        op_mismatch = abs((op_d.get('parent_reported_value') or 0) - html_operating_income) / html_operating_income

        self.assertLessEqual(rev_mismatch, 0.03)
        self.assertLessEqual(gp_mismatch, 0.03)
        self.assertLessEqual(op_mismatch, 0.05)

        self.assertTrue(data_by_year[2025].get('__statement_tree_required__'))
        self.assertTrue(isinstance(data_by_year[2025].get('__canonical_fact_candidates__', {}).get('annual_revenue'), list))


if __name__ == '__main__':
    unittest.main()
