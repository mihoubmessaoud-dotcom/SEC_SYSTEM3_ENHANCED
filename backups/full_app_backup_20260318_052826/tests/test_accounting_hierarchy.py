import unittest

from modules.sec_fetcher import SECDataFetcher


class AccountingHierarchyTests(unittest.TestCase):
    def setUp(self):
        self.fetcher = SECDataFetcher()

    def test_parent_child_aggregation_and_warning(self):
        data_by_year = {
            2024: {
                'CashAndCashEquivalentsAtCarryingValue': 100.0,
                'AccountsReceivableNetCurrent': 80.0,
                'InventoryNet': 70.0,
                'PrepaidExpenseCurrent': 50.0,
                'AssetsCurrent': 200.0,  # children=300 -> mismatch >5%
                'AccountsPayableCurrent': 40.0,
                'AccruedLiabilitiesCurrent': 30.0,
                'CurrentPortionOfLongTermDebt': 20.0,
            }
        }
        out = self.fetcher._apply_accounting_hierarchy(data_by_year)
        row = out[2024]
        self.assertAlmostEqual(row.get('TotalCurrentAssets_Hierarchy'), 300.0, places=6)
        self.assertAlmostEqual(row.get('TotalCurrentLiabilities_Hierarchy'), 90.0, places=6)
        diag = row.get('_accounting_hierarchy_diagnostics') or {}
        warnings = diag.get('warnings') or []
        self.assertTrue(any(w.get('parent') == 'AssetsCurrent' for w in warnings))

    def test_net_income_rule_excludes_accumulated_comprehensive(self):
        data_by_year = {
            2024: {
                'OperatingIncomeLoss': 120.0,
                'OtherNonoperatingIncomeExpense': 30.0,
                'IncomeTaxExpenseBenefit': 20.0,
                'AccumulatedOtherComprehensiveIncomeLossNetOfTax': 999999.0,
            }
        }
        out = self.fetcher._apply_accounting_hierarchy(data_by_year)
        row = out[2024]
        self.assertAlmostEqual(row.get('NetIncomeLoss_Hierarchy'), 130.0, places=6)
        self.assertAlmostEqual(row.get('OtherEquity') or 0.0, 999999.0, places=6)

    def test_unclassified_routes_to_other_buckets(self):
        data_by_year = {
            2024: {
                'MysteryLiabilityNode': 10.0,
                'UnknownAssetNode': 8.0,
                'WeirdIncomeNode': 5.0,
            }
        }
        out = self.fetcher._apply_accounting_hierarchy(data_by_year)
        row = out[2024]
        self.assertAlmostEqual(row.get('OtherLiabilities') or 0.0, 10.0, places=6)
        self.assertAlmostEqual(row.get('OtherAssets') or 0.0, 8.0, places=6)
        self.assertAlmostEqual(row.get('OtherIncomeStatement') or 0.0, 5.0, places=6)

    def test_ar_days_uses_aggregated_receivables(self):
        data_by_year = {
            2024: {
                'Revenues': 365.0,
                'AccountsReceivableNetCurrent': 30.0,
                'FinancingReceivableExcludingAccruedInterestBeforeAllowanceForCreditLoss': 20.0,
                'CashAndCashEquivalentsAtCarryingValue': 50.0,
                'InventoryNet': 25.0,
                'AccountsPayableCurrent': 15.0,
                'AccruedLiabilitiesCurrent': 10.0,
                'CurrentPortionOfLongTermDebt': 5.0,
                'PrepaidExpenseCurrent': 20.0,
                'NetIncomeLoss': 10.0,
                'Assets': 100.0,
                'Liabilities': 40.0,
                'StockholdersEquity': 60.0,
            }
        }
        hier = self.fetcher._apply_accounting_hierarchy(data_by_year)
        ratios = self.fetcher._calculate_financial_ratios(hier)
        r = ratios[2024]
        self.assertAlmostEqual(r.get('days_sales_outstanding') or 0.0, 50.0, places=6)
        self.assertEqual(r.get('ar_days_reliability'), 1.0)
        self.assertTrue(r.get('ar_days_inputs_aggregated'))
        self.assertAlmostEqual(r.get('current_ratio') or 0.0, (125.0 / 30.0), places=6)

    def test_strict_mapping_hierarchy_labels_are_not_unclassified(self):
        data_by_year = {
            2024: {
                'TotalCurrentAssets_Hierarchy': 500.0,
                'TotalCurrentLiabilities_Hierarchy': 200.0,
                'OperatingExpenses_Hierarchy': 100.0,
            }
        }
        layers = self.fetcher._build_data_layers(data_by_year)
        rows = layers.get('label_rows') or []
        by_raw = {r['raw_label']: r for r in rows}
        self.assertEqual(by_raw['TotalCurrentAssets_Hierarchy']['category'], 'Balance Sheet')
        self.assertEqual(by_raw['TotalCurrentLiabilities_Hierarchy']['category'], 'Balance Sheet')
        self.assertEqual(by_raw['OperatingExpenses_Hierarchy']['category'], 'Income Statement')

    def test_layer2_prefers_primary_over_subcomponents(self):
        data_by_year = {
            2024: {
                'AccountsReceivableNetCurrent': 100.0,
                'AccountsReceivableNetCurrent_Hierarchy': 80.0,
                'AccountsReceivableSale': 40.0,
                'TotalCurrentLiabilities_Parent': 30.0,
            }
        }
        layers = self.fetcher._build_data_layers(data_by_year)
        l2 = layers.get('layer2_by_year', {}).get(2024, {})
        self.assertAlmostEqual(l2.get('Accounts Receivable') or 0.0, 100.0, places=6)
        self.assertAlmostEqual(l2.get('Current Liabilities') or 0.0, 30.0, places=6)
        self.assertIsNone(l2.get('Total Liabilities'))

    def test_layer2_derives_total_liabilities_when_missing(self):
        data_by_year = {
            2024: {
                'Assets': 200.0,
                'StockholdersEquity': 120.0,
            }
        }
        layers = self.fetcher._build_data_layers(data_by_year)
        l2 = layers.get('layer2_by_year', {}).get(2024, {})
        l3 = layers.get('layer3_by_year', {}).get(2024, {})
        self.assertAlmostEqual(l2.get('Total Liabilities') or 0.0, 80.0, places=6)
        self.assertAlmostEqual(l3.get('Balance Sheet::Total Liabilities') or 0.0, 80.0, places=6)

    def test_smart_sniper_assets_from_hierarchy_and_balance(self):
        data_by_year = {
            2024: {
                'AssetsCurrent': 300.0,
                'AssetsNoncurrent': 700.0,
            },
            2025: {
                'Liabilities': 600.0,
                'StockholdersEquity': 500.0,
            }
        }
        out = self.fetcher._apply_accounting_hierarchy(data_by_year)
        self.assertAlmostEqual(out[2024].get('Assets') or 0.0, 1000.0, places=6)
        self.assertAlmostEqual(out[2025].get('Assets') or 0.0, 1100.0, places=6)

    def test_golden_tags_are_forced_into_classified_layer3(self):
        data_by_year = {
            2024: {
                'AssetsNoncurrent': 900.0,
                'LiabilitiesAndStockholdersEquity': 1200.0,
                'NetCashProvidedByUsedInOperatingActivities': 100.0,
            }
        }
        layers = self.fetcher._build_data_layers(data_by_year)
        l3 = layers.get('layer3_by_year', {}).get(2024, {})
        self.assertIn('Balance Sheet::Non-current Assets', l3)
        self.assertIn('Balance Sheet::Total Assets', l3)
        self.assertIn('Cash Flow::Operating Cash Flow', l3)


if __name__ == '__main__':
    unittest.main()
