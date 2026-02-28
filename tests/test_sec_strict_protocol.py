import unittest

from modules.sec_fetcher import SECDataFetcher


class TestSECStrictProtocol(unittest.TestCase):
    def setUp(self):
        self.fetcher = SECDataFetcher.__new__(SECDataFetcher)

    def test_decimals_normalization_negative_scale(self):
        v = self.fetcher._normalize_value_by_decimals(100, -6)
        self.assertEqual(v, 100000000.0)

    def test_validate_balance_sheet_balanced(self):
        row = {
            'Assets': 1000.0,
            'Liabilities': 600.0,
            'StockholdersEquity': 400.0,
        }
        result = self.fetcher.Validate_Balance_Sheet(row)
        self.assertTrue(result.get('ok'))

    def test_validate_balance_sheet_gap_fill_unclassified(self):
        row = {
            'Assets': 1000.0,
            'Liabilities': 500.0,
            'StockholdersEquity': 400.0,
            'SomeUnknownBucket': 100.0,
        }
        result = self.fetcher.Validate_Balance_Sheet(row)
        self.assertTrue(result.get('ok'))
        self.assertEqual(result.get('reason'), 'gap_filled_from_unclassified')

    def test_extract_all_items_strict_context_filters(self):
        facts = {
            'facts': {
                'us-gaap': {
                    'Revenues': {
                        'label': 'Revenues',
                        'units': {
                            'USD': [
                                {
                                    'accn': '0000000000-24-000001',
                                    'fy': 2024,
                                    'fp': 'FY',
                                    'frame': 'ConsolidatedStatement',
                                    'val': 100,
                                    'decimals': -6,
                                    'start': '2024-01-01',
                                    'end': '2024-12-31',
                                    'filed': '2025-01-30',
                                    'form': '10-K',
                                },
                                {
                                    'accn': '0000000000-24-000001',
                                    'fy': 2024,
                                    'fp': 'FY',
                                    'frame': 'ProductSegmentAxis',
                                    'val': 999,
                                    'decimals': -6,
                                    'start': '2024-01-01',
                                    'end': '2024-12-31',
                                    'filed': '2025-01-30',
                                    'form': '10-K',
                                },
                            ]
                        },
                    }
                }
            }
        }
        accn_map = {
            '000000000024000001': {'filing_year': 2024}
        }
        out = self.fetcher._extract_all_items_with_periods(facts, accn_map, include_all_concepts=True)
        self.assertIn('Revenues', out)
        self.assertIn('2024-FY', out['Revenues'])
        self.assertAlmostEqual(out['Revenues']['2024-FY']['value'], 100000000.0, places=6)


if __name__ == '__main__':
    unittest.main()
