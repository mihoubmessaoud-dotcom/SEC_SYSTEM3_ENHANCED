import unittest

from modules.institutional import InstitutionalEngineAPI, InstitutionalFinancialIntelligenceEngine


class InstitutionalEngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = InstitutionalFinancialIntelligenceEngine()

    def _aapl_like_data(self):
        company_meta = {
            'name': 'AAPL-like Industrial',
            'ticker': 'AAPLX',
            'cik': '0000000001',
            'sic': '3571',
            'naics': '334111',
            'filing_type': '10-K',
            'filing_grade': 'IN_RANGE_ANNUAL',
            'filing_in_range': True,
        }
        data_by_year = {
            2022: {
                'Revenues': 1000,
                'CostOfRevenue': 650,
                'GrossProfit': 350,
                'OperatingIncomeLoss': 150,
                'NetIncomeLoss': 120,
                'Assets': 2200,
                'Liabilities': 1200,
                'StockholdersEquity': 1000,
                'InventoryNet': 160,
                'NetCashProvidedByUsedInOperatingActivities': 210,
                'PaymentsToAcquirePropertyPlantAndEquipment': 80,
            },
            2023: {
                'Revenues': 1140,
                'CostOfRevenue': 730,
                'GrossProfit': 410,
                'OperatingIncomeLoss': 175,
                'NetIncomeLoss': 135,
                'Assets': 2400,
                'Liabilities': 1300,
                'StockholdersEquity': 1100,
                'InventoryNet': 170,
                'NetCashProvidedByUsedInOperatingActivities': 235,
                'PaymentsToAcquirePropertyPlantAndEquipment': 92,
            },
        }
        return company_meta, data_by_year

    def test_engine_required_outputs_exist(self):
        company_meta, data_by_year = self._aapl_like_data()
        outputs = self.engine.run(company_meta, data_by_year)
        required_keys = [
            'normalized_financial_statements',
            'sector_ratio_dashboard',
            'ratio_explanations',
            'risk_integrity_dashboard',
            'structural_change_report',
            'mapping_versions',
            'yearly_outputs',
            'classifier_diagnostics',
        ]
        for key in required_keys:
            self.assertIn(key, outputs)

    def test_aapl_like_classifies_industrial(self):
        company_meta, data_by_year = self._aapl_like_data()
        cls = self.engine.classifier.classify(company_meta, data_by_year[2023].keys(), data_by_year)
        self.assertEqual(cls['primary_profile'], 'industrial')

    def test_diagnostics_and_reliability_always_exist(self):
        company_meta, data_by_year = self._aapl_like_data()
        outputs = self.engine.run(company_meta, data_by_year)
        self.assertIn('sector', outputs['classifier_diagnostics'])
        self.assertIn('scores', outputs['classifier_diagnostics'])

        ratio_expl = outputs['ratio_explanations']
        self.assertTrue(len(ratio_expl.index) > 0)
        for _, row in ratio_expl.iterrows():
            self.assertIn('grade', row['reliability'])
            self.assertIn('score', row['reliability'])
            self.assertIn('gates_passed', row['reliability'])
            self.assertIn('gates_failed', row['reliability'])
            self.assertIn('validators_run', row['reliability'])
            if row.get('value') is None:
                self.assertEqual(row['reliability'].get('grade'), 'REJECTED')
                self.assertTrue(len(row.get('reasons') or []) > 0)

    def test_out_of_range_filing_caps_reliability(self):
        company_meta, data_by_year = self._aapl_like_data()
        company_meta['filing_grade'] = 'OUT_OF_RANGE_ANNUAL_FALLBACK'
        company_meta['filing_in_range'] = False
        outputs = self.engine.run(company_meta, data_by_year)
        ratio_expl = outputs['ratio_explanations']
        for _, row in ratio_expl.iterrows():
            rel = row['reliability']
            if rel.get('grade') != 'REJECTED':
                self.assertEqual(rel.get('grade'), 'LOW')
                self.assertIn('out_of_range_filing', row['reasons'])

    def test_json_api_payload(self):
        company_meta, data_by_year = self._aapl_like_data()
        api = InstitutionalEngineAPI()
        payload = api.process_company(company_meta, data_by_year, save_outputs=False)
        self.assertIn('classification', payload)
        self.assertIn('normalized_financial_statements', payload)
        self.assertIsInstance(payload['normalized_financial_statements'], list)


if __name__ == '__main__':
    unittest.main()
