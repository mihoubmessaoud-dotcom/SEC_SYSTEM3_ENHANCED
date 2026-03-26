import unittest

from modules.sec_fetcher import SECDataFetcher
from modules.institutional import InstitutionalFinancialIntelligenceEngine


class FilingFallbackTests(unittest.TestCase):
    def test_select_filings_with_fallback_logic(self):
        f = SECDataFetcher()
        recent = {
            'form': ['8-K', '10-k/a', '20-f'],
            'filingDate': ['2024-05-01', '2024-02-15', '2023-03-10'],
            'accessionNumber': ['0001-1', '0001-2', '0001-3'],
            'reportDate': ['2024-03-31', '2023-12-31', '2022-12-31'],
        }
        filtered, accn_map, diag = f._select_filings_with_fallback(recent, 2024, 2024, '10-K')
        self.assertTrue(filtered)
        self.assertIn('attempts', diag)
        self.assertIn('filing_grade', diag)
        self.assertIn(diag['filing_grade'], ['IN_RANGE_ANNUAL', 'IN_RANGE_EQUIVALENT', 'OUT_OF_RANGE_ANNUAL_FALLBACK'])


class StrictSectorAndRatioValidationTests(unittest.TestCase):
    def setUp(self):
        self.engine = InstitutionalFinancialIntelligenceEngine()

    def _jpm_like(self):
        return {
            2022: {
                'NetInterestIncome': 12_000_000_000,
                'NetIncomeLoss': 6_000_000_000,
                'Assets': 3_000_000_000_000,
                'Liabilities': 2_700_000_000_000,
                'StockholdersEquity': 300_000_000_000,
                'LoansAndLeasesReceivableNetReportedAmount': 1_800_000_000_000,
                'Deposits': 2_200_000_000_000,
                'CommonEquityTier1CapitalAmount': 280_000_000_000,
                'NetCashProvidedByUsedInOperatingActivities': 90_000_000_000,
            },
            2023: {
                'NetInterestIncome': 14_000_000_000,
                'NetIncomeLoss': 7_000_000_000,
                'Assets': 3_300_000_000_000,
                'Liabilities': 2_950_000_000_000,
                'StockholdersEquity': 350_000_000_000,
                'LoansAndLeasesReceivableNetReportedAmount': 1_950_000_000_000,
                'Deposits': 2_400_000_000_000,
                'CommonEquityTier1CapitalAmount': 300_000_000_000,
                'NetCashProvidedByUsedInOperatingActivities': 110_000_000_000,
            },
        }

    def _aig_like(self):
        return {
            2022: {
                'PremiumsEarnedNet': 52_000_000_000,
                'PolicyholderBenefitsAndClaimsIncurredNet': 39_000_000_000,
                'ReinsuranceRecoverables': 8_500_000_000,
                'UnderwritingIncomeLoss': 4_500_000_000,
                'NetIncomeLoss': 3_200_000_000,
                'Assets': 2_100_000_000_000,
                'Liabilities': 1_700_000_000_000,
                'StockholdersEquity': 400_000_000_000,
                'NetCashProvidedByUsedInOperatingActivities': 70_000_000_000,
            },
            2023: {
                'PremiumsEarnedNet': 56_000_000_000,
                'PolicyholderBenefitsAndClaimsIncurredNet': 41_500_000_000,
                'ReinsuranceRecoverables': 9_000_000_000,
                'UnderwritingIncomeLoss': 5_100_000_000,
                'NetIncomeLoss': 3_600_000_000,
                'Assets': 2_250_000_000_000,
                'Liabilities': 1_810_000_000_000,
                'StockholdersEquity': 440_000_000_000,
                'NetCashProvidedByUsedInOperatingActivities': 78_000_000_000,
            },
        }

    def test_jpm_classifies_bank(self):
        meta = {'name': 'JPM-like', 'ticker': 'JPMX', 'cik': '0002', 'sic': '6021', 'naics': '522110', 'filing_grade': 'IN_RANGE_ANNUAL', 'filing_in_range': True}
        cls = self.engine.classifier.classify(meta, self._jpm_like()[2023].keys(), self._jpm_like())
        self.assertEqual(cls['primary_profile'], 'bank')

    def test_aig_not_bank(self):
        meta = {'name': 'AIG-like', 'ticker': 'AIGX', 'cik': '0003', 'sic': '6331', 'naics': '524126', 'filing_grade': 'IN_RANGE_ANNUAL', 'filing_in_range': True}
        cls = self.engine.classifier.classify(meta, self._aig_like()[2023].keys(), self._aig_like())
        self.assertEqual(cls['primary_profile'], 'insurance')
        self.assertNotEqual(cls['primary_profile'], 'bank')

    def test_bank_ratios_and_gating(self):
        # JPM-like
        meta_j = {'name': 'JPM-like', 'ticker': 'JPMX', 'cik': '0002', 'sic': '6021', 'naics': '522110', 'filing_grade': 'IN_RANGE_ANNUAL', 'filing_in_range': True}
        out_j = self.engine.run(meta_j, self._jpm_like())
        cols_j = set(out_j['sector_ratio_dashboard'].columns)
        self.assertIn('net_interest_margin', cols_j)
        self.assertIn('loan_to_deposit_ratio', cols_j)
        self.assertIn('capital_ratio_proxy', cols_j)
        self.assertNotIn('gross_margin', cols_j)
        self.assertNotIn('inventory_turnover', cols_j)

        # AIG-like NIM must be rejected with insurance_company
        meta_a = {'name': 'AIG-like', 'ticker': 'AIGX', 'cik': '0003', 'sic': '6331', 'naics': '524126', 'filing_grade': 'IN_RANGE_ANNUAL', 'filing_in_range': True}
        out_a = self.engine.run(meta_a, self._aig_like())
        rexp = out_a['ratio_explanations']
        nim_rows = rexp[rexp['ratio_id'] == 'net_interest_margin']
        self.assertTrue(len(nim_rows.index) > 0)
        for _, r in nim_rows.iterrows():
            self.assertEqual((r['reliability'] or {}).get('grade'), 'REJECTED')
            reasons = r.get('reasons', [])
            self.assertIn('ratio_not_applicable_insurance', reasons)
            self.assertIn('insurance_company', reasons)

        # JPM-like NIM should be computed (not rejected)
        rexp_j = out_j['ratio_explanations']
        nim_j = rexp_j[rexp_j['ratio_id'] == 'net_interest_margin']
        self.assertTrue(len(nim_j.index) > 0)
        self.assertTrue(any((r.get('reliability') or {}).get('grade') != 'REJECTED' for _, r in nim_j.iterrows()))


if __name__ == '__main__':
    unittest.main()
