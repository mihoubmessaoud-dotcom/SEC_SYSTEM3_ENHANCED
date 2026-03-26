import unittest

from modules.institutional.ratios import SectorRatioEngine


class ReliabilityPipelineTests(unittest.TestCase):
    def setUp(self):
        self.engine = SectorRatioEngine()
        self.base_row = {
            'IS.NII': 12_000_000_000.0,
            'BS.ASSETS': 3_000_000_000_000.0,
            'BS.LOANS': 1_700_000_000_000.0,
            'BS.DEPOSITS': 2_100_000_000_000.0,
            'BS.CET1': 300_000_000_000.0,
            'BS.EQ': 350_000_000_000.0,
            'IS.NET': 7_000_000_000.0,
            'BS.LIAB': 2_650_000_000_000.0,
        }
        self.common_kwargs = {
            'provenance_ctx': {
                'form': '10-K',
                'accession': '0000-00-000000',
                'filing_date': '2024-12-31',
                'period_end': '2024-12-31',
                'filing_grade': 'IN_RANGE_ANNUAL',
                'in_range': True,
            },
            'classification': {
                'primary_profile': 'bank',
                'classifier_diagnostics': {'bank_gate': {'passed': True}, 'decision_rule': 'bank_hard_gate_passed'},
            },
            'mapping_ctx': {
                'confidence_by_year': {2024: {'IS.NII': 95, 'BS.ASSETS': 95, 'BS.LOANS': 95, 'BS.DEPOSITS': 95, 'BS.CET1': 95, 'BS.EQ': 95}},
                'drift_flags': {2024: []},
            },
            'normalization_ctx': {'by_year': {2024: {'currency_conflict': False, 'scale_conflict': False}}},
            'reconciliation_ctx': {2024: {'balance_sheet_ok': True}},
            'node_inputs_by_year': {},
            'company_meta': {'cik': '000001', 'ticker': 'JPMX', 'name': 'JPMX'},
        }

    def test_scale_mismatch_rejected(self):
        kwargs = dict(self.common_kwargs)
        kwargs['normalization_ctx'] = {'by_year': {2024: {'currency_conflict': False, 'scale_conflict': True}}}
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **kwargs)
        nim = out[2024]['net_interest_margin']
        self.assertEqual((nim['reliability'] or {}).get('grade'), 'REJECTED')
        self.assertIn('scale_conflict_unresolved', nim.get('reasons', []))

    def test_mapping_confidence_below_80_rejected(self):
        kwargs = dict(self.common_kwargs)
        kwargs['mapping_ctx'] = {
            'confidence_by_year': {2024: {'IS.NII': 60, 'BS.ASSETS': 95}},
            'drift_flags': {2024: []},
        }
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **kwargs)
        nim = out[2024]['net_interest_margin']
        self.assertEqual((nim['reliability'] or {}).get('grade'), 'REJECTED')
        reasons = nim.get('reasons', [])
        self.assertTrue(any(r.startswith('mapping_confidence_below_80:IS.NII') for r in reasons))

    def test_reconciliation_failure_downgrade_or_reject(self):
        kwargs = dict(self.common_kwargs)
        kwargs['reconciliation_ctx'] = {2024: {'balance_sheet_ok': False}}
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **kwargs)
        cap = out[2024]['capital_ratio_proxy']
        grade = (cap['reliability'] or {}).get('grade')
        self.assertIn(grade, ['LOW', 'REJECTED', 'MEDIUM'])
        self.assertIn('balance_sheet_reconciliation_failed', cap.get('reasons', []))

    def test_sector_specific_bounds_reflected(self):
        kwargs = dict(self.common_kwargs)
        row = dict(self.base_row)
        row['IS.NII'] = 600_000_000_000.0  # forces NIM > bank bound
        out = self.engine.compute_reliable('bank', {2024: row}, **kwargs)
        nim = out[2024]['net_interest_margin']
        self.assertEqual((nim['reliability'] or {}).get('grade'), 'REJECTED')
        self.assertIn('implausible_value', nim.get('reasons', []))
        self.assertIn('bounds_used', nim.get('diagnostics', {}))

    def test_diagnostics_snapshot_shape(self):
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **self.common_kwargs)
        nim = out[2024]['net_interest_margin']
        self.assertIn('reliability', nim)
        self.assertIn('diagnostics', nim)
        self.assertIn('inputs_used', nim)
        self.assertIn('provenance', nim)
        rel = nim['reliability']
        for key in ['grade', 'score', 'gates_passed', 'gates_failed', 'validators_run']:
            self.assertIn(key, rel)
        self.assertEqual(rel['validators_run'][0], 'phase_1_filing_provenance')
        self.assertEqual(rel['validators_run'][-1], 'phase_14_final_reliability_aggregation')


if __name__ == '__main__':
    unittest.main()
