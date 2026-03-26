import unittest

from modules.institutional.ratios import SectorRatioEngine


class ExceptionPolicyTests(unittest.TestCase):
    def setUp(self):
        self.engine = SectorRatioEngine()
        self.base_kwargs = {
            'provenance_ctx': {
                'form': '10-K',
                'accession': '0000-00-000000',
                'filing_date': '2024-12-31',
                'period_end': '2024-12-31',
                'filing_grade': 'IN_RANGE_ANNUAL',
                'in_range': True,
            },
            'classification': {
                'primary_profile': 'industrial',
                'classifier_diagnostics': {'bank_gate': {'passed': False}, 'decision_rule': 'max_score'},
            },
            'normalization_ctx': {'by_year': {2022: {'currency_conflict': False, 'scale_conflict': False}, 2023: {'currency_conflict': False, 'scale_conflict': False}, 2024: {'currency_conflict': False, 'scale_conflict': False}}},
            'reconciliation_ctx': {2022: {'balance_sheet_ok': True}, 2023: {'balance_sheet_ok': True}, 2024: {'balance_sheet_ok': True}},
            'node_inputs_by_year': {},
            'company_meta': {'cik': '000100', 'ticker': 'TEST', 'name': 'TestCo'},
        }

    def _mapping_ctx_for(self, years, required_nodes):
        return {
            'confidence_by_year': {y: {n: 95 for n in required_nodes} for y in years},
            'drift_flags': {y: [] for y in years},
        }

    def test_negative_equity_exception_downgrades_not_crash(self):
        data = {
            2024: {
                'IS.GP': 30_000_000.0,
                'IS.REV': 100_000_000.0,
                'IS.NET': -15_000_000.0,
                'BS.EQ': -20_000_000.0,
                'BS.ASSETS': 500_000_000.0,
            }
        }
        kwargs = dict(self.base_kwargs)
        kwargs['mapping_ctx'] = self._mapping_ctx_for([2024], ['IS.GP', 'IS.REV', 'BS.ASSETS'])
        out = self.engine.compute_reliable('industrial', data, **kwargs)
        gm = out[2024]['gross_margin']
        self.assertIsNotNone(gm.get('value'))
        self.assertEqual((gm.get('reliability') or {}).get('grade'), 'LOW')
        self.assertIn('negative_equity_exception', gm.get('reasons') or [])

    def test_acquisition_exception_relaxes_yoy_but_reconciliation_still_applies(self):
        data = {
            2023: {
                'IS.REV': 100_000_000.0,
                'IS.GP': 35_000_000.0,
                'BS.ASSETS': 200_000_000.0,
                'IS.NET': 5_000_000.0,
                'BS.EQ': 60_000_000.0,
            },
            2024: {
                'IS.REV': 600_000_000.0,  # >300% jump
                'IS.GP': 220_000_000.0,
                'BS.ASSETS': 1_400_000_000.0,  # >300% jump
                'IS.NET': 10_000_000.0,
                'BS.EQ': 120_000_000.0,
            },
        }
        kwargs = dict(self.base_kwargs)
        kwargs['company_meta'] = {'cik': '000100', 'ticker': 'TEST', 'name': 'TestCo', 'acquisition_evidence': True}
        kwargs['mapping_ctx'] = self._mapping_ctx_for([2023, 2024], ['IS.REV', 'BS.ASSETS', 'IS.GP'])
        kwargs['reconciliation_ctx'] = {2023: {'balance_sheet_ok': True}, 2024: {'balance_sheet_ok': False}}
        out = self.engine.compute_reliable('industrial', data, **kwargs)
        at = out[2024]['asset_turnover']
        self.assertIn('balance_sheet_reconciliation_failed', at.get('reasons') or [])
        ex = ((at.get('diagnostics') or {}).get('exceptions') or {})
        ids = [e.get('exception_id') for e in (ex.get('entries') or [])]
        self.assertIn('EX-03', ids)

    def test_early_stage_margin_exception_allows_temporary_extreme_margin(self):
        data = {
            2023: {
                'IS.REV': 20_000_000.0,
                'IS.OP': -12_000_000.0,
                'IS.NET': -8_000_000.0,
                'IS.RD': 9_000_000.0,
                'BS.ASSETS': 180_000_000.0,
                'BS.EQ': 40_000_000.0,
            },
            2024: {
                'IS.REV': 24_000_000.0,
                'IS.OP': 96_000_000.0,  # operating margin = 4.0
                'IS.NET': 7_000_000.0,  # volatility + sign change
                'IS.RD': 10_000_000.0,  # >30% revenue
                'BS.ASSETS': 210_000_000.0,
                'BS.EQ': 50_000_000.0,
            },
        }
        kwargs = dict(self.base_kwargs)
        kwargs['mapping_ctx'] = self._mapping_ctx_for([2023, 2024], ['IS.OP', 'IS.REV', 'BS.ASSETS'])
        out = self.engine.compute_reliable('industrial', data, **kwargs)
        opm = out[2024]['operating_margin']
        self.assertIn('early_stage_margin_exception', opm.get('reasons') or [])
        self.assertNotIn('implausible_value', opm.get('reasons') or [])
        self.assertIn((opm.get('reliability') or {}).get('grade'), ['LOW', 'MEDIUM'])

    def test_exception_cannot_bypass_mapping_confidence_or_scale_integrity(self):
        data = {
            2024: {
                'IS.GP': 30_000_000.0,
                'IS.REV': 100_000_000.0,
                'IS.NET': -15_000_000.0,
                'BS.EQ': -20_000_000.0,
                'BS.ASSETS': 500_000_000.0,
            }
        }
        kwargs = dict(self.base_kwargs)
        kwargs['mapping_ctx'] = {
            'confidence_by_year': {2024: {'IS.GP': 60, 'IS.REV': 95, 'BS.ASSETS': 95}},
            'drift_flags': {2024: []},
        }
        kwargs['normalization_ctx'] = {'by_year': {2024: {'currency_conflict': False, 'scale_conflict': True}}}
        out = self.engine.compute_reliable('industrial', data, **kwargs)
        gm = out[2024]['gross_margin']
        self.assertEqual((gm.get('reliability') or {}).get('grade'), 'REJECTED')
        reasons = gm.get('reasons') or []
        self.assertTrue(any(r.startswith('mapping_confidence_below_80:IS.GP') for r in reasons))
        self.assertIn('scale_conflict_unresolved', reasons)

    def test_three_consecutive_exception_years_forces_rejected(self):
        data = {
            2022: {'IS.GP': 10_000_000.0, 'IS.REV': 40_000_000.0, 'IS.NET': -6_000_000.0, 'BS.EQ': -15_000_000.0, 'BS.ASSETS': 200_000_000.0},
            2023: {'IS.GP': 11_000_000.0, 'IS.REV': 44_000_000.0, 'IS.NET': -5_000_000.0, 'BS.EQ': -12_000_000.0, 'BS.ASSETS': 220_000_000.0},
            2024: {'IS.GP': 12_000_000.0, 'IS.REV': 48_000_000.0, 'IS.NET': -4_000_000.0, 'BS.EQ': -10_000_000.0, 'BS.ASSETS': 240_000_000.0},
        }
        kwargs = dict(self.base_kwargs)
        kwargs['mapping_ctx'] = self._mapping_ctx_for([2022, 2023, 2024], ['IS.GP', 'IS.REV', 'BS.ASSETS'])
        out = self.engine.compute_reliable('industrial', data, **kwargs)
        gm = out[2024]['gross_margin']
        self.assertEqual((gm.get('reliability') or {}).get('grade'), 'REJECTED')
        self.assertIn('structural_instability_exception_persistence', gm.get('reasons') or [])


if __name__ == '__main__':
    unittest.main()
