import unittest

from modules.institutional.ratios import SectorRatioEngine
from modules.ratio_formats import format_ratio_value


class WeightedReliabilityScoringTests(unittest.TestCase):
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

    def _kwargs(self, filing_grade='IN_RANGE_ANNUAL', mapping_conf=95, recon_ok=True):
        return {
            'provenance_ctx': {
                'form': '10-K',
                'accession': '0000-00-000000',
                'filing_date': '2024-12-31',
                'period_end': '2024-12-31',
                'filing_grade': filing_grade,
                'in_range': filing_grade != 'OUT_OF_RANGE_ANNUAL_FALLBACK',
            },
            'classification': {
                'primary_profile': 'bank',
                'classifier_diagnostics': {'bank_gate': {'passed': True}, 'decision_rule': 'bank_hard_gate_passed'},
            },
            'mapping_ctx': {
                'confidence_by_year': {2024: {'IS.NII': mapping_conf, 'BS.ASSETS': mapping_conf, 'BS.LOANS': mapping_conf, 'BS.DEPOSITS': mapping_conf, 'BS.CET1': mapping_conf, 'BS.EQ': mapping_conf}},
                'drift_flags': {2024: []},
            },
            'normalization_ctx': {'by_year': {2024: {'currency_conflict': False, 'scale_conflict': False, 'detected_scale': 1.0}}},
            'reconciliation_ctx': {2024: {'balance_sheet_ok': recon_ok, 'balance_sheet_rel_diff': 0.06 if not recon_ok else 0.0, 'cash_roll_ok': True}},
            'node_inputs_by_year': {},
            'company_meta': {'cik': '000001', 'ticker': 'JPMX', 'name': 'JPMX'},
        }

    def test_out_of_range_caps_at_low(self):
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **self._kwargs(filing_grade='OUT_OF_RANGE_ANNUAL_FALLBACK', mapping_conf=95, recon_ok=True))
        nim = out[2024]['net_interest_margin']
        rel = nim['reliability']
        self.assertEqual(rel.get('grade'), 'LOW')
        scoring = (nim.get('diagnostics') or {}).get('reliability_scoring') or {}
        self.assertIn('CAP_FIL_OOR', scoring.get('caps_applied', []))

    def test_mapping_confidence_84_penalized_not_high(self):
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **self._kwargs(mapping_conf=84))
        nim = out[2024]['net_interest_margin']
        rel = nim['reliability']
        self.assertIn(rel.get('grade'), ['MEDIUM', 'LOW'])
        self.assertLessEqual(rel.get('score', 100), 80)

    def test_reconciliation_failure_reduces_score(self):
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **self._kwargs(mapping_conf=95, recon_ok=False))
        nim = out[2024]['net_interest_margin']
        rel = nim['reliability']
        self.assertLessEqual(rel.get('score', 100), 75)
        self.assertIn('balance_sheet_reconciliation_failed', nim.get('reasons', []))

    def test_exception_caps_grade_not_high(self):
        data = {
            2024: {
                'IS.GP': 80_000_000.0,
                'IS.REV': 200_000_000.0,
                'IS.NET': -15_000_000.0,
                'BS.EQ': -20_000_000.0,
                'BS.ASSETS': 500_000_000.0,
            }
        }
        kwargs = {
            'provenance_ctx': {
                'form': '10-K',
                'accession': '0000-00-000000',
                'filing_date': '2024-12-31',
                'period_end': '2024-12-31',
                'filing_grade': 'IN_RANGE_ANNUAL',
                'in_range': True,
            },
            'classification': {'primary_profile': 'industrial', 'classifier_diagnostics': {'decision_rule': 'max_score'}},
            'mapping_ctx': {'confidence_by_year': {2024: {'IS.GP': 95, 'IS.REV': 95}}, 'drift_flags': {2024: []}},
            'normalization_ctx': {'by_year': {2024: {'currency_conflict': False, 'scale_conflict': False, 'detected_scale': 1.0}}},
            'reconciliation_ctx': {2024: {'balance_sheet_ok': True}},
            'node_inputs_by_year': {},
            'company_meta': {'cik': '000001', 'ticker': 'DSTR', 'name': 'Distressed'},
        }
        out = self.engine.compute_reliable('industrial', data, **kwargs)
        gm = out[2024]['gross_margin']
        rel = gm['reliability']
        self.assertIn(rel.get('grade'), ['MEDIUM', 'LOW'])
        self.assertNotEqual(rel.get('grade'), 'HIGH')
        scoring = (gm.get('diagnostics') or {}).get('reliability_scoring') or {}
        self.assertIn('CAP_EX_ANY', scoring.get('caps_applied', []))

    def test_high_requires_all_strict_conditions(self):
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **self._kwargs(mapping_conf=95, recon_ok=True))
        cap = out[2024]['capital_ratio_proxy']
        self.assertEqual((cap['reliability'] or {}).get('grade'), 'HIGH')

    def test_scoring_breakdown_always_present(self):
        out = self.engine.compute_reliable('bank', {2024: dict(self.base_row)}, **self._kwargs(mapping_conf=95, recon_ok=True))
        nim = out[2024]['net_interest_margin']
        scoring = (nim.get('diagnostics') or {}).get('reliability_scoring') or {}
        self.assertIn('penalties_breakdown', scoring)
        pb = scoring.get('penalties_breakdown') or {}
        self.assertIn('base_score', pb)
        self.assertIn('penalties', pb)
        self.assertIn('caps', pb)
        self.assertIn('final_score', pb)
        self.assertIn('final_grade', pb)

    def test_canonical_fraction_and_display_percent(self):
        row = {
            'IS.GP': 43.81e9,
            'IS.REV': 79.02e9,
            'IS.OP': 10.0e9,
            'BS.ASSETS': 100.0e9,
            'IS.NET': 5.0e9,
            'BS.EQ': 40.0e9,
        }
        kwargs = {
            'provenance_ctx': {'form': '10-K', 'accession': 'x', 'filing_date': '2024-12-31', 'period_end': '2024-12-31', 'filing_grade': 'IN_RANGE_ANNUAL', 'in_range': True},
            'classification': {'primary_profile': 'industrial', 'classifier_diagnostics': {'decision_rule': 'max_score'}},
            'mapping_ctx': {'confidence_by_year': {2024: {'IS.GP': 95, 'IS.REV': 95, 'IS.OP': 95, 'BS.ASSETS': 95}}, 'drift_flags': {2024: []}},
            'normalization_ctx': {'by_year': {2024: {'currency_conflict': False, 'scale_conflict': False, 'detected_scale': 1.0}}},
            'reconciliation_ctx': {2024: {'balance_sheet_ok': True}},
            'node_inputs_by_year': {},
            'company_meta': {'cik': '1', 'ticker': 'X', 'name': 'X'},
        }
        out = self.engine.compute_reliable('industrial', {2024: row}, **kwargs)
        gm = out[2024]['gross_margin']
        self.assertAlmostEqual(gm.get('value'), 0.5544166, places=6)
        fmt = format_ratio_value('gross_margin', gm.get('value'))
        self.assertAlmostEqual(fmt.get('display_value'), 55.44166, places=5)
        self.assertIn('%', fmt.get('display_text', ''))

    def test_double_percent_guard_explicit(self):
        canonical = 0.5544166
        fmt = format_ratio_value('gross_margin', canonical)
        display = float(fmt.get('display_value'))
        self.assertAlmostEqual(display, canonical * 100.0, places=6)
        self.assertNotAlmostEqual(display, canonical * 100.0 * 100.0, places=3)

    def test_formatter_fail_closed_for_extreme_percent(self):
        fmt = format_ratio_value('gross_margin', 9912.0)
        self.assertIsNone(fmt.get('display_value'))
        self.assertEqual(fmt.get('format_rejection_reason'), 'percent_out_of_range')

    def test_percent_guardrail_rejects_9912_percent(self):
        row = {
            'IS.GP': 9912.0,
            'IS.REV': 100.0,
            'IS.OP': 100.0,
            'BS.ASSETS': 1000.0,
            'IS.NET': 10.0,
            'BS.EQ': 400.0,
        }
        kwargs = {
            'provenance_ctx': {'form': '10-K', 'accession': 'x', 'filing_date': '2024-12-31', 'period_end': '2024-12-31', 'filing_grade': 'IN_RANGE_ANNUAL', 'in_range': True},
            'classification': {'primary_profile': 'industrial', 'classifier_diagnostics': {'decision_rule': 'max_score'}},
            'mapping_ctx': {'confidence_by_year': {2024: {'IS.GP': 95, 'IS.REV': 95}}, 'drift_flags': {2024: []}},
            'normalization_ctx': {'by_year': {2024: {'currency_conflict': False, 'scale_conflict': False, 'detected_scale': 1.0}}},
            'reconciliation_ctx': {2024: {'balance_sheet_ok': True}},
            'node_inputs_by_year': {},
            'company_meta': {'cik': '1', 'ticker': 'X', 'name': 'X'},
        }
        out = self.engine.compute_reliable('industrial', {2024: row}, **kwargs)
        gm = out[2024]['gross_margin']
        self.assertEqual((gm.get('reliability') or {}).get('grade'), 'REJECTED')
        self.assertIn('percent_ratio_guardrail_exceeded', gm.get('reasons') or [])
        self.assertIn('percent_out_of_range', gm.get('reasons') or [])


if __name__ == '__main__':
    unittest.main()
