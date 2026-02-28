import json
import unittest
from datetime import datetime
from pathlib import Path

from modules.sec_fetcher import SECDataFetcher


class INTCStatementAnchorReconciliationTests(unittest.TestCase):
    def test_intc_statement_anchor_reconciliation(self):
        fetcher = SECDataFetcher()
        current_year = datetime.now().year
        res = fetcher.fetch_company_data(
            company_name='INTC',
            start_year=current_year - 2,
            end_year=current_year,
            filing_type='10-K',
            callback=None,
            include_all_concepts=True,
        )
        self.assertTrue(res.get('success'), msg=f"INTC fetch failed: {res.get('error')}")

        db = res.get('data_by_year', {}) or {}
        self.assertTrue(db, msg='No yearly data returned for INTC')
        latest_year = max([y for y in db.keys() if isinstance(y, int)])
        row = db.get(latest_year, {})

        # Anchor lock must be active (no silent concept-first fallback for statement metrics)
        self.assertTrue(bool(row.get('__statement_tree_required__')), msg='Statement anchor lock was not enabled')

        # Validate diagnostics produced
        st_path = Path('exports/sector_comparison/statement_tree_diagnostics.json')
        rc_path = Path('exports/sector_comparison/sec_reconciliation_report.json')
        self.assertTrue(st_path.exists(), msg='statement_tree_diagnostics.json missing')
        self.assertTrue(rc_path.exists(), msg='sec_reconciliation_report.json missing')

        st = json.loads(st_path.read_text(encoding='utf-8'))
        rc_payload = json.loads(rc_path.read_text(encoding='utf-8'))
        if isinstance(rc_payload, dict):
            rc_rows = rc_payload.get('rows') or []
        else:
            rc_rows = rc_payload or []

        ydiag = (st.get('years') or {}).get(str(latest_year), {})
        self.assertTrue(ydiag, msg='Missing statement diagnostics for latest INTC year')
        self.assertIsNotNone(ydiag.get('anchor'), msg='Anchor context not found for INTC latest annual filing')

        by_metric = {}
        for r in rc_rows:
            if r.get('year') == latest_year:
                by_metric[r.get('metric')] = r

        # Required metrics must exist and not be fallback/no-anchor failures.
        for metric in ('annual_revenue', 'gross_profit', 'operating_income', 'net_income'):
            self.assertIn(metric, by_metric, msg=f'Missing reconciliation row for {metric}')
            self.assertNotIn(
                by_metric[metric].get('reason'),
                ('fallback_companyfacts_no_statement_anchor', 'statement_anchor_not_found'),
                msg=f'{metric} used forbidden fallback path',
            )

        # Tolerance checks
        rev = by_metric['annual_revenue']
        gp = by_metric['gross_profit']
        op = by_metric['operating_income']

        rev_mm = float(rev.get('mismatch_pct') or 0.0)
        gp_mm = float(gp.get('mismatch_pct') or 0.0)
        op_mm = float(op.get('mismatch_pct') or 0.0)

        self.assertLessEqual(rev_mm, 0.03, msg=f'INTC revenue mismatch too high: {rev_mm:.4f}')
        self.assertLessEqual(gp_mm, 0.03, msg=f'INTC gross profit mismatch too high: {gp_mm:.4f}')
        self.assertLessEqual(op_mm, 0.05, msg=f'INTC operating income mismatch too high: {op_mm:.4f}')


if __name__ == '__main__':
    unittest.main()
