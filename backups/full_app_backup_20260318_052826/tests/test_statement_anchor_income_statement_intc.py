import json
import unittest
from datetime import datetime
from pathlib import Path

from modules.sec_fetcher import SECDataFetcher
from modules.ratio_source import maybe_guard_ratios_by_year


class StatementAnchorIncomeStatementINTCTests(unittest.TestCase):
    def test_income_statement_anchor_for_intc(self):
        fetcher = SECDataFetcher()
        y = datetime.now().year
        res = fetcher.fetch_company_data(
            company_name='INTC',
            start_year=y - 2,
            end_year=y,
            filing_type='10-K',
            callback=None,
            include_all_concepts=True,
        )
        self.assertTrue(res.get('success'), msg=f"INTC fetch failed: {res.get('error')}")

        db = res.get('data_by_year', {}) or {}
        latest_year = max([k for k in db.keys() if isinstance(k, int)])
        row = db.get(latest_year, {})
        self.assertTrue(bool(row.get('__statement_tree_required__')), msg='statement tree required lock not enabled')

        st = json.loads(Path('exports/sector_comparison/statement_tree_diagnostics.json').read_text(encoding='utf-8'))
        yr = (st.get('years') or {}).get(str(latest_year), {})
        self.assertTrue(yr, msg='missing statement tree diagnostics for latest year')
        self.assertIsNotNone(yr.get('statement_role_selected') or yr.get('primary_income_role'))

        role = (yr.get('statement_role_selected') or yr.get('primary_income_role') or '').lower()
        self.assertTrue(
            any(k in role for k in ('income', 'operations', 'statementofincome', 'statementsofoperations')),
            msg=f'role selected is not income statement-like: {role}',
        )

        metrics = yr.get('metrics') or {}
        rev_info = metrics.get('annual_revenue') or {}
        rev_concept = rev_info.get('selected_parent_concept')
        self.assertIsNotNone(rev_concept, msg='revenue concept not anchored from income statement tree')
        self.assertTrue('revenue' in str(rev_concept).lower() or 'sales' in str(rev_concept).lower())

        # Ensure selected revenue concept belongs to selected income role tree nodes.
        tree_nodes = yr.get('tree_nodes') or []
        def _norm(c):
            txt = str(c or '')
            if ':' in txt:
                txt = txt.split(':', 1)[1]
            if '_' in txt:
                txt = txt.split('_', 1)[1]
            return txt
        tree_concepts = {_norm(n.get('concept_name')) for n in tree_nodes}
        self.assertIn(
            _norm(rev_concept),
            tree_concepts,
            msg='selected revenue concept not present in selected income statement tree',
        )

        # Gross margin must be canonical ratio in [0, 1]
        ratios = maybe_guard_ratios_by_year(res.get('financial_ratios', {}) or {})
        gm = ((ratios.get(latest_year) or {}).get('gross_margin'))
        self.assertIsInstance(gm, (int, float), msg='gross_margin missing')
        self.assertGreaterEqual(gm, 0.0, msg=f'gross_margin below 0: {gm}')
        self.assertLessEqual(gm, 1.0, msg=f'gross_margin above 1: {gm}')


if __name__ == '__main__':
    unittest.main()
