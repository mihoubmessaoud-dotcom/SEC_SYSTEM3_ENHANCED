from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ExceptionDefinition:
    exception_id: str
    description: str
    sector_scope: List[str]
    trigger_conditions: Dict[str, object]
    override_rules: Dict[str, object]
    downgrade_rules: Dict[str, object]
    max_years_allowed: int
    documentation_required: List[str]


EXCEPTION_REGISTRY: Dict[str, ExceptionDefinition] = {
    'EX-01': ExceptionDefinition(
        exception_id='EX-01',
        description='Negative Equity (Distressed / Turnaround Companies)',
        sector_scope=['industrial', 'insurance', 'broker_dealer', 'unknown'],
        trigger_conditions={
            'sector_not_bank': True,
            'net_income_negative_at_least_one_year': True,
            'total_equity_negative': True,
        },
        override_rules={
            'allow_negative_roe': True,
            'allow_negative_pb': True,
            'company_status': 'distressed',
        },
        downgrade_rules={'reliability_cap': 'LOW', 'score_penalty': 20},
        max_years_allowed=99,
        documentation_required=['equity_line_item', 'net_income_history'],
    ),
    'EX-02': ExceptionDefinition(
        exception_id='EX-02',
        description='Extreme Margins for Early-Stage / Biotech',
        sector_scope=['industrial', 'unknown'],
        trigger_conditions={
            'revenue_lt_50m': True,
            'rd_gt_30pct_revenue': True,
            'net_income_high_volatility': True,
        },
        override_rules={'margin_bounds_override': (-5.0, 5.0)},
        downgrade_rules={'reliability_cap': 'LOW', 'score_penalty': 15},
        max_years_allowed=3,
        documentation_required=['revenue', 'r_and_d', 'net_income_series'],
    ),
    'EX-03': ExceptionDefinition(
        exception_id='EX-03',
        description='Acquisition / Merger Year Jump',
        sector_scope=['bank', 'insurance', 'broker_dealer', 'industrial', 'unknown'],
        trigger_conditions={
            'assets_or_revenue_growth_gt_300pct': True,
            'acquisition_keyword_evidence': True,
        },
        override_rules={'relax_yoy_anomaly_gate': True, 'company_status': 'structural_event'},
        downgrade_rules={'reliability_cap': 'MEDIUM', 'score_penalty': 12},
        max_years_allowed=1,
        documentation_required=['growth_trace', 'acquisition_keyword'],
    ),
    'EX-04': ExceptionDefinition(
        exception_id='EX-04',
        description='Banking Leverage Structure',
        sector_scope=['bank'],
        trigger_conditions={'bank_gate_strong': True},
        override_rules={'debt_to_equity_max': 50.0},
        downgrade_rules={'reliability_cap': 'MEDIUM', 'score_penalty': 10},
        max_years_allowed=99,
        documentation_required=['bank_gate_strength'],
    ),
    'EX-05': ExceptionDefinition(
        exception_id='EX-05',
        description='SPAC / Shell Company Phase',
        sector_scope=['industrial', 'broker_dealer', 'unknown'],
        trigger_conditions={'revenue_near_zero': True, 'assets_mostly_cash': True},
        override_rules={'suppress_profitability_ratios': True, 'company_status': 'shell/spac'},
        downgrade_rules={'reliability_cap': 'LOW', 'score_penalty': 18},
        max_years_allowed=99,
        documentation_required=['revenue', 'cash_to_assets'],
    ),
    'EX-06': ExceptionDefinition(
        exception_id='EX-06',
        description='One-time Impairment Shock',
        sector_scope=['bank', 'insurance', 'broker_dealer', 'industrial', 'unknown'],
        trigger_conditions={'impairment_gt_20pct_assets': True},
        override_rules={'allow_impairment_once': True},
        downgrade_rules={'reliability_cap': 'MEDIUM', 'score_penalty': 12},
        max_years_allowed=1,
        documentation_required=['impairment', 'assets'],
    ),
    'EX-07': ExceptionDefinition(
        exception_id='EX-07',
        description='Hyperinflation or Currency Transition',
        sector_scope=['bank', 'insurance', 'broker_dealer', 'industrial', 'unknown'],
        trigger_conditions={'currency_changed': True, 'currency_note_evidence': True},
        override_rules={'allow_rescaling_with_fx': True},
        downgrade_rules={'reliability_cap': 'MEDIUM', 'score_penalty': 10},
        max_years_allowed=99,
        documentation_required=['currency_transition', 'currency_note'],
    ),
}


class ExceptionPolicyEngine:
    """
    Deterministic exception policy for rare but legitimate anomalies.
    """

    def __init__(self) -> None:
        self.registry = EXCEPTION_REGISTRY

    def detect_yearly_exceptions(
        self,
        *,
        years: List[int],
        computed_by_year: Dict[int, Dict[str, float]],
        profile: str,
        classification: Dict,
        company_meta: Optional[Dict],
        normalization_ctx: Optional[Dict],
    ) -> Dict[int, Dict]:
        company_meta = company_meta or {}
        normalization_ctx = normalization_ctx or {}
        by_year: Dict[int, Dict] = {}
        history: Dict[str, List[int]] = {eid: [] for eid in self.registry}

        # Static evidence flags
        acq_hint = bool(company_meta.get('acquisition_evidence'))
        bank_gate_strong = bool((((classification or {}).get('classifier_diagnostics') or {}).get('bank_gate_strength') or {}).get('material_core_bank_footprint'))

        for idx, year in enumerate(years):
            row = computed_by_year.get(year, {})
            prev = computed_by_year.get(years[idx - 1], {}) if idx > 0 else {}
            entries = []
            status_flags = set()

            # EX-01: Negative equity distress
            eq = self._num(row.get('BS.EQ'))
            net_hist_neg = any(self._num(computed_by_year.get(y, {}).get('IS.NET')) is not None and self._num(computed_by_year.get(y, {}).get('IS.NET')) < 0 for y in years if y <= year)
            if profile != 'bank' and eq is not None and eq < 0 and net_hist_neg:
                entries.append(self._entry('EX-01', 'negative_equity_and_negative_net_income', year))
                status_flags.add('distressed')

            # EX-02: Early-stage extreme margins
            rev = self._num(row.get('IS.REV'))
            rd = self._num(row.get('IS.RD'))
            ni_vol = self._net_income_volatile(years=years, computed_by_year=computed_by_year, year=year)
            if rev is not None and rev < 50_000_000 and rd is not None and rev > 0 and (rd / rev) > 0.30 and ni_vol:
                entries.append(self._entry('EX-02', 'early_stage_margin_profile', year))

            # EX-03: Acquisition jump year
            rev_prev = self._num(prev.get('IS.REV'))
            assets = self._num(row.get('BS.ASSETS'))
            assets_prev = self._num(prev.get('BS.ASSETS'))
            rev_growth = self._growth(rev, rev_prev)
            assets_growth = self._growth(assets, assets_prev)
            if ((rev_growth is not None and rev_growth > 3.0) or (assets_growth is not None and assets_growth > 3.0)) and acq_hint:
                entries.append(self._entry('EX-03', 'acquisition_growth_jump', year))
                status_flags.add('structural_event')

            # EX-04: Bank leverage structure
            if profile == 'bank' and bank_gate_strong:
                entries.append(self._entry('EX-04', 'bank_leverage_structure', year))

            # EX-05: SPAC/Shell
            cash = self._num(row.get('BS.CASH'))
            cash_ratio = (cash / assets) if (cash is not None and assets is not None and abs(assets) > 1e-12) else None
            if rev is not None and abs(rev) < 1_000_000 and cash_ratio is not None and cash_ratio >= 0.80:
                entries.append(self._entry('EX-05', 'shell_spac_balance_profile', year))
                status_flags.add('shell/spac')

            # EX-06: Impairment shock
            impairment = self._num(row.get('IS.IMPAIRMENT'))
            if impairment is not None and assets is not None and abs(assets) > 1e-12 and abs(impairment) / abs(assets) > 0.20:
                entries.append(self._entry('EX-06', 'impairment_gt_20pct_assets', year))

            # EX-07: Currency transition
            norm_y = (normalization_ctx.get('by_year') or {}).get(year, {})
            currency_changed = bool(norm_y.get('currency_transition'))
            note_evidence = bool(norm_y.get('currency_note_evidence'))
            if currency_changed and note_evidence:
                entries.append(self._entry('EX-07', 'currency_transition_with_note', year))

            by_year[year] = {
                'exception_triggered': bool(entries),
                'entries': entries,
                'status_flags': sorted(status_flags),
            }
            for e in entries:
                history[e['exception_id']].append(year)

        # finalize years_active, consecutive counts, caps
        for year in years:
            entries = by_year[year]['entries']
            for e in entries:
                eid = e['exception_id']
                active_years = [y for y in history.get(eid, []) if y <= year]
                consecutive = self._consecutive_tail(history.get(eid, []), year)
                e['years_active'] = len(active_years)
                e['consecutive_years_active'] = consecutive
                max_allowed = self.registry[eid].max_years_allowed
                e['max_years_allowed'] = max_allowed
                if len(active_years) > max_allowed:
                    e['exceeded_max_years_allowed'] = True
                else:
                    e['exceeded_max_years_allowed'] = False

            entry_count = len(entries)
            force_rejected = any((e.get('consecutive_years_active', 0) >= 3) for e in entries)
            by_year[year]['exception_count'] = entry_count
            by_year[year]['cap_low_due_to_multi_exceptions'] = entry_count > 2
            by_year[year]['force_rejected_due_to_persistence'] = force_rejected
        return by_year

    def _entry(self, exception_id: str, reason: str, year: int) -> Dict:
        d = self.registry[exception_id]
        return {
            'exception_triggered': True,
            'exception_id': exception_id,
            'reason': reason,
            'impact_on_reliability': d.downgrade_rules,
            'year': year,
            'years_active': 1,
            'documentation_required': list(d.documentation_required),
            'override_rules': dict(d.override_rules),
        }

    @staticmethod
    def _num(v) -> Optional[float]:
        if isinstance(v, (int, float)):
            return float(v)
        return None

    @staticmethod
    def _growth(v: Optional[float], p: Optional[float]) -> Optional[float]:
        if v is None or p is None:
            return None
        if abs(p) < 1e-12:
            return None
        return (v - p) / abs(p)

    @staticmethod
    def _consecutive_tail(years: List[int], year: int) -> int:
        if not years:
            return 0
        s = set(years)
        k = 0
        cur = year
        while cur in s:
            k += 1
            cur -= 1
        return k

    def _net_income_volatile(self, *, years: List[int], computed_by_year: Dict[int, Dict[str, float]], year: int) -> bool:
        vals = []
        for y in years:
            if y > year:
                continue
            v = self._num(computed_by_year.get(y, {}).get('IS.NET'))
            if v is not None:
                vals.append((y, v))
        if len(vals) < 2:
            return False
        sign_changes = 0
        big_swings = 0
        for i in range(1, len(vals)):
            prev = vals[i - 1][1]
            cur = vals[i][1]
            if (prev < 0 <= cur) or (prev >= 0 > cur):
                sign_changes += 1
            if abs(prev) > 1e-12:
                yoy = (cur - prev) / abs(prev)
                if abs(yoy) > 1.0:
                    big_swings += 1
        return sign_changes >= 1 or big_swings >= 1
