from __future__ import annotations

import hashlib
import json
from typing import Dict, List, Optional, Tuple

from ..ratio_formats import format_ratio_value, get_ratio_metadata
from .plausibility_bounds import get_bounds


class RatioValidationPipeline:
    """
    Ordered, fail-closed validation pipeline for ratio outputs.
    """

    ABS_MIN_USD = 10_000_000.0
    REL_MIN = 0.002
    REQUIRED_MAPPING_CONFIDENCE = 80.0
    DENOM_EPS = 1e-12

    PHASES = [
        'phase_1_filing_provenance',
        'phase_2_raw_extraction_sanity',
        'phase_3_unit_scale_currency_normalization',
        'phase_4_mapping_confidence_drift',
        'phase_5_materiality_scaling',
        'phase_6_sector_classification',
        'phase_7_coverage_confidence',
        'phase_8_ratio_computation',
        'phase_9_sector_gating',
        'phase_10_plausibility_bounds',
        'phase_11_cross_statement_reconciliation',
        'phase_12_multi_year_consistency',
        'phase_13_robust_anomaly_detection',
        'phase_14_final_reliability_aggregation',
    ]

    def empty_contract(
        self,
        *,
        ratio_id: str,
        provenance: Dict,
        diagnostics_extra: Optional[Dict] = None,
    ) -> Dict:
        out = {
            'value': None,
            'reliability': {
                'grade': 'REJECTED',
                'score': 0,
                'gates_passed': [],
                'gates_failed': [],
                'validators_run': [],
            },
            'reasons': [],
            'diagnostics': diagnostics_extra or {},
            'inputs_used': {},
            'provenance': provenance,
            'ratio_id': ratio_id,
        }
        return out

    def run_pipeline(
        self,
        *,
        ratio_id: str,
        profile: str,
        ratio_def: Dict,
        year: int,
        row: Dict[str, float],
        prev: Optional[Dict[str, float]],
        compute_value_fn,
        inputs_used: Dict[str, Dict],
        provenance: Dict,
        classification: Dict,
        mapping_ctx: Dict,
        normalization_ctx: Dict,
        reconciliation_ctx: Dict,
        series_ctx: Dict,
        exception_ctx: Dict,
    ) -> Dict:
        out = self.empty_contract(
            ratio_id=ratio_id,
            provenance=self._with_hash(provenance, inputs_used),
            diagnostics_extra={'phase_order': list(self.PHASES)},
        )
        out['inputs_used'] = inputs_used or {}
        out['diagnostics']['ratio_definition'] = ratio_def
        out['diagnostics']['year'] = year
        out['diagnostics']['profile'] = profile
        out['diagnostics']['ratio_metadata'] = get_ratio_metadata(ratio_id)
        out['diagnostics']['exceptions'] = {
            'exception_triggered': bool((exception_ctx or {}).get('exception_triggered')),
            'entries': list((exception_ctx or {}).get('entries') or []),
            'status_flags': list((exception_ctx or {}).get('status_flags') or []),
            'exception_count': int((exception_ctx or {}).get('exception_count') or 0),
            'cap_low_due_to_multi_exceptions': bool((exception_ctx or {}).get('cap_low_due_to_multi_exceptions')),
            'force_rejected_due_to_persistence': bool((exception_ctx or {}).get('force_rejected_due_to_persistence')),
        }

        required_nodes = list(ratio_def.get('required_nodes') or ratio_def.get('dependency_graph') or [])
        supporting_nodes = list(ratio_def.get('supporting_nodes') or [])
        out['diagnostics']['required_nodes'] = required_nodes
        out['diagnostics']['supporting_nodes'] = supporting_nodes

        # Phase 1
        self._mark_run(out, self.PHASES[0])
        filing_grade = str((provenance or {}).get('filing_grade') or '').upper()
        out_of_range = filing_grade == 'OUT_OF_RANGE_ANNUAL_FALLBACK'
        out['diagnostics']['filing'] = {'filing_grade': filing_grade, 'out_of_range': out_of_range}
        if out_of_range:
            out['reasons'].append('out_of_range_filing')
            out['diagnostics']['max_ratio_grade_cap'] = 'LOW'
            self._pass(out, 'out_of_range_filing_cap_applied')
        else:
            self._pass(out, 'filing_in_range_or_equivalent')

        # Phase 2
        self._mark_run(out, self.PHASES[1])
        p2_ok = True
        required_currencies = set()
        for node_id in required_nodes:
            inp = inputs_used.get(node_id) or {}
            val = inp.get('value')
            if val is None:
                continue
            if not self._is_finite(val):
                p2_ok = False
                self._fail(out, f'non_finite:{node_id}')
            unit = inp.get('unit')
            currency = inp.get('currency')
            if not unit or not currency:
                p2_ok = False
                self._fail(out, f'missing_unit_or_currency:{node_id}')
            if currency:
                required_currencies.add(str(currency))
            if isinstance(val, (int, float)) and abs(float(val)) > 1e15 and profile != 'bank':
                p2_ok = False
                self._fail(out, f'absurd_magnitude:{node_id}')
            period_type = str(inp.get('period_type') or '').lower()
            if period_type:
                expected = 'instant' if str(node_id).startswith('BS.') else 'duration'
                if period_type != expected:
                    p2_ok = False
                    self._fail(out, 'period_type_mismatch')
        if len(required_currencies) > 1:
            p2_ok = False
            self._fail(out, 'currency_mismatch_required_inputs')
        if p2_ok:
            self._pass(out, 'raw_extraction_sanity_ok')

        # Phase 3
        self._mark_run(out, self.PHASES[2])
        norm_y = (normalization_ctx or {}).get('by_year', {}).get(year, {})
        out['diagnostics']['normalization'] = norm_y
        if norm_y.get('currency_conflict'):
            self._fail(out, 'currency_conflict_unresolved')
        if norm_y.get('scale_conflict'):
            self._fail(out, 'scale_conflict_unresolved')
        if not norm_y.get('currency_conflict') and not norm_y.get('scale_conflict'):
            self._pass(out, 'normalization_consistent')

        # Phase 4
        self._mark_run(out, self.PHASES[3])
        map_conf_y = (mapping_ctx or {}).get('confidence_by_year', {}).get(year, {})
        drift_y = (mapping_ctx or {}).get('drift_flags', {}).get(year, [])
        out['diagnostics']['mapping_confidence'] = {
            'per_node': {n: map_conf_y.get(n, 0.0) for n in required_nodes + supporting_nodes},
            'required_min': self.REQUIRED_MAPPING_CONFIDENCE,
            'drift_flags': drift_y,
        }
        req_scores = [map_conf_y.get(n, 0.0) for n in required_nodes] if required_nodes else []
        mc_min = min(req_scores) if req_scores else 100.0
        mc_avg = (sum(req_scores) / len(req_scores)) if req_scores else 100.0
        out['diagnostics']['mapping_confidence']['required_min_score'] = mc_min
        out['diagnostics']['mapping_confidence']['required_avg_score'] = mc_avg
        low_conf = [n for n in required_nodes if map_conf_y.get(n, 0.0) < self.REQUIRED_MAPPING_CONFIDENCE]
        if low_conf:
            for n in low_conf:
                self._fail(out, f'mapping_confidence_below_80:{n}')
        else:
            self._pass(out, 'mapping_confidence_gate_pass')
        if drift_y:
            self._pass(out, 'mapping_drift_detected')

        # Phase 5
        self._mark_run(out, self.PHASES[4])
        scale_base = self._scale_base(row)
        out['diagnostics']['materiality'] = {
            'abs_min_usd': self.ABS_MIN_USD,
            'rel_min': self.REL_MIN,
            'scale_base': scale_base,
        }
        if ratio_id in {'net_interest_margin', 'loan_to_deposit_ratio', 'capital_ratio_proxy'}:
            deposits = self._num(row.get('BS.DEPOSITS'))
            nii = self._num(row.get('IS.NII'))
            dep_ok = self._is_material(deposits, scale_base)
            nii_ok = self._is_material(nii, scale_base)
            if not dep_ok:
                self._fail(out, 'deposits_not_material')
            if not nii_ok and ratio_id == 'net_interest_margin':
                self._fail(out, 'nii_not_material')
            if dep_ok and (nii_ok or ratio_id != 'net_interest_margin'):
                self._pass(out, 'materiality_gate_pass')
        else:
            self._pass(out, 'materiality_not_required_for_ratio')

        # Phase 6
        self._mark_run(out, self.PHASES[5])
        primary_profile = str((classification or {}).get('primary_profile') or profile)
        diag = (classification or {}).get('classifier_diagnostics', {})
        bank_gate = ((diag or {}).get('bank_gate') or {}).get('passed')
        out['diagnostics']['classification'] = {
            'primary_profile': primary_profile,
            'decision_rule': (diag or {}).get('decision_rule'),
            'bank_gate_passed': bank_gate,
        }
        self._pass(out, 'classification_loaded')

        # Phase 7
        self._mark_run(out, self.PHASES[6])
        missing_required = [n for n in required_nodes if inputs_used.get(n, {}).get('value') is None]
        coverage = 0.0 if not required_nodes else (len(required_nodes) - len(missing_required)) / len(required_nodes)
        out['diagnostics']['coverage'] = {
            'required_nodes': required_nodes,
            'missing_required_nodes': missing_required,
            'coverage_score': coverage,
        }
        if missing_required:
            for n in missing_required:
                self._fail(out, f'missing_required_input:{n}')
        else:
            self._pass(out, 'coverage_gate_pass')

        # Phase 8
        self._mark_run(out, self.PHASES[7])
        if not self._has_blocking_failures(out):
            value, comp_diag = compute_value_fn(row=row, prev=prev, ratio_id=ratio_id)
            out['diagnostics']['formula_trace'] = comp_diag
            if value is None:
                self._fail(out, 'missing_required_inputs_or_denominator_near_zero')
            elif not self._is_finite(value):
                self._fail(out, 'non_finite_value')
            else:
                # explicit denominator near-zero and sign checks
                if comp_diag.get('denominator_near_zero'):
                    self._fail(out, 'denominator_near_zero')
                if comp_diag.get('sign_inconsistency'):
                    self._fail(out, 'sign_inconsistency')
                if not self._has_blocking_failures(out):
                    out['value'] = float(value)
                    self._pass(out, 'ratio_computation_pass')
                    meta = get_ratio_metadata(ratio_id)
                    ex_ids = {e.get('exception_id') for e in ((exception_ctx or {}).get('entries') or [])}
                    allow_extreme = ('EX-02' in ex_ids and ratio_id in {'gross_margin', 'operating_margin', 'net_margin'})
                    if meta['ratio_format'] == 'percent' and abs(out['value']) > 2.0 and not allow_extreme:
                        self._fail(out, 'percent_out_of_range')
                        self._fail(out, 'percent_ratio_guardrail_exceeded')

        # Phase 9
        self._mark_run(out, self.PHASES[8])
        bank_only = ratio_id in {'net_interest_margin', 'loan_to_deposit_ratio', 'capital_ratio_proxy'}
        if bank_only and primary_profile != 'bank':
            self._fail(out, 'sector_not_bank')
            if primary_profile == 'insurance':
                self._fail(out, 'ratio_not_applicable_insurance')
                self._fail(out, 'insurance_company')
        if primary_profile == 'unknown' and profile != 'unknown':
            self._fail(out, 'unknown_sector_profile_restriction')

        # Phase 10
        self._mark_run(out, self.PHASES[9])
        bounds = get_bounds(primary_profile, ratio_id)
        bounds = self._apply_exception_bounds_override(
            ratio_id=ratio_id,
            profile=primary_profile,
            bounds=bounds,
            exception_ctx=exception_ctx or {},
            out=out,
        )
        out['diagnostics']['bounds_used'] = {'profile': primary_profile, 'ratio': ratio_id, 'bounds': bounds}
        if out.get('value') is not None and bounds is not None:
            lo, hi = bounds
            v = out['value']
            if v < lo or v > hi:
                self._fail(out, 'implausible_value')
            elif v == lo or v == hi:
                self._pass(out, 'value_on_plausibility_bound')
                out['diagnostics']['bound_edge_hit'] = True
            else:
                span = hi - lo
                if span > 0:
                    d = min(abs(v - lo), abs(hi - v))
                    if (d / span) <= 0.05:
                        out['diagnostics']['bound_near_edge_hit'] = True

        # Phase 11
        self._mark_run(out, self.PHASES[10])
        rec_y = (reconciliation_ctx or {}).get(year, {})
        out['diagnostics']['reconciliation'] = rec_y
        if rec_y.get('mvfs_fail'):
            self._fail(out, 'MVFS_FAIL')
        if not rec_y.get('balance_sheet_ok', True):
            self._fail(out, 'balance_sheet_reconciliation_failed')
        else:
            self._pass(out, 'reconciliation_pass')

        # Phase 12
        self._mark_run(out, self.PHASES[11])
        yflags = (series_ctx or {}).get('yoy_flags', {}).get((ratio_id, year), [])
        ex_ids = {e.get('exception_id') for e in ((exception_ctx or {}).get('entries') or [])}
        if 'EX-03' in ex_ids:
            out['diagnostics']['yoy_flags'] = yflags
            out['diagnostics']['yoy_relaxed_by_exception'] = True
            self._pass(out, 'multi_year_consistency_relaxed_by_ex03')
        else:
            out['diagnostics']['yoy_flags'] = yflags
            out['diagnostics']['yoy_relaxed_by_exception'] = False
        if yflags and 'EX-03' not in ex_ids:
            for f in yflags:
                self._fail(out, f'yoy_flag:{f}')
        elif 'EX-03' not in ex_ids:
            self._pass(out, 'multi_year_consistency_pass')

        # Phase 13
        self._mark_run(out, self.PHASES[12])
        zdiag = (series_ctx or {}).get('robust_z', {}).get((ratio_id, year), {})
        out['diagnostics']['anomaly'] = zdiag
        if zdiag.get('flagged'):
            self._fail(out, 'robust_z_outlier')
        else:
            self._pass(out, 'anomaly_check_pass')

        # Phase 14
        self._mark_run(out, self.PHASES[13])
        fmt_dbg = format_ratio_value(ratio_id, out.get('value'))
        out['diagnostics']['ratio_debug'] = {
            'raw_inputs': dict(out.get('inputs_used') or {}),
            'stored_ratio_value_canonical': fmt_dbg.get('canonical_value'),
            'formatted_display_value': fmt_dbg.get('display_value'),
            'formatted_display_text': fmt_dbg.get('display_text'),
            'display_suffix': fmt_dbg.get('display_suffix'),
            'formatter_path_used': fmt_dbg.get('formatter_path'),
            'format_rejection_reason': fmt_dbg.get('format_rejection_reason'),
            'ratio_format': fmt_dbg.get('ratio_format'),
            'ratio_unit': fmt_dbg.get('ratio_unit'),
            'ratio_display_multiplier': fmt_dbg.get('ratio_display_multiplier'),
        }
        self._finalize(out, out_of_range=out_of_range, exception_ctx=exception_ctx or {}, ratio_id=ratio_id)
        return out

    def _with_hash(self, provenance: Dict, inputs_used: Dict[str, Dict]) -> Dict:
        prov = dict(provenance or {})
        key = {
            'cik': prov.get('cik'),
            'ticker': prov.get('ticker'),
            'company_name': prov.get('company_name'),
            'form': prov.get('form'),
            'accession': prov.get('accession'),
            'filing_date': prov.get('filing_date'),
            'period_end': prov.get('period_end'),
            'in_range': prov.get('in_range'),
            'filing_grade': prov.get('filing_grade'),
            'inputs': sorted(
                (
                    n,
                    (v or {}).get('source_tag'),
                    (v or {}).get('value'),
                    (v or {}).get('context_id'),
                    (v or {}).get('unit'),
                )
                for n, v in (inputs_used or {}).items()
            ),
        }
        prov['hash_of_sources'] = hashlib.sha256(json.dumps(key, sort_keys=True, default=str).encode('utf-8')).hexdigest()
        prov['xbrl_context_ids'] = sorted({str((v or {}).get('context_id') or '') for v in (inputs_used or {}).values() if (v or {}).get('context_id')})
        prov.setdefault('extraction_method', 'companyfacts_mapped')
        return prov

    def _finalize(self, out: Dict, *, out_of_range: bool, exception_ctx: Dict, ratio_id: str) -> None:
        failed = out['reliability']['gates_failed']
        reasons = out.get('reasons', [])
        diag = out.get('diagnostics', {})
        filing_grade = ((diag.get('filing') or {}).get('filing_grade') or (out.get('provenance') or {}).get('filing_grade') or '').upper()

        # Exception policy (strict + explicit + deterministic)
        ex_entries = list((exception_ctx or {}).get('entries') or [])
        ex_ids = [e.get('exception_id') for e in ex_entries if e.get('exception_id')]
        for ex_id in ex_ids:
            if ex_id == 'EX-01':
                reasons.append('negative_equity_exception')
            elif ex_id == 'EX-02':
                reasons.append('early_stage_margin_exception')
            elif ex_id == 'EX-03':
                reasons.append('acquisition_structural_event_exception')
            elif ex_id == 'EX-04':
                reasons.append('banking_leverage_structure_exception')
            elif ex_id == 'EX-05':
                reasons.append('shell_spac_exception')
            elif ex_id == 'EX-06':
                reasons.append('impairment_shock_exception')
            elif ex_id == 'EX-07':
                reasons.append('currency_transition_exception')

        hard_reject = self._has_hard_reject(reasons)

        # EX-05 suppress profitability ratios
        if 'EX-05' in ex_ids and ratio_id in {
            'gross_margin', 'operating_margin', 'net_margin', 'fcf_margin',
            'net_income_to_assets', 'returns_on_assets_proxy',
        }:
            hard_reject = True
            reasons.append('profitability_ratio_suppressed_for_shell_spac')
        scoring = self.compute_reliability_score(
            validations={
                'reasons': list(reasons),
                'hard_reject': hard_reject,
                'value_present': out.get('value') is not None,
                'required_nodes': list(diag.get('required_nodes') or []),
                'inputs_used': dict(out.get('inputs_used') or {}),
                'yoy_flags': list(diag.get('yoy_flags') or []),
                'reconciliation': dict(diag.get('reconciliation') or {}),
                'normalization': dict(diag.get('normalization') or {}),
                'mapping_confidence': dict(diag.get('mapping_confidence') or {}),
                'bound_edge_hit': bool(diag.get('bound_edge_hit')),
                'bound_near_edge_hit': bool(diag.get('bound_near_edge_hit')),
                'classification': dict(diag.get('classification') or {}),
                'anomaly': dict(diag.get('anomaly') or {}),
                'materiality': dict(diag.get('materiality') or {}),
                'drift_flags': list((diag.get('mapping_confidence') or {}).get('drift_flags') or []),
            },
            provenance=out.get('provenance') or {},
            filing_grade=filing_grade,
            mapping_confidence=(diag.get('mapping_confidence') or {}).get('required_min_score', 100.0),
            flags={
                'fuzzy_tag_match_used': bool((diag.get('mapping_confidence') or {}).get('fuzzy_tag_match_used')),
                'label_match_weak': bool((diag.get('mapping_confidence') or {}).get('label_match_weak')),
                'statement_role_uncertain': bool((diag.get('mapping_confidence') or {}).get('statement_role_uncertain')),
            },
            exceptions=exception_ctx or {},
        )

        grade = scoring['grade']
        score = scoring['score']
        if 'CAP_EXCEPTION_PERSISTENCE' in (scoring.get('caps_applied') or []):
            reasons.append('structural_instability_exception_persistence')
        if grade == 'REJECTED':
            out['value'] = None
            if not reasons:
                reasons.append('validation_failed')
            if out.get('value') is None and 'value_null_rejected' not in reasons:
                reasons.append('value_null_rejected')

        out['reliability']['grade'] = grade
        out['reliability']['score'] = max(0, min(100, int(score)))
        out['diagnostics']['reliability_scoring'] = scoring
        out['reasons'] = self._dedup(reasons)

    def compute_reliability_score(
        self,
        validations: Dict,
        provenance: Dict,
        filing_grade: str,
        mapping_confidence: float,
        flags: Dict,
        exceptions: Dict,
    ) -> Dict:
        base_score = 100
        penalties = []
        caps = []
        reasons = validations.get('reasons') or []

        def penalize(pid: str, label: str, delta: int, evidence):
            if delta <= 0:
                return
            penalties.append({'id': pid, 'label': label, 'delta': -int(delta), 'evidence': evidence})

        def cap(cid: str, label: str, max_score: int, applied: bool):
            caps.append({'id': cid, 'label': label, 'max_score': int(max_score), 'applied': bool(applied)})

        # Hard rejection rules
        hard_reject = bool(validations.get('hard_reject'))
        if self._has_hard_reject(reasons):
            hard_reject = True
        if hard_reject:
            return {
                'score': 0,
                'grade': 'REJECTED',
                'penalties_breakdown': {
                    'base_score': base_score,
                    'penalties': penalties,
                    'caps': caps,
                    'final_score': 0,
                    'final_grade': 'REJECTED',
                    'forced_rejection': True,
                },
                'caps_applied': [],
            }

        score = float(base_score)

        # A) Filing penalties
        fg = str(filing_grade or '').upper()
        if fg == 'IN_RANGE_EQUIVALENT':
            penalize('A_FIL_EQ', 'IN_RANGE_EQUIVALENT', 10, {'filing_grade': fg})
            score -= 10
        elif fg == 'OUT_OF_RANGE_ANNUAL_FALLBACK':
            penalize('A_FIL_OOR', 'OUT_OF_RANGE_ANNUAL_FALLBACK', 30, {'filing_grade': fg})
            score -= 30

        # B) Mapping confidence penalties
        mc = float(mapping_confidence or 0.0)
        if mc < 85:
            penalize('B_MC_LT85', 'mapping_confidence < 85', 20, {'mapping_confidence': mc})
            score -= 20
        elif mc < 90:
            penalize('B_MC_LT90', 'mapping_confidence < 90', 10, {'mapping_confidence': mc})
            score -= 10
        if flags.get('fuzzy_tag_match_used'):
            penalize('B_FUZZY', 'fuzzy tag match used', 10, flags)
            score -= 10
        if flags.get('label_match_weak'):
            penalize('B_WEAK_LABEL', 'label match weak', 5, flags)
            score -= 5
        if flags.get('statement_role_uncertain'):
            penalize('B_ROLE_UNCERT', 'statement role uncertain', 10, flags)
            score -= 10

        # C) Context & period penalties
        inputs = validations.get('inputs_used') or {}
        period_ends = sorted({str((v or {}).get('period_end') or '') for v in inputs.values() if (v or {}).get('period_end')})
        if len(period_ends) > 1:
            penalize('C_PERIOD_MISMATCH', 'period_end mismatch across statements', 15, {'period_ends': period_ends})
            score -= 15
        if any('segment' in str((v or {}).get('context_id') or '').lower() or 'axis' in str((v or {}).get('context_id') or '').lower() for v in inputs.values()):
            penalize('C_NON_CONSOL', 'used non-consolidated dimension fact', 10, {'context_ids': [str((v or {}).get('context_id')) for v in inputs.values()]})
            score -= 10
        if any(abs(float((v or {}).get('fiscal_duration_days') or 365) - 365) > 5 for v in inputs.values() if (v or {}).get('fiscal_duration_days') is not None):
            penalize('C_FY_NOT_CLEAN', 'FY duration not clean', 10, {})
            score -= 10

        # D) Unit/Scale penalties
        norm = validations.get('normalization') or {}
        if norm.get('detected_scale', 1.0) != 1.0:
            penalize('D_SCALE_INFERRED', 'scale inferred', 10, {'detected_scale': norm.get('detected_scale')})
            score -= 10
        if norm.get('scale_conflict_resolved'):
            penalize('D_SCALE_CONFLICT_RESOLVED', 'minor scale conflict resolved', 15, norm)
            score -= 15
        if any(f.get('flag') in {'unit_changed', 'scale_changed'} for f in (validations.get('drift_flags') or []) if isinstance(f, dict)):
            penalize('D_SCALE_DRIFT_RESOLVED', 'scale drift across years (resolved)', 10, {'drift_flags': validations.get('drift_flags')})
            score -= 10

        # E) Reconciliation penalties
        rec = validations.get('reconciliation') or {}
        rel_diff = float(rec.get('balance_sheet_rel_diff') or 0.0)
        tol = 0.05
        if rec.get('balance_sheet_ok', True) and rel_diff >= (0.8 * tol):
            penalize('E_BS_NEAR_TOL', 'BS identity near tolerance edge', 10, rec)
            score -= 10
        if not rec.get('balance_sheet_ok', True):
            penalize('E_BS_FAIL_MILD', 'BS identity fails mildly', 25, rec)
            score -= 25
        if rec.get('cash_roll_ok') is False:
            penalize('E_CASH_ROLL_FAIL', 'cash roll-forward fails mildly', 15, rec)
            score -= 15
        if rec.get('gross_profit_identity_fail'):
            penalize('E_GP_IDENTITY_FAIL', 'gross profit identity fails', 15, rec)
            score -= 15

        # F) Multi-year consistency penalties
        yoy_flags = validations.get('yoy_flags') or []
        if len(yoy_flags) >= 2:
            penalize('F_YOY_2P', '2+ YoY flags', 20, {'yoy_flags': yoy_flags})
            score -= 20
        elif len(yoy_flags) == 1:
            penalize('F_YOY_1', '1 YoY flag', 10, {'yoy_flags': yoy_flags})
            score -= 10
        if any(str(x).startswith('definition_stability') for x in yoy_flags):
            penalize('F_DEF_STABILITY', 'definition stability warning', 10, {'yoy_flags': yoy_flags})
            score -= 10

        # G) Anomaly penalties
        an = validations.get('anomaly') or {}
        if an.get('flagged'):
            penalize('G_ROBUST_Z', 'robust_z > 6 flagged', 10, an)
            score -= 10
            z = abs(float(an.get('z') or 0.0))
            if z > 10:
                penalize('G_EXTREME_ANOM', 'extreme anomaly flagged', 20, an)
                score -= 20

        # H) Materiality penalties
        mat = validations.get('materiality') or {}
        # indicator near threshold captured by deposits/nii gates when present
        if any(r in {'deposits_not_material', 'nii_not_material'} for r in reasons):
            penalize('H_IND_BARELY_MAT', 'indicator barely material', 5, reasons)
            score -= 5
        if any(r.startswith('materiality_near_threshold:') for r in reasons):
            penalize('H_RATIO_INPUT_BARELY_MAT', 'ratio inputs barely material', 10, reasons)
            score -= 10

        # I) Exception penalties
        ex_entries = list((exceptions or {}).get('entries') or [])
        ex_status = list((exceptions or {}).get('status_flags') or [])
        if ex_entries:
            penalize('I_EXCEPTION', 'exception triggered', 15, {'exceptions': ex_entries})
            score -= 15
        if 'structural_event' in ex_status:
            penalize('I_STRUCT_EVENT', 'structural_event (M&A year)', 10, {'status_flags': ex_status})
            score -= 10
        if 'distressed' in ex_status:
            penalize('I_DISTRESSED', 'distressed flag', 10, {'status_flags': ex_status})
            score -= 10

        # J) Bounds edge penalties
        if validations.get('bound_edge_hit'):
            penalize('J_BOUND_EQ', 'value equals bound exactly', 10, {})
            score -= 10
        elif validations.get('bound_near_edge_hit'):
            penalize('J_BOUND_NEAR', 'value within 5% of bound', 5, {})
            score -= 5

        # Caps after penalties
        cap_max = 100
        applied_caps = []
        if fg == 'IN_RANGE_EQUIVALENT':
            cap('CAP_FIL_EQ', 'filing grade cap for IN_RANGE_EQUIVALENT', 85, True)
            cap_max = min(cap_max, 85)
            applied_caps.append('CAP_FIL_EQ')
        elif fg == 'OUT_OF_RANGE_ANNUAL_FALLBACK':
            cap('CAP_FIL_OOR', 'filing grade cap for OUT_OF_RANGE_ANNUAL_FALLBACK', 60, True)
            cap_max = min(cap_max, 60)
            applied_caps.append('CAP_FIL_OOR')

        if ex_entries:
            cap('CAP_EX_ANY', 'exception cap any', 75, True)
            cap_max = min(cap_max, 75)
            applied_caps.append('CAP_EX_ANY')
            ex_caps = []
            for e in ex_entries:
                impact = e.get('impact_on_reliability') or {}
                c = str(impact.get('reliability_cap') or '').upper()
                if c in {'MEDIUM', 'LOW'}:
                    ex_caps.append(c)
            if 'LOW' in ex_caps:
                cap('CAP_EX_LOW', 'exception explicit LOW cap', 60, True)
                cap_max = min(cap_max, 60)
                applied_caps.append('CAP_EX_LOW')
            elif 'MEDIUM' in ex_caps:
                cap('CAP_EX_MED', 'exception explicit MEDIUM cap', 75, True)
                cap_max = min(cap_max, 75)
                applied_caps.append('CAP_EX_MED')
        if int((exceptions or {}).get('exception_count') or 0) >= 2:
            cap('CAP_EX_MULTI', '2+ exceptions same year cap', 60, True)
            cap_max = min(cap_max, 60)
            applied_caps.append('CAP_EX_MULTI')
        if bool((exceptions or {}).get('cap_low_due_to_multi_exceptions')):
            cap('CAP_EX_MULTI_CTX', 'context multi-exception LOW cap', 60, True)
            cap_max = min(cap_max, 60)
            applied_caps.append('CAP_EX_MULTI_CTX')

        drift_flags = validations.get('drift_flags') or []
        if drift_flags:
            cap('CAP_DRIFT', 'mapping drift cap', 75, True)
            cap_max = min(cap_max, 75)
            applied_caps.append('CAP_DRIFT')
            if mc < 90:
                return {
                    'score': 0,
                    'grade': 'REJECTED',
                    'penalties_breakdown': {
                        'base_score': base_score,
                        'penalties': penalties,
                        'caps': caps + [{'id': 'CAP_DRIFT_MC_REJECT', 'label': 'drift + confidence < 90', 'max_score': 0, 'applied': True}],
                        'final_score': 0,
                        'final_grade': 'REJECTED',
                        'forced_rejection': True,
                    },
                    'caps_applied': applied_caps + ['CAP_DRIFT_MC_REJECT'],
                }

        if bool((exceptions or {}).get('force_rejected_due_to_persistence')):
            return {
                'score': 0,
                'grade': 'REJECTED',
                'penalties_breakdown': {
                    'base_score': base_score,
                    'penalties': penalties,
                    'caps': caps + [{'id': 'CAP_EXCEPTION_PERSISTENCE', 'label': '3+ consecutive exception years', 'max_score': 0, 'applied': True}],
                    'final_score': 0,
                    'final_grade': 'REJECTED',
                    'forced_rejection': True,
                },
                'caps_applied': applied_caps + ['CAP_EXCEPTION_PERSISTENCE'],
            }

        score = min(score, cap_max)
        score = max(0, min(100, int(round(score))))

        if score >= 90:
            grade = 'HIGH'
        elif score >= 75:
            grade = 'MEDIUM'
        elif score >= 55:
            grade = 'LOW'
        else:
            grade = 'REJECTED'

        # HIGH strict gate
        no_recon_flags = bool((validations.get('reconciliation') or {}).get('balance_sheet_ok', True))
        no_anomaly_flags = not bool((validations.get('anomaly') or {}).get('flagged'))
        if grade == 'HIGH':
            if not (fg == 'IN_RANGE_ANNUAL' and mc >= 90 and not ex_entries and no_recon_flags and no_anomaly_flags):
                grade = 'MEDIUM'

        if not validations.get('value_present'):
            grade = 'REJECTED'
            score = 0

        breakdown = {
            'base_score': base_score,
            'penalties': penalties,
            'caps': caps,
            'final_score': score,
            'final_grade': grade,
            'forced_rejection': grade == 'REJECTED' and score == 0 and (hard_reject or not validations.get('value_present')),
        }
        return {
            'score': score,
            'grade': grade,
            'penalties_breakdown': breakdown,
            'caps_applied': applied_caps,
        }

    @staticmethod
    def _has_hard_reject(reasons: List[str]) -> bool:
        hard_exact = {
            'MVFS_FAIL',
            'currency_conflict_unresolved',
            'scale_conflict_unresolved',
            'denominator_near_zero',
            'sector_not_bank',
            'insurance_company',
            'implausible_value',
            'period_type_mismatch',
            'currency_mismatch_required_inputs',
            'missing_required_inputs_or_denominator_near_zero',
            'percent_ratio_guardrail_exceeded',
            'percent_out_of_range',
        }
        for r in reasons:
            if r in hard_exact:
                return True
            if r.startswith('missing_required_input:'):
                return True
            if r.startswith('mapping_confidence_below_80:'):
                return True
            if r.startswith('missing_unit_or_currency:'):
                return True
        return False

    def _apply_exception_bounds_override(
        self,
        *,
        ratio_id: str,
        profile: str,
        bounds: Optional[Tuple[float, float]],
        exception_ctx: Dict,
        out: Dict,
    ) -> Optional[Tuple[float, float]]:
        entries = list((exception_ctx or {}).get('entries') or [])
        ex_ids = {e.get('exception_id') for e in entries}
        if bounds is None:
            return bounds

        lo, hi = bounds
        # EX-02: relax margin bounds for early-stage periods (strictly temporary)
        if 'EX-02' in ex_ids and ratio_id in {'net_margin', 'operating_margin', 'gross_margin'}:
            lo, hi = (-5.0, 5.0)
        # EX-04: banking leverage structure
        if 'EX-04' in ex_ids and profile == 'bank' and ratio_id == 'debt_to_equity':
            hi = max(hi, 50.0)
        out['diagnostics']['exception_bounds_override_applied'] = (lo, hi) != bounds
        return (lo, hi)

    @staticmethod
    def _downgrade_one_level(grade: str) -> str:
        if grade == 'HIGH':
            return 'MEDIUM'
        if grade == 'MEDIUM':
            return 'LOW'
        return grade

    def _has_blocking_failures(self, out: Dict) -> bool:
        return len(out['reliability']['gates_failed']) > 0

    def _mark_run(self, out: Dict, phase: str) -> None:
        out['reliability']['validators_run'].append(phase)

    def _pass(self, out: Dict, gate: str) -> None:
        out['reliability']['gates_passed'].append(gate)

    def _fail(self, out: Dict, reason: str) -> None:
        out['reliability']['gates_failed'].append(reason)
        out['reasons'].append(reason)

    def _scale_base(self, row: Dict[str, float]) -> float:
        assets = self._num(row.get('BS.ASSETS'))
        revenue = self._num(row.get('IS.REV'))
        return max(abs(assets or 0.0), abs(revenue or 0.0), 1.0)

    def _is_material(self, value: Optional[float], scale_base: float) -> bool:
        if value is None:
            return False
        threshold = max(self.ABS_MIN_USD, self.REL_MIN * scale_base)
        return abs(float(value)) >= threshold

    @staticmethod
    def _num(v) -> Optional[float]:
        if isinstance(v, (int, float)):
            return float(v)
        return None

    @staticmethod
    def _is_finite(x) -> bool:
        try:
            v = float(x)
            return v == v and abs(v) != float('inf')
        except Exception:
            return False

    @staticmethod
    def _dedup(items: List[str]) -> List[str]:
        seen = set()
        out = []
        for i in items:
            if i not in seen:
                seen.add(i)
                out.append(i)
        return out
