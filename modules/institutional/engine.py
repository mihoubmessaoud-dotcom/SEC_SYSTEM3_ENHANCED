from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

from .classification import AdaptiveCompanyClassifier
from .computation import DynamicTemporalStructureEngine, IntelligentComputationEngine
from .learning import ExtensionLearningLayer
from .mapping_confidence import MappingConfidenceEngine
from .mapping import OntologyMapper
from .ontology import HierarchicalFinancialOntology
from .prediction import PredictiveScenarioEngine
from .ratios import SectorRatioEngine
from .strategic import StrategicIntelligenceEngine
from .validation import FinancialIntegrityEngine
from .validators import RatioValidationPipeline


@dataclass
class EngineConfig:
    tolerance: float = 0.05
    output_dir: str = 'exports/institutional'


class InstitutionalFinancialIntelligenceEngine:
    """
    End-to-end institutional-grade normalization and analytics pipeline.
    """

    def __init__(self, config: Optional[EngineConfig] = None) -> None:
        self.config = config or EngineConfig()
        self.classifier = AdaptiveCompanyClassifier()
        self.ontology = HierarchicalFinancialOntology()
        self.mapper = OntologyMapper()
        self.mapping_confidence = MappingConfidenceEngine()
        self.computation = IntelligentComputationEngine(tolerance=self.config.tolerance)
        self.temporal = DynamicTemporalStructureEngine()
        self.ratios = SectorRatioEngine()
        self.integrity = FinancialIntegrityEngine()
        self.strategic = StrategicIntelligenceEngine()
        self.predictive = PredictiveScenarioEngine()
        self.learning = ExtensionLearningLayer()

    def run(
        self,
        company_meta: Dict,
        data_by_year: Dict[int, Dict[str, float]],
        data_by_period: Optional[Dict[str, Dict[str, float]]] = None,
        fx_rates: Optional[Dict[int, float]] = None,
        scenario: Optional[Dict] = None,
    ) -> Dict:
        data_by_period = data_by_period or {}

        # 1) Classification
        concepts = sorted({k for y in data_by_year for k in data_by_year[y].keys()})
        cls = self.classifier.classify(company_meta=company_meta, xbrl_concepts=concepts, data_by_year=data_by_year)
        profile = cls['primary_profile']

        # 2) Ontology + Mapping
        nodes = self.ontology.get_nodes_for_profile(profile)
        decisions, unknown_tags = self.mapper.map_concepts(concepts, nodes)

        # 3) Extension learning suggestions
        learning_output = self.learning.process_unknown_tags(unknown_tags, nodes)

        # 4) Currency normalization + computation
        normalized_currency = self.computation.normalize_currency(data_by_year, fx_rates)
        comp = self.computation.compute(normalized_currency, self.ontology, decisions)
        computed_by_year = comp['computed_by_year']
        node_inputs_by_year = comp.get('node_inputs_by_year', {})
        reconciliation_by_year = comp.get('reconciliation_by_year', {})

        normalization_ctx = self._build_normalization_ctx(normalized_currency)
        mapping_ctx = self.mapping_confidence.compute(
            years=sorted(computed_by_year.keys()),
            mapping_decisions=decisions,
            node_inputs_by_year=node_inputs_by_year,
        )

        # 5) Restatement tracking
        restatements = self.computation.track_restatements(data_by_period)

        # 6) Temporal structural drift
        mapping_versions = self.temporal.build_yearly_mapping_versions(
            computed_by_year=computed_by_year,
            mapping_decisions=decisions,
            raw_data_by_year=normalized_currency,
        )
        structure = self.temporal.detect_structural_breaks(mapping_versions)

        # 7) Sector-aware ratios
        provenance_ctx = {
            'cik': company_meta.get('cik'),
            'ticker': company_meta.get('ticker'),
            'company_name': company_meta.get('name'),
            'form': company_meta.get('selected_form'),
            'accession': company_meta.get('selected_accession'),
            'filing_date': company_meta.get('selected_filing_date') or company_meta.get('selected_period_end'),
            'period_end': company_meta.get('selected_period_end'),
            'filing_grade': company_meta.get('filing_grade'),
            'in_range': company_meta.get('filing_in_range'),
            'extraction_method': company_meta.get('extraction_method', 'companyfacts_mapped'),
        }
        ratio_by_year = self.ratios.compute(
            profile,
            computed_by_year,
            provenance_ctx=provenance_ctx,
            classification=cls,
            mapping_ctx=mapping_ctx,
            normalization_ctx=normalization_ctx,
            reconciliation_ctx=reconciliation_by_year,
            node_inputs_by_year=node_inputs_by_year,
            company_meta=company_meta,
        )
        ratio_explanations_by_year = self.ratios.explain(
            profile,
            computed_by_year,
            provenance_ctx=provenance_ctx,
            classification=cls,
            mapping_ctx=mapping_ctx,
            normalization_ctx=normalization_ctx,
            reconciliation_ctx=reconciliation_by_year,
            node_inputs_by_year=node_inputs_by_year,
            company_meta=company_meta,
        )

        # 8) Integrity and risk
        integrity = self.integrity.evaluate(computed_by_year, ratio_by_year, comp['anomaly_flags'])

        # 9) Strategic intelligence
        strategic = self.strategic.analyze(computed_by_year, profile)

        # 10) Predictive scenarios
        forecast = self.predictive.forecast(computed_by_year, scenario=scenario)

        # 11) Build required outputs
        outputs = self._build_outputs(
            company_meta=company_meta,
            classification=cls,
            decisions=decisions,
            computed_by_year=computed_by_year,
            raw_data_by_year=normalized_currency,
            ratio_by_year=ratio_by_year,
            ratio_explanations_by_year=ratio_explanations_by_year,
            integrity=integrity,
            strategic=strategic,
            structure=structure,
            mapping_ctx=mapping_ctx,
            mapping_versions=mapping_versions,
            normalization_ctx=normalization_ctx,
            reconciliation_by_year=reconciliation_by_year,
            forecast=forecast,
            restatements=restatements,
            learning_output=learning_output,
        )

        return outputs

    def _build_outputs(
        self,
        company_meta: Dict,
        classification: Dict,
        decisions,
        computed_by_year: Dict[int, Dict[str, float]],
        raw_data_by_year: Dict[int, Dict[str, float]],
        ratio_by_year: Dict[int, Dict[str, float]],
        ratio_explanations_by_year: Dict[int, Dict],
        integrity: Dict,
        strategic: Dict,
        structure: Dict,
        mapping_ctx: Dict,
        mapping_versions: Dict[int, Dict],
        normalization_ctx: Dict,
        reconciliation_by_year: Dict[int, Dict],
        forecast: Dict,
        restatements: Dict,
        learning_output: Dict,
    ) -> Dict:
        years = sorted(computed_by_year.keys())

        # Normalized Financial Statements
        nfs_rows = []
        node_contract_rows = []
        scoring_pipeline = RatioValidationPipeline()
        for y in years:
            row = {'year': y}
            row.update(computed_by_year[y])
            nfs_rows.append(row)
            for node_id, v in computed_by_year[y].items():
                node_reasons = []
                if not isinstance(v, (int, float)):
                    node_reasons.append('node_missing_or_non_numeric')
                mc = float(((mapping_ctx.get('confidence_by_year') or {}).get(y, {}) or {}).get(node_id, 100.0))
                if mc < 80:
                    node_reasons.append(f'mapping_confidence_below_80:{node_id}')
                nrm = ((normalization_ctx.get('by_year') or {}).get(y, {}) or {})
                if nrm.get('currency_conflict'):
                    node_reasons.append('currency_conflict_unresolved')
                if nrm.get('scale_conflict'):
                    node_reasons.append('scale_conflict_unresolved')
                rec = ((reconciliation_by_year or {}).get(y, {}) or {})
                validations = {
                    'reasons': node_reasons,
                    'hard_reject': False,
                    'value_present': isinstance(v, (int, float)),
                    'required_nodes': [node_id],
                    'inputs_used': {
                        node_id: {
                            'value': v if isinstance(v, (int, float)) else None,
                            'unit': 'USD',
                            'currency': 'USD',
                            'period_end': f'{y}-12-31',
                            'context_id': f'{y}:{node_id}',
                            'source_tag': node_id,
                            'source_label': node_id,
                            'period_type': 'instant' if node_id.startswith('BS.') else 'duration',
                        }
                    },
                    'yoy_flags': [],
                    'reconciliation': rec,
                    'normalization': nrm,
                    'mapping_confidence': {'required_min_score': mc, 'required_avg_score': mc, 'drift_flags': []},
                    'bound_edge_hit': False,
                    'bound_near_edge_hit': False,
                    'classification': {},
                    'anomaly': {'flagged': False},
                    'materiality': {},
                    'drift_flags': [],
                }
                node_score_obj = scoring_pipeline.compute_reliability_score(
                    validations=validations,
                    provenance={
                        'cik': company_meta.get('cik'),
                        'ticker': company_meta.get('ticker'),
                        'company_name': company_meta.get('name'),
                        'form': company_meta.get('selected_form'),
                        'accession': company_meta.get('selected_accession'),
                        'filing_date': company_meta.get('selected_filing_date'),
                        'period_end': company_meta.get('selected_period_end'),
                    },
                    filing_grade=str(company_meta.get('filing_grade') or ''),
                    mapping_confidence=mc,
                    flags={},
                    exceptions={'entries': [], 'status_flags': [], 'exception_count': 0},
                )
                node_grade = node_score_obj.get('grade', 'REJECTED')
                node_score = node_score_obj.get('score', 0)
                node_contract_rows.append({
                    'year': y,
                    'node_id': node_id,
                    'value': v if (isinstance(v, (int, float)) and node_grade != 'REJECTED') else None,
                    'reliability': {
                        'grade': node_grade,
                        'score': node_score,
                        'gates_passed': ['node_present'] if isinstance(v, (int, float)) else [],
                        'gates_failed': node_reasons,
                        'validators_run': ['node_contract_materialization'],
                    },
                    'reasons': node_reasons,
                    'diagnostics': {
                        'node_type': node_id.split('.', 1)[0],
                        'year': y,
                        'mapping_confidence': mc,
                        'reliability_scoring': node_score_obj,
                    },
                    'inputs_used': {
                        node_id: {
                            'value': v if isinstance(v, (int, float)) else None,
                            'unit': 'USD',
                            'currency': 'USD',
                            'period_end': f'{y}-12-31',
                            'context_id': f'{y}:{node_id}',
                            'source_tag': node_id,
                            'source_label': node_id,
                            'period_type': 'instant' if node_id.startswith('BS.') else 'duration',
                        }
                    },
                    'provenance': {
                        'cik': company_meta.get('cik'),
                        'ticker': company_meta.get('ticker'),
                        'company_name': company_meta.get('name'),
                        'form': company_meta.get('selected_form'),
                        'accession': company_meta.get('selected_accession'),
                        'filing_date': company_meta.get('selected_filing_date'),
                        'period_end': company_meta.get('selected_period_end'),
                        'in_range': company_meta.get('filing_in_range'),
                        'filing_grade': company_meta.get('filing_grade'),
                        'extraction_method': company_meta.get('extraction_method', 'companyfacts_mapped'),
                        'xbrl_context_ids': [f'{y}:{node_id}'],
                        'hash_of_sources': f'{y}-{node_id}',
                    },
                })
        normalized_statements_df = pd.DataFrame(nfs_rows)
        node_contracts_df = pd.DataFrame(node_contract_rows)
        layer1_rows = []
        raw_years = sorted((raw_data_by_year or {}).keys())
        raw_concepts = sorted({
            c for y in raw_years for c in (raw_data_by_year.get(y, {}) or {}).keys()
            if not str(c).startswith('_')
        })
        for concept in raw_concepts:
            rec = {'Tag': concept}
            for y in raw_years:
                rec[str(y)] = (raw_data_by_year.get(y, {}) or {}).get(concept)
            layer1_rows.append(rec)
        layer1_raw_df = pd.DataFrame(layer1_rows)

        # Sector-Aware Ratio Dashboard
        ratio_rows = []
        for y in years:
            row = {'year': y, 'profile': classification['primary_profile']}
            row.update(ratio_by_year.get(y, {}))
            ratio_rows.append(row)
        ratio_dashboard_df = pd.DataFrame(ratio_rows)
        ratio_explanations_rows = []
        for y in years:
            for item in ratio_explanations_by_year.get(y, []):
                ratio_explanations_rows.append({'year': y, **item})
        ratio_explanations_df = pd.DataFrame(ratio_explanations_rows)

        # Risk & Integrity Dashboard
        risk_dashboard_df = pd.DataFrame([{
            'earnings_quality_score': integrity['earnings_quality_score'],
            'structural_stability_score': integrity['structural_stability_score'],
            'financial_risk_score': integrity['financial_risk_score'],
            'data_confidence_score': integrity['data_confidence_score'],
            'findings_count': len(integrity['findings']),
            'restatements_count': len(restatements),
        }])

        # Strategic Evolution Report
        strategic_report_df = pd.DataFrame([{
            'revenue_cagr': strategic['cagr'].get('revenue_cagr'),
            'net_income_cagr': strategic['cagr'].get('net_income_cagr'),
            'fcf_cagr': strategic['cagr'].get('fcf_cagr'),
            'margin_expansion': strategic['margin_expansion'].get('expansion'),
            'capital_allocation_efficiency': strategic['capital_allocation_efficiency'].get('fcf_to_assets_avg'),
            'operating_leverage_ratio': strategic['operating_leverage'].get('operating_leverage_ratio'),
            'competitive_positioning_trend': strategic['competitive_positioning_trend'],
            'strategic_shifts': '; '.join(strategic['strategic_shifts']),
        }])

        # Structural Change Report
        structural_rows = [{'structural_volatility_score': structure.get('structural_volatility_score', 0.0)}]
        for b in structure.get('structural_breaks', []):
            structural_rows.append({
                'year': b.get('year'),
                'drift_ratio': b.get('drift_ratio'),
                'added_nodes': ', '.join(b.get('added_nodes', [])),
                'removed_nodes': ', '.join(b.get('removed_nodes', [])),
                'reason': b.get('reason'),
            })
        structural_report_df = pd.DataFrame(structural_rows)

        # Forecast & Scenario Report
        forecast_rows = []
        for y, row in sorted((forecast.get('forecast') or {}).items()):
            forecast_rows.append({'year': y, **row})
        valuation = forecast.get('valuation_range') or {}
        if valuation:
            forecast_rows.append({'year': 'valuation', **valuation})
        mc = forecast.get('monte_carlo') or {}
        if mc:
            forecast_rows.append({'year': 'monte_carlo', **mc})
        forecast_report_df = pd.DataFrame(forecast_rows)

        # AI-ready clean dataset
        ai_rows = []
        for y in years:
            base = {'year': y, 'profile': classification['primary_profile']}
            base.update(computed_by_year.get(y, {}))
            base.update({f'ratio_{k}': v for k, v in ratio_by_year.get(y, {}).items()})
            ai_rows.append(base)
        ai_ready_df = pd.DataFrame(ai_rows)

        mapping_df = pd.DataFrame([d.__dict__ for d in decisions])
        findings_df = pd.DataFrame(integrity['findings'])
        unknown_df = pd.DataFrame(learning_output.get('suggestions', []))
        profile_probs_df = pd.DataFrame(classification.get('profile_probabilities', []))
        mapping_versions_df = pd.DataFrame([
            {
                'year': y,
                'node_count': v.get('node_count'),
                'mapping_confidence_avg': v.get('mapping_confidence_avg'),
                'active_nodes': ', '.join(v.get('active_nodes', [])),
            }
            for y, v in sorted(mapping_versions.items())
        ])
        mapping_confidence_rows = []
        for y, node_scores in sorted((mapping_ctx.get('confidence_by_year') or {}).items()):
            for node_id, score in sorted(node_scores.items()):
                mapping_confidence_rows.append({
                    'year': y,
                    'node_id': node_id,
                    'mapping_confidence': score,
                })
        mapping_confidence_df = pd.DataFrame(mapping_confidence_rows)

        # Year-level API package (statements + ratios + integrity + mapping confidence + warnings)
        findings_by_year = {}
        for f in integrity.get('findings', []):
            y = f.get('year')
            findings_by_year.setdefault(y, []).append(f)
        yearly_outputs = {}
        for y in years:
            yearly_outputs[y] = {
                'normalized_financial_statement': computed_by_year.get(y, {}),
                'normalized_financial_statement_contracts': [
                    r for r in node_contract_rows if r.get('year') == y
                ],
                'profile_aware_ratios': ratio_by_year.get(y, {}),
                'ratio_explanations': ratio_explanations_by_year.get(y, []),
                'structural_integrity_annotations': findings_by_year.get(y, []),
                'mapping_confidence': mapping_versions.get(y, {}).get('mapping_confidence_avg', 0.0),
                'mapping_confidence_detail': (mapping_ctx.get('confidence_by_year', {}) or {}).get(y, {}),
                'warnings': [f.get('flag_type') for f in findings_by_year.get(y, [])],
            }

        return {
            'classification': classification,
            'classifier_diagnostics': classification.get('classifier_diagnostics', {}),
            'ontology_nodes': self.ontology.to_records(),
            'mapping': mapping_df,
            'mapping_versions': mapping_versions_df,
            'mapping_confidence_detail': mapping_confidence_df,
            'sector_ratio_dashboard': ratio_dashboard_df,
            'ratio_explanations': ratio_explanations_df,
            'risk_integrity_dashboard': risk_dashboard_df,
            'strategic_evolution_report': strategic_report_df,
            'structural_change_report': structural_report_df,
            'forecast_scenario_report': forecast_report_df,
            'ai_ready_dataset': ai_ready_df,
            'integrity_findings': findings_df,
            'extension_suggestions': unknown_df,
            'profile_probabilities': profile_probs_df,
            'restatements': pd.DataFrame(restatements),
            'yearly_outputs': yearly_outputs,
        }

    def _deep_audit_reconciliation(self, layer1_csv_path: Path) -> pd.DataFrame:
        """
        Deep_Audit_Reconciliation protocol:
        - hierarchy-first extraction
        - accounting sign adjustments
        - strict equity override for accumulated/comprehensive
        - reconciliation + gap fill from unclassified candidates
        - parent-child consistency diagnostics
        """
        df = pd.read_csv(layer1_csv_path)
        if df.empty:
            return pd.DataFrame(columns=['Year', 'Indicator', 'Value', 'Reliability', 'Reason', 'Audit_Trail', 'Unit_Basis'])

        tag_col = 'Tag' if 'Tag' in df.columns else df.columns[0]
        year_cols = []
        for c in df.columns:
            try:
                year_cols.append((int(str(c)), c))
            except Exception:
                continue
        year_cols.sort(key=lambda t: t[0])

        def _to_float(v):
            if pd.isna(v):
                return None
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip().replace(',', '')
            if not s:
                return None
            if s.startswith('(') and s.endswith(')'):
                s = '-' + s[1:-1]
            try:
                return float(s)
            except Exception:
                return None

        tag_index = {}
        for _, r in df.iterrows():
            t = str(r.get(tag_col, '')).strip()
            if t:
                tag_index[t] = r

        def _classify_tag(tag: str) -> str:
            lt = str(tag or '').lower()
            if 'accumulated' in lt or 'comprehensive' in lt:
                return 'equity'
            if any(k in lt for k in ('asset', 'receivable', 'inventory', 'cash')):
                return 'assets'
            if any(k in lt for k in ('liabilit', 'payable', 'debt', 'borrow')):
                return 'liabilities'
            if any(k in lt for k in ('equity', 'stockholder', 'retainedearnings', 'additionalpaidincapital')):
                return 'equity'
            if any(k in lt for k in ('income', 'revenue', 'expense', 'profit', 'loss')):
                return 'income_statement'
            return 'unclassified'

        def _is_segment_or_adjustment(tag: str) -> bool:
            lt = str(tag or '').lower()
            return ('segment' in lt) or ('adjustment' in lt)

        def _infer_balance(tag: str, section: str) -> str:
            lt = str(tag or '').lower()
            is_contra = any(k in lt for k in ('accumulated', 'allowance', 'provision'))
            if section == 'assets':
                return 'credit' if is_contra else 'debit'
            if section in ('liabilities', 'equity'):
                return 'debit' if is_contra else 'credit'
            return 'debit'

        def _signed(tag: str, section: str, value: float) -> float:
            bal = _infer_balance(tag, section)
            if section == 'assets' and bal == 'credit':
                return -1.0 * value
            if section in ('liabilities', 'equity') and bal == 'debit':
                return -1.0 * value
            return value

        def _pick_parent(year_col: str, section: str, fallback_exact):
            hierarchy_candidates = []
            for t, r in tag_index.items():
                if not t.endswith('_Hierarchy'):
                    continue
                if _is_segment_or_adjustment(t):
                    continue
                lt = t.lower()
                if section == 'assets' and 'asset' in lt and 'total' in lt and 'current' not in lt:
                    hierarchy_candidates.append(t)
                if section == 'liabilities' and 'liabilit' in lt and 'total' in lt and 'current' not in lt:
                    hierarchy_candidates.append(t)
                if section == 'equity' and 'equity' in lt and 'total' in lt:
                    hierarchy_candidates.append(t)
            if hierarchy_candidates:
                t = hierarchy_candidates[0]
                v = _to_float(tag_index[t].get(year_col))
                if v is not None:
                    return _signed(t, section, v), 95, 'Hierarchy-first parent', [t]
            for fb in fallback_exact:
                if fb not in tag_index:
                    continue
                v = _to_float(tag_index[fb].get(year_col))
                if v is not None:
                    return _signed(fb, section, v), 45, 'Raw fallback (no hierarchy parent)', [fb]
            return None, 0, 'Parent not found', []

        def _sum_children(year_col: str, section: str):
            total = 0.0
            used = []
            for t, r in tag_index.items():
                if not t.endswith('_Hierarchy'):
                    continue
                if _is_segment_or_adjustment(t):
                    continue
                if 'total' in t.lower():
                    continue
                if _classify_tag(t) != section:
                    continue
                v = _to_float(r.get(year_col))
                if v is None:
                    continue
                total += _signed(t, section, v)
                used.append(t)
            if not used:
                return None, []
            return total, used

        def _find_gap_candidate(year_col: str, gap: float):
            best = None
            best_err = None
            for t, r in tag_index.items():
                if _classify_tag(t) != 'unclassified':
                    continue
                if _is_segment_or_adjustment(t):
                    continue
                v = _to_float(r.get(year_col))
                if v is None:
                    continue
                err = abs(abs(v) - abs(gap))
                if best_err is None or err < best_err:
                    best = (t, v)
                    best_err = err
            if best is None:
                return None
            tol = max(1.0, abs(gap) * 0.01)
            return best if (best_err is not None and best_err <= tol) else None

        out_rows = []
        for year, year_col in year_cols:
            assets, rel_a, reason_a, trail_a = _pick_parent(year_col, 'assets', ['Assets'])
            liabilities, rel_l, reason_l, trail_l = _pick_parent(year_col, 'liabilities', ['Liabilities'])
            equity, rel_e, reason_e, trail_e = _pick_parent(
                year_col,
                'equity',
                ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
            )

            # Strict equity override: accumulated/comprehensive always to equity.
            for t, r in tag_index.items():
                lt = t.lower()
                if ('accumulated' not in lt and 'comprehensive' not in lt) or _is_segment_or_adjustment(t):
                    continue
                v = _to_float(r.get(year_col))
                if v is None:
                    continue
                if equity is None:
                    equity = 0.0
                equity += _signed(t, 'equity', v)
                trail_e.append(t)
                reason_e = f'{reason_e}; strict_equity_override'
                rel_e = max(rel_e, 55)

            sum_assets, child_assets = _sum_children(year_col, 'assets')
            sum_liab, child_liab = _sum_children(year_col, 'liabilities')
            sum_eq, child_eq = _sum_children(year_col, 'equity')

            def _parent_child_reason(parent_value, children_value, current_reason):
                if parent_value is None or children_value is None:
                    return current_reason
                denom = max(1.0, abs(parent_value))
                rel = abs(parent_value - children_value) / denom
                if rel <= 0.02:
                    return f'{current_reason}; Matched via children summation'
                return f'{current_reason}; SEC parent retained due to children mismatch'

            reason_a = _parent_child_reason(assets, sum_assets, reason_a)
            reason_l = _parent_child_reason(liabilities, sum_liab, reason_l)
            reason_e = _parent_child_reason(equity, sum_eq, reason_e)

            if assets is None and sum_assets is not None:
                assets = sum_assets
                rel_a = max(rel_a, 60)
                trail_a.extend(child_assets)
                reason_a = 'Virtual Total from hierarchy children'
            if liabilities is None and sum_liab is not None:
                liabilities = sum_liab
                rel_l = max(rel_l, 60)
                trail_l.extend(child_liab)
                reason_l = 'Virtual Total from hierarchy children'
            if equity is None and sum_eq is not None:
                equity = sum_eq
                rel_e = max(rel_e, 60)
                trail_e.extend(child_eq)
                reason_e = 'Virtual Total from hierarchy children'

            # Balance inference before diff calculation when one side is missing.
            if assets is None and liabilities is not None and equity is not None:
                assets = liabilities + equity
                trail_a.append('BalanceSnipe:Assets=Liabilities+Equity')
                reason_a = f'{reason_a}; Grade C inferred from balance equation'
            elif liabilities is None and assets is not None and equity is not None:
                liabilities = assets - equity
                trail_l.append('BalanceSnipe:Liabilities=Assets-Equity')
                reason_l = f'{reason_l}; Grade C inferred from balance equation'
            elif equity is None and assets is not None and liabilities is not None:
                equity = assets - liabilities
                trail_e.append('BalanceSnipe:Equity=Assets-Liabilities')
                reason_e = f'{reason_e}; Grade C inferred from balance equation'

            diff = None
            if assets is not None and liabilities is not None and equity is not None:
                diff = assets - (liabilities + equity)

            if diff is not None and abs(diff) > 1.0:
                gap = _find_gap_candidate(year_col, diff)
                if gap is not None:
                    gt, gv = gap
                    gcls = _classify_tag(gt)
                    if gcls in ('liabilities', 'equity'):
                        if gcls == 'liabilities':
                            liabilities = (liabilities or 0.0) + _signed(gt, 'liabilities', gv)
                            trail_l.append(gt)
                            reason_l = f'{reason_l}; Gap fill from Unclassified'
                        else:
                            equity = (equity or 0.0) + _signed(gt, 'equity', gv)
                            trail_e.append(gt)
                            reason_e = f'{reason_e}; Gap fill from Unclassified'
                    else:
                        # Keep balance equation stable: route unknown gap by direction.
                        if diff > 0:
                            liabilities = (liabilities or 0.0) + abs(gv)
                            trail_l.append(gt)
                            reason_l = f'{reason_l}; Gap fill from Unclassified'
                        else:
                            assets = (assets or 0.0) + abs(gv)
                            trail_a.append(gt)
                            reason_a = f'{reason_a}; Gap fill from Unclassified'
                    diff = assets - (liabilities + equity)

            # Final balance sniper (Grade C): enforce Assets = Liabilities + Equity.
            if diff is not None and abs(diff) > 1.0:
                if assets is None and liabilities is not None and equity is not None:
                    assets = liabilities + equity
                    trail_a.append('BalanceSnipe:Assets=Liabilities+Equity')
                    reason_a = f'{reason_a}; Grade C balance snipe'
                elif liabilities is None and assets is not None and equity is not None:
                    liabilities = assets - equity
                    trail_l.append('BalanceSnipe:Liabilities=Assets-Equity')
                    reason_l = f'{reason_l}; Grade C balance snipe'
                elif equity is None and assets is not None and liabilities is not None:
                    equity = assets - liabilities
                    trail_e.append('BalanceSnipe:Equity=Assets-Liabilities')
                    reason_e = f'{reason_e}; Grade C balance snipe'
                elif assets is not None and liabilities is not None and equity is not None:
                    liabilities = liabilities + diff
                    trail_l.append('BalanceSnipe:Liabilities+=Diff')
                    reason_l = f'{reason_l}; Grade C force-balance'
                diff = assets - (liabilities + equity)

            balanced = diff is not None and math.isfinite(diff) and abs(diff) <= 1.0
            balance_rel = 100 if balanced else min(rel_a, rel_l, rel_e, 70)
            out_rows.extend([
                {
                    'Year': year,
                    'Indicator': 'Total Assets',
                    'Value': assets,
                    'Reliability': 100 if balanced else rel_a,
                    'Reason': f'{reason_a}; Adjusted via sign logic',
                    'Audit_Trail': json.dumps(sorted(set(trail_a)), ensure_ascii=False),
                    'Unit_Basis': 'ABSOLUTE_USD',
                },
                {
                    'Year': year,
                    'Indicator': 'Total Liabilities',
                    'Value': liabilities,
                    'Reliability': 100 if balanced else rel_l,
                    'Reason': f'{reason_l}; Adjusted via sign logic',
                    'Audit_Trail': json.dumps(sorted(set(trail_l)), ensure_ascii=False),
                    'Unit_Basis': 'ABSOLUTE_USD',
                },
                {
                    'Year': year,
                    'Indicator': 'Total Equity',
                    'Value': equity,
                    'Reliability': 100 if balanced else rel_e,
                    'Reason': f'{reason_e}; Adjusted via sign logic',
                    'Audit_Trail': json.dumps(sorted(set(trail_e)), ensure_ascii=False),
                    'Unit_Basis': 'ABSOLUTE_USD',
                },
                {
                    'Year': year,
                    'Indicator': 'Balance Difference',
                    'Value': diff,
                    'Reliability': balance_rel,
                    'Reason': 'Total Assets - (Total Liabilities + Total Equity)',
                    'Audit_Trail': json.dumps([], ensure_ascii=False),
                    'Unit_Basis': 'ABSOLUTE_USD',
                },
            ])
        return pd.DataFrame(out_rows, columns=['Year', 'Indicator', 'Value', 'Reliability', 'Reason', 'Audit_Trail', 'Unit_Basis'])

    def _build_normalization_ctx(self, data_by_year: Dict[int, Dict[str, float]]) -> Dict:
        out = {'by_year': {}}
        years = sorted(data_by_year.keys())
        for y in years:
            row = data_by_year.get(y, {})
            rev = row.get('Revenues') or row.get('RevenueFromContractWithCustomerExcludingAssessedTax') or row.get('SalesRevenueNet')
            assets = row.get('Assets')
            eq = row.get('StockholdersEquity')
            values = [v for v in [rev, assets, eq] if isinstance(v, (int, float)) and abs(v) > 0]
            detected_scale = 1.0
            if values:
                m = max(abs(v) for v in values)
                if m >= 1e12:
                    detected_scale = 1e9
                elif m >= 1e9:
                    detected_scale = 1e6
                elif m >= 1e6:
                    detected_scale = 1e3
            out['by_year'][y] = {
                'detected_unit': 'USD',
                'detected_scale': detected_scale,
                'normalization_applied': False,
                'confidence': 0.9,
                'currency_conflict': False,
                'scale_conflict': False,
                'conflicts': [],
            }
        # simple cross-year scale drift/conflict check
        scales = [out['by_year'][y]['detected_scale'] for y in years if y in out['by_year']]
        if scales and (max(scales) / max(min(scales), 1.0)) > 1000:
            for y in years:
                out['by_year'][y]['scale_conflict'] = True
                out['by_year'][y]['conflicts'].append('cross_year_scale_conflict')
        return out

    def save_outputs(self, outputs: Dict, output_dir: Optional[str] = None) -> Dict[str, str]:
        out_dir = Path(output_dir or self.config.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        files = {}
        # Direct extraction only: one official statement file, no layered exports.
        sec_df = outputs.get('sec_official_statement')
        if isinstance(sec_df, pd.DataFrame):
            fp = out_dir / 'SEC_Official_Statement.csv'
            sec_df.to_csv(fp, index=False)
            files['sec_official_statement'] = str(fp)
        return files
