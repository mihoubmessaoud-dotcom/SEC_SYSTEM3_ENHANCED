from __future__ import annotations

from statistics import median
from typing import Dict, List, Optional, Tuple

from .exception_registry import ExceptionPolicyEngine
from ..ratio_formats import get_ratio_metadata
from .validators import RatioValidationPipeline


class SectorRatioEngine:
    """
    Sector-aware ratio engine with strict ordered validation pipeline.
    """

    def __init__(self) -> None:
        self.registry = self._build_registry()
        self.pipeline = RatioValidationPipeline()
        self.exception_policy = ExceptionPolicyEngine()

    def _build_registry(self) -> Dict[str, List[Dict]]:
        return {
            'industrial': [
                self._ratio_def('gross_margin', 'Gross Profit / Revenue', ['IS.GP', 'IS.REV'], ['industrial'], False, False, 0.10),
                self._ratio_def('operating_margin', 'Operating Income / Revenue', ['IS.OP', 'IS.REV'], ['industrial'], False, False, 0.12),
                self._ratio_def('fcf_margin', 'FCF / Revenue', ['CF.FCF', 'IS.REV'], ['industrial'], False, True, 0.10),
                self._ratio_def('asset_turnover', 'Revenue / Assets', ['IS.REV', 'BS.ASSETS'], ['industrial'], True, False, 0.10),
                self._ratio_def('inventory_turnover', 'Cost of Revenue / Inventory', ['IS.COGS', 'BS.INV'], ['industrial'], False, False, 0.10),
            ],
            'bank': [
                self._ratio_def('net_interest_margin', 'Net Interest Income / Average Assets', ['IS.NII', 'BS.ASSETS'], ['bank'], True, False, 0.20),
                self._ratio_def('loan_to_deposit_ratio', 'Loans / Deposits', ['BS.LOANS', 'BS.DEPOSITS'], ['bank'], False, False, 0.20),
                self._ratio_def('capital_ratio_proxy', 'CET1 Capital / Assets (fallback Equity/Assets)', ['BS.CET1', 'BS.EQ', 'BS.ASSETS'], ['bank'], True, False, 0.25),
                self._ratio_def('net_income_to_assets', 'Net Income / Assets', ['IS.NET', 'BS.ASSETS'], ['bank'], True, False, 0.15),
                self._ratio_def('equity_ratio', 'Equity / Assets', ['BS.EQ', 'BS.ASSETS'], ['bank'], True, False, 0.20),
            ],
            'insurance': [
                self._ratio_def('combined_proxy', 'Expense Proxy / Revenue', ['IS.COGS', 'IS.REV'], ['insurance'], False, False, 0.15),
                self._ratio_def('capital_adequacy_proxy', 'Equity / Liabilities', ['BS.EQ', 'BS.LIAB'], ['insurance'], True, False, 0.20),
                self._ratio_def('net_income_to_assets', 'Net Income / Assets', ['IS.NET', 'BS.ASSETS'], ['insurance'], True, False, 0.15),
                self._ratio_def('equity_ratio', 'Equity / Assets', ['BS.EQ', 'BS.ASSETS'], ['insurance'], True, False, 0.20),
            ],
            'broker_dealer': [
                self._ratio_def('returns_on_assets_proxy', 'Net Income / Assets', ['IS.NET', 'BS.ASSETS'], ['broker_dealer'], True, False, 0.14),
                self._ratio_def('capital_buffer_proxy', 'Equity / Liabilities', ['BS.EQ', 'BS.LIAB'], ['broker_dealer'], True, False, 0.20),
                self._ratio_def('net_income_to_assets', 'Net Income / Assets', ['IS.NET', 'BS.ASSETS'], ['broker_dealer'], True, False, 0.15),
                self._ratio_def('equity_ratio', 'Equity / Assets', ['BS.EQ', 'BS.ASSETS'], ['broker_dealer'], True, False, 0.20),
            ],
            'unknown': [
                self._ratio_def('net_income_to_assets', 'Net Income / Assets', ['IS.NET', 'BS.ASSETS'], ['unknown'], True, False, 0.05),
                self._ratio_def('equity_ratio', 'Equity / Assets', ['BS.EQ', 'BS.ASSETS'], ['unknown'], True, False, 0.05),
            ],
        }

    def _ratio_def(
        self,
        ratio_id: str,
        formula: str,
        dependency_graph: List[str],
        allowed_profiles: List[str],
        use_average_flag: bool,
        forward_looking_flag: bool,
        risk_classification_impact: float,
    ) -> Dict:
        return {
            'ratio_id': ratio_id,
            'formula': formula,
            'dependency_graph': dependency_graph,
            'required_nodes': list(dependency_graph),
            'supporting_nodes': [],
            'allowed_profiles': allowed_profiles,
            'use_average_flag': use_average_flag,
            'forward_looking_flag': forward_looking_flag,
            'risk_classification_impact': risk_classification_impact,
            **get_ratio_metadata(ratio_id),
        }

    def ratio_definitions(self, profile: str) -> List[Dict]:
        defs = list(self.registry.get(profile, self.registry['unknown']))
        bank_only_defs = [
            self._ratio_def('net_interest_margin', 'Net Interest Income / Average Assets', ['IS.NII', 'BS.ASSETS'], ['bank'], True, False, 0.20),
            self._ratio_def('loan_to_deposit_ratio', 'Loans / Deposits', ['BS.LOANS', 'BS.DEPOSITS'], ['bank'], False, False, 0.20),
            self._ratio_def('capital_ratio_proxy', 'CET1 Capital / Assets (fallback Equity/Assets)', ['BS.CET1', 'BS.EQ', 'BS.ASSETS'], ['bank'], True, False, 0.25),
        ]
        existing = {d['ratio_id'] for d in defs}
        for d in bank_only_defs:
            if d['ratio_id'] not in existing:
                defs.append(d)
        return defs

    def compute_reliable(
        self,
        profile: str,
        computed_by_year: Dict[int, Dict[str, float]],
        provenance_ctx: Optional[Dict] = None,
        classification: Optional[Dict] = None,
        mapping_ctx: Optional[Dict] = None,
        normalization_ctx: Optional[Dict] = None,
        reconciliation_ctx: Optional[Dict] = None,
        node_inputs_by_year: Optional[Dict[int, Dict[str, List[Dict]]]] = None,
        company_meta: Optional[Dict] = None,
    ) -> Dict[int, Dict[str, Dict]]:
        years = sorted(computed_by_year.keys())
        defs = self.ratio_definitions(profile)
        raw_series = self._build_raw_series(defs, years, computed_by_year)
        series_ctx = self._build_series_ctx(raw_series)
        exception_ctx_by_year = self.exception_policy.detect_yearly_exceptions(
            years=years,
            computed_by_year=computed_by_year,
            profile=profile,
            classification=classification or {},
            company_meta=company_meta or {},
            normalization_ctx=normalization_ctx or {},
        )
        series_ctx['exception_ctx_by_year'] = exception_ctx_by_year
        out: Dict[int, Dict[str, Dict]] = {}

        for idx, y in enumerate(years):
            row = computed_by_year[y]
            prev = computed_by_year[years[idx - 1]] if idx > 0 else None
            out[y] = {}
            for d in defs:
                rid = d['ratio_id']
                provenance = self._build_provenance(provenance_ctx, company_meta, y)
                inputs_used = self._build_inputs_used(
                    required_nodes=d.get('required_nodes', []),
                    row=row,
                    year=y,
                    node_inputs=node_inputs_by_year.get(y, {}) if node_inputs_by_year else {},
                    provenance=provenance,
                )
                contract = self.pipeline.run_pipeline(
                    ratio_id=rid,
                    profile=profile,
                    ratio_def=d,
                    year=y,
                    row=row,
                    prev=prev,
                    compute_value_fn=self._compute_ratio_value,
                    inputs_used=inputs_used,
                    provenance=provenance,
                    classification=classification or {},
                    mapping_ctx=mapping_ctx or {},
                    normalization_ctx=normalization_ctx or {},
                    reconciliation_ctx=reconciliation_ctx or {},
                    series_ctx=series_ctx,
                    exception_ctx=exception_ctx_by_year.get(y, {}),
                )
                out[y][rid] = contract
        return out

    def compute(
        self,
        profile: str,
        computed_by_year: Dict[int, Dict[str, float]],
        provenance_ctx: Optional[Dict] = None,
        classification: Optional[Dict] = None,
        mapping_ctx: Optional[Dict] = None,
        normalization_ctx: Optional[Dict] = None,
        reconciliation_ctx: Optional[Dict] = None,
        node_inputs_by_year: Optional[Dict[int, Dict[str, List[Dict]]]] = None,
        company_meta: Optional[Dict] = None,
    ) -> Dict[int, Dict[str, float]]:
        reliable = self.compute_reliable(
            profile=profile,
            computed_by_year=computed_by_year,
            provenance_ctx=provenance_ctx,
            classification=classification,
            mapping_ctx=mapping_ctx,
            normalization_ctx=normalization_ctx,
            reconciliation_ctx=reconciliation_ctx,
            node_inputs_by_year=node_inputs_by_year,
            company_meta=company_meta,
        )
        values: Dict[int, Dict[str, float]] = {}
        for y, ratios in reliable.items():
            values[y] = {}
            for rid, obj in ratios.items():
                grade = (((obj or {}).get('reliability') or {}).get('grade'))
                values[y][rid] = obj.get('value') if grade != 'REJECTED' else None
        return values

    def explain(
        self,
        profile: str,
        computed_by_year: Dict[int, Dict[str, float]],
        provenance_ctx: Optional[Dict] = None,
        classification: Optional[Dict] = None,
        mapping_ctx: Optional[Dict] = None,
        normalization_ctx: Optional[Dict] = None,
        reconciliation_ctx: Optional[Dict] = None,
        node_inputs_by_year: Optional[Dict[int, Dict[str, List[Dict]]]] = None,
        company_meta: Optional[Dict] = None,
    ) -> Dict[int, List[Dict]]:
        reliable = self.compute_reliable(
            profile=profile,
            computed_by_year=computed_by_year,
            provenance_ctx=provenance_ctx,
            classification=classification,
            mapping_ctx=mapping_ctx,
            normalization_ctx=normalization_ctx,
            reconciliation_ctx=reconciliation_ctx,
            node_inputs_by_year=node_inputs_by_year,
            company_meta=company_meta,
        )
        defs = {d['ratio_id']: d for d in self.ratio_definitions(profile)}
        out: Dict[int, List[Dict]] = {}
        for y, ratios in reliable.items():
            out[y] = []
            for rid, obj in ratios.items():
                d = defs.get(rid, {})
                rec = {
                    'ratio_id': rid,
                    'value': obj.get('value'),
                    'reliability': obj.get('reliability'),
                    'reasons': obj.get('reasons', []),
                    'diagnostics': obj.get('diagnostics', {}),
                    'inputs_used': obj.get('inputs_used', {}),
                    'provenance': obj.get('provenance', {}),
                    'formula': d.get('formula', obj.get('formula')),
                    'dependencies': d.get('dependency_graph', obj.get('dependencies', [])),
                    'valid_profiles': d.get('allowed_profiles', []),
                    'use_average_flag': d.get('use_average_flag', False),
                    'forward_looking_flag': d.get('forward_looking_flag', False),
                    'risk_classification_impact': d.get('risk_classification_impact', 0.0),
                }
                out[y].append(rec)
        return out

    def _compute_ratio_value(self, *, row: Dict[str, float], prev: Optional[Dict[str, float]], ratio_id: str) -> Tuple[Optional[float], Dict]:
        diag = {'ratio_id': ratio_id, 'denominator_near_zero': False, 'sign_inconsistency': False}
        value = None
        if ratio_id == 'gross_margin':
            value, den = self._safe_div(row.get('IS.GP'), row.get('IS.REV'))
            diag['formula'] = 'IS.GP / IS.REV'
            diag['denominator'] = den
        elif ratio_id == 'operating_margin':
            value, den = self._safe_div(row.get('IS.OP'), row.get('IS.REV'))
            diag['formula'] = 'IS.OP / IS.REV'
            diag['denominator'] = den
        elif ratio_id == 'fcf_margin':
            value, den = self._safe_div(row.get('CF.FCF'), row.get('IS.REV'))
            diag['formula'] = 'CF.FCF / IS.REV'
            diag['denominator'] = den
        elif ratio_id == 'asset_turnover':
            den_avg = self._avg(row.get('BS.ASSETS'), prev.get('BS.ASSETS') if prev else None)
            value, den = self._safe_div(row.get('IS.REV'), den_avg)
            diag['formula'] = 'IS.REV / avg(BS.ASSETS)'
            diag['denominator'] = den
        elif ratio_id == 'inventory_turnover':
            num = abs(row.get('IS.COGS')) if isinstance(row.get('IS.COGS'), (int, float)) else None
            value, den = self._safe_div(num, row.get('BS.INV'))
            diag['formula'] = 'abs(IS.COGS) / BS.INV'
            diag['denominator'] = den
        elif ratio_id == 'net_interest_margin':
            den_avg = self._avg(row.get('BS.ASSETS'), prev.get('BS.ASSETS') if prev else None)
            value, den = self._safe_div(row.get('IS.NII'), den_avg)
            diag['formula'] = 'IS.NII / avg(BS.ASSETS)'
            diag['denominator'] = den
        elif ratio_id == 'loan_to_deposit_ratio':
            value, den = self._safe_div(row.get('BS.LOANS'), row.get('BS.DEPOSITS'))
            diag['formula'] = 'BS.LOANS / BS.DEPOSITS'
            diag['denominator'] = den
        elif ratio_id == 'capital_ratio_proxy':
            den_avg = self._avg(row.get('BS.ASSETS'), prev.get('BS.ASSETS') if prev else None)
            cet1 = row.get('BS.CET1')
            if isinstance(cet1, (int, float)) and cet1 <= 1.0:
                value = float(cet1)
                den = None
            elif isinstance(cet1, (int, float)):
                value, den = self._safe_div(cet1, den_avg)
            else:
                value, den = self._safe_div(row.get('BS.EQ'), den_avg)
            diag['formula'] = 'BS.CET1 / avg(BS.ASSETS) else BS.EQ / avg(BS.ASSETS)'
            diag['denominator'] = den
        elif ratio_id == 'net_income_to_assets':
            den_avg = self._avg(row.get('BS.ASSETS'), prev.get('BS.ASSETS') if prev else None)
            value, den = self._safe_div(row.get('IS.NET'), den_avg)
            diag['formula'] = 'IS.NET / avg(BS.ASSETS)'
            diag['denominator'] = den
        elif ratio_id == 'equity_ratio':
            den_avg = self._avg(row.get('BS.ASSETS'), prev.get('BS.ASSETS') if prev else None)
            value, den = self._safe_div(row.get('BS.EQ'), den_avg)
            diag['formula'] = 'BS.EQ / avg(BS.ASSETS)'
            diag['denominator'] = den
        elif ratio_id == 'combined_proxy':
            num = abs(row.get('IS.COGS')) if isinstance(row.get('IS.COGS'), (int, float)) else None
            value, den = self._safe_div(num, row.get('IS.REV'))
            diag['formula'] = 'abs(IS.COGS) / IS.REV'
            diag['denominator'] = den
        elif ratio_id == 'capital_adequacy_proxy':
            value, den = self._safe_div(row.get('BS.EQ'), row.get('BS.LIAB'))
            diag['formula'] = 'BS.EQ / BS.LIAB'
            diag['denominator'] = den
        elif ratio_id == 'returns_on_assets_proxy':
            den_avg = self._avg(row.get('BS.ASSETS'), prev.get('BS.ASSETS') if prev else None)
            value, den = self._safe_div(row.get('IS.NET'), den_avg)
            diag['formula'] = 'IS.NET / avg(BS.ASSETS)'
            diag['denominator'] = den
        elif ratio_id == 'capital_buffer_proxy':
            value, den = self._safe_div(row.get('BS.EQ'), row.get('BS.LIAB'))
            diag['formula'] = 'BS.EQ / BS.LIAB'
            diag['denominator'] = den
        else:
            value, den = None, None
            diag['formula'] = 'unsupported'

        if den is not None and abs(den) < self.pipeline.DENOM_EPS:
            diag['denominator_near_zero'] = True
        if ratio_id in {'loan_to_deposit_ratio', 'capital_ratio_proxy', 'equity_ratio', 'asset_turnover', 'inventory_turnover'}:
            if isinstance(value, (int, float)) and value < 0:
                diag['sign_inconsistency'] = True
        return value, diag

    def _build_provenance(self, provenance_ctx: Optional[Dict], company_meta: Optional[Dict], year: int) -> Dict:
        provenance_ctx = provenance_ctx or {}
        company_meta = company_meta or {}
        return {
            'cik': company_meta.get('cik') or provenance_ctx.get('cik'),
            'ticker': company_meta.get('ticker') or provenance_ctx.get('ticker'),
            'company_name': company_meta.get('name') or provenance_ctx.get('company_name'),
            'form': provenance_ctx.get('form'),
            'accession': provenance_ctx.get('accession'),
            'filing_date': provenance_ctx.get('filing_date'),
            'period_end': provenance_ctx.get('period_end'),
            'in_range': provenance_ctx.get('in_range'),
            'filing_grade': provenance_ctx.get('filing_grade'),
            'extraction_method': provenance_ctx.get('extraction_method', 'companyfacts_mapped'),
            'year': year,
        }

    def _build_inputs_used(
        self,
        *,
        required_nodes: List[str],
        row: Dict[str, float],
        year: int,
        node_inputs: Dict[str, List[Dict]],
        provenance: Dict,
    ) -> Dict[str, Dict]:
        out = {}
        currency = provenance.get('currency', 'USD')
        for node in required_nodes:
            candidates = node_inputs.get(node, [])
            if candidates:
                # highest absolute value as canonical source for deterministic trace
                best = sorted(candidates, key=lambda x: abs(float((x or {}).get('value') or 0.0)), reverse=True)[0]
                out[node] = {
                    'value': best.get('value'),
                    'unit': best.get('unit') or 'USD',
                    'currency': best.get('currency') or currency,
                    'period_end': best.get('period_end') or f'{year}-12-31',
                    'context_id': best.get('context_id') or f'{year}:{node}',
                    'source_tag': best.get('source_tag') or node,
                    'source_label': best.get('source_label') or node,
                }
            else:
                out[node] = {
                    'value': row.get(node),
                    'unit': 'USD',
                    'currency': currency,
                    'period_end': f'{year}-12-31',
                    'context_id': f'{year}:{node}',
                    'source_tag': node,
                    'source_label': node,
                }
        return out

    def _build_raw_series(self, defs: List[Dict], years: List[int], computed_by_year: Dict[int, Dict[str, float]]) -> Dict[str, Dict[int, Optional[float]]]:
        out: Dict[str, Dict[int, Optional[float]]] = {}
        for d in defs:
            rid = d['ratio_id']
            out[rid] = {}
            for idx, y in enumerate(years):
                row = computed_by_year[y]
                prev = computed_by_year[years[idx - 1]] if idx > 0 else None
                v, _ = self._compute_ratio_value(row=row, prev=prev, ratio_id=rid)
                out[rid][y] = v
        return out

    def _build_series_ctx(self, raw_series: Dict[str, Dict[int, Optional[float]]]) -> Dict:
        yoy_flags = {}
        robust_z = {}
        for rid, by_year in raw_series.items():
            years = sorted(by_year.keys())
            vals_for_z = [by_year[y] for y in years if isinstance(by_year[y], (int, float))]
            med = median(vals_for_z) if vals_for_z else None
            mad = median([abs(v - med) for v in vals_for_z]) if (vals_for_z and med is not None) else None

            for i, y in enumerate(years):
                v = by_year.get(y)
                flags = []
                if i > 0 and isinstance(v, (int, float)):
                    p = by_year.get(years[i - 1])
                    if isinstance(p, (int, float)):
                        if rid in {'gross_margin', 'operating_margin', 'net_margin', 'roa', 'roe', 'net_interest_margin'}:
                            if abs(v - p) > 0.50:
                                flags.append('margin_or_return_delta_gt_0_50')
                        else:
                            denom = abs(p) if abs(p) > 1e-12 else None
                            if denom:
                                yoy = (v - p) / denom
                                if abs(yoy) > 3.0:
                                    flags.append('abs_yoy_growth_gt_3_0')
                yoy_flags[(rid, y)] = flags

                zdiag = {'flagged': False, 'median': med, 'mad': mad, 'z': None, 'threshold': 6.0}
                if isinstance(v, (int, float)) and med is not None and mad not in (None, 0):
                    z = 0.6745 * (v - med) / mad
                    zdiag['z'] = z
                    zdiag['flagged'] = abs(z) > 6.0
                robust_z[(rid, y)] = zdiag
        return {'yoy_flags': yoy_flags, 'robust_z': robust_z}

    @staticmethod
    def _safe_div(a, b) -> Tuple[Optional[float], Optional[float]]:
        if a is None or b is None:
            return None, b
        try:
            av = float(a)
            bv = float(b)
            if abs(bv) < 1e-12:
                return None, bv
            return av / bv, bv
        except Exception:
            return None, b

    @staticmethod
    def _avg(a, b):
        vals = [v for v in [a, b] if isinstance(v, (int, float))]
        if not vals:
            return None
        return sum(vals) / len(vals)
