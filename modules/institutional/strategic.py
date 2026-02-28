from __future__ import annotations

from typing import Dict, List


class StrategicIntelligenceEngine:
    """
    Multi-year strategic behavior analytics.
    """

    def analyze(self, computed_by_year: Dict[int, Dict[str, float]], profile: str) -> Dict:
        years = sorted(computed_by_year.keys())
        if not years:
            return {
                'cagr': {},
                'margin_expansion': {},
                'segment_evolution': {},
                'capital_allocation_efficiency': {},
                'operating_leverage': {},
                'strategic_shifts': [],
                'competitive_positioning_trend': 'insufficient_data',
            }

        cagr = self._compute_cagr(computed_by_year, years)
        margin_expansion = self._margin_expansion(computed_by_year, years)
        segment_evolution = self._segment_evolution(computed_by_year, years)
        cap_alloc = self._capital_allocation_efficiency(computed_by_year, years)
        op_lev = self._operating_leverage(computed_by_year, years)
        shifts = self._strategic_shifts(cagr, margin_expansion, cap_alloc)
        positioning = self._competitive_positioning(cagr, margin_expansion, profile)

        return {
            'cagr': cagr,
            'margin_expansion': margin_expansion,
            'segment_evolution': segment_evolution,
            'capital_allocation_efficiency': cap_alloc,
            'operating_leverage': op_lev,
            'strategic_shifts': shifts,
            'competitive_positioning_trend': positioning,
        }

    def _compute_cagr(self, by_year: Dict[int, Dict[str, float]], years: List[int]) -> Dict:
        out = {}
        if len(years) < 2:
            return out
        y0, y1 = years[0], years[-1]
        n = max(1, y1 - y0)

        for label, node in [('revenue_cagr', 'IS.REV'), ('net_income_cagr', 'IS.NET'), ('fcf_cagr', 'CF.FCF')]:
            a = by_year[y0].get(node)
            b = by_year[y1].get(node)
            if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a > 0 and b > 0:
                out[label] = (b / a) ** (1 / n) - 1
            else:
                out[label] = None
        return out

    def _margin_expansion(self, by_year: Dict[int, Dict[str, float]], years: List[int]) -> Dict:
        if len(years) < 2:
            return {'start_margin': None, 'end_margin': None, 'expansion': None}
        y0, y1 = years[0], years[-1]
        m0 = self._margin(by_year[y0])
        m1 = self._margin(by_year[y1])
        return {'start_margin': m0, 'end_margin': m1, 'expansion': (m1 - m0) if m0 is not None and m1 is not None else None}

    def _segment_evolution(self, by_year: Dict[int, Dict[str, float]], years: List[int]) -> Dict:
        # Proxy: concentration shift in revenue-related concept distribution
        out = {'concentration_shift': None, 'trend': 'stable'}
        if len(years) < 2:
            return out
        first = self._segment_proxy(by_year[years[0]])
        last = self._segment_proxy(by_year[years[-1]])
        shift = abs(last - first)
        out['concentration_shift'] = shift
        if shift > 0.20:
            out['trend'] = 'major_evolution'
        elif shift > 0.08:
            out['trend'] = 'moderate_evolution'
        return out

    def _capital_allocation_efficiency(self, by_year: Dict[int, Dict[str, float]], years: List[int]) -> Dict:
        vals = []
        for y in years:
            r = by_year[y]
            fcf = r.get('CF.FCF')
            assets = r.get('BS.ASSETS')
            if isinstance(fcf, (int, float)) and isinstance(assets, (int, float)) and assets != 0:
                vals.append(fcf / assets)
        score = sum(vals) / max(1, len(vals)) if vals else None
        return {'fcf_to_assets_avg': score, 'efficiency_level': self._bucket(score, [0.02, 0.05])}

    def _operating_leverage(self, by_year: Dict[int, Dict[str, float]], years: List[int]) -> Dict:
        pairs = []
        for i in range(1, len(years)):
            y0, y1 = years[i - 1], years[i]
            rev_g = self._growth(by_year[y0].get('IS.REV'), by_year[y1].get('IS.REV'))
            op_g = self._growth(by_year[y0].get('IS.OP'), by_year[y1].get('IS.OP'))
            if rev_g is not None and op_g is not None and rev_g != 0:
                pairs.append(op_g / rev_g)
        avg = sum(pairs) / max(1, len(pairs)) if pairs else None
        return {'operating_leverage_ratio': avg, 'classification': self._bucket(avg, [1.0, 1.5])}

    def _strategic_shifts(self, cagr: Dict, margin_expansion: Dict, cap_alloc: Dict) -> List[str]:
        shifts = []
        rev_cagr = cagr.get('revenue_cagr')
        ni_cagr = cagr.get('net_income_cagr')
        exp = margin_expansion.get('expansion')
        eff = cap_alloc.get('fcf_to_assets_avg')
        if rev_cagr and ni_cagr and ni_cagr > rev_cagr * 1.4:
            shifts.append('profitability-led strategy shift')
        if exp and exp > 0.05:
            shifts.append('material margin expansion strategy')
        if eff and eff > 0.05:
            shifts.append('capital allocation efficiency improvement')
        return shifts

    def _competitive_positioning(self, cagr: Dict, margin_expansion: Dict, profile: str) -> str:
        rev = cagr.get('revenue_cagr')
        exp = margin_expansion.get('expansion')
        if rev is None or exp is None:
            return 'insufficient_data'
        if rev > 0.08 and exp > 0.02:
            return 'strengthening'
        if rev < 0.02 and exp < 0:
            return 'weakening'
        return 'stable'

    def _margin(self, row: Dict[str, float]):
        rev = row.get('IS.REV')
        op = row.get('IS.OP')
        if not isinstance(rev, (int, float)) or rev == 0 or not isinstance(op, (int, float)):
            return None
        return op / rev

    def _growth(self, a, b):
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)) or a == 0:
            return None
        return (b - a) / abs(a)

    def _segment_proxy(self, row: Dict[str, float]) -> float:
        rev = abs(row.get('IS.REV') or 0.0)
        op = abs(row.get('IS.OP') or 0.0)
        if rev == 0:
            return 0.0
        return min(1.0, op / rev)

    def _bucket(self, x, cuts):
        if x is None:
            return 'unknown'
        if x < cuts[0]:
            return 'low'
        if x < cuts[1]:
            return 'medium'
        return 'high'
