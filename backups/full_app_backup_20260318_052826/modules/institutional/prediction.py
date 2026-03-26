from __future__ import annotations

import random
from typing import Dict, Optional


class PredictiveScenarioEngine:
    """
    Forecasts Revenue, EBITDA proxy, Free Cash Flow with scenario inputs.
    Optional Monte Carlo valuation range.
    """

    def forecast(
        self,
        computed_by_year: Dict[int, Dict[str, float]],
        scenario: Optional[Dict] = None,
    ) -> Dict:
        scenario = scenario or {}
        years = sorted(computed_by_year.keys())
        if not years:
            return {'forecast': {}, 'valuation_range': {}}

        latest = computed_by_year[years[-1]]
        rev0 = latest.get('IS.REV') or 0.0
        op0 = latest.get('IS.OP') or 0.0
        fcf0 = latest.get('CF.FCF') or 0.0

        growth = scenario.get('revenue_growth_pct', 0.05)
        margin_delta = scenario.get('margin_change_pct', 0.0)
        capex_intensity = scenario.get('capex_intensity_pct', 0.06)
        interest_shift = scenario.get('interest_rate_change_pct', 0.0)

        horizon = int(scenario.get('horizon_years', 5))

        op_margin0 = (op0 / rev0) if rev0 else 0.12
        out = {}
        rev = rev0

        for i in range(1, horizon + 1):
            year = years[-1] + i
            rev = rev * (1.0 + growth)
            op_margin = max(-0.2, min(0.6, op_margin0 + margin_delta * i))
            ebitda = rev * op_margin
            capex = rev * capex_intensity
            financing_penalty = rev * max(0.0, interest_shift) * 0.01
            fcf = ebitda - capex - financing_penalty
            out[year] = {
                'revenue': rev,
                'ebitda_proxy': ebitda,
                'free_cash_flow': fcf,
            }

        valuation = self._valuation_range(out, scenario)
        mc = self._monte_carlo(out, scenario) if scenario.get('enable_monte_carlo') else None

        return {'forecast': out, 'valuation_range': valuation, 'monte_carlo': mc}

    def _valuation_range(self, forecast: Dict[int, Dict[str, float]], scenario: Dict) -> Dict:
        discount = scenario.get('discount_rate', 0.10)
        terminal_growth = scenario.get('terminal_growth', 0.02)

        pv = 0.0
        years = sorted(forecast.keys())
        for i, y in enumerate(years, start=1):
            fcf = forecast[y].get('free_cash_flow') or 0.0
            pv += fcf / ((1 + discount) ** i)

        last_fcf = forecast[years[-1]].get('free_cash_flow') if years else 0.0
        terminal = (last_fcf * (1 + terminal_growth) / max(1e-6, (discount - terminal_growth))) if years else 0.0
        terminal_pv = terminal / ((1 + discount) ** max(1, len(years)))
        base = pv + terminal_pv

        return {
            'bear': base * 0.75,
            'base': base,
            'bull': base * 1.25,
        }

    def _monte_carlo(self, forecast: Dict[int, Dict[str, float]], scenario: Dict) -> Dict:
        runs = int(scenario.get('mc_runs', 500))
        discount = scenario.get('discount_rate', 0.10)
        samples = []

        for _ in range(runs):
            pv = 0.0
            for i, y in enumerate(sorted(forecast.keys()), start=1):
                fcf = forecast[y].get('free_cash_flow') or 0.0
                shock = random.gauss(1.0, 0.15)
                pv += (fcf * shock) / ((1 + discount) ** i)
            samples.append(pv)

        samples.sort()
        if not samples:
            return {}
        q10 = samples[int(0.10 * (len(samples) - 1))]
        q50 = samples[int(0.50 * (len(samples) - 1))]
        q90 = samples[int(0.90 * (len(samples) - 1))]
        return {'p10': q10, 'p50': q50, 'p90': q90, 'runs': runs}
