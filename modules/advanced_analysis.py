# -*- coding: utf-8 -*-
"""
advanced_analysis.py
Advanced analytics that consume ratio engine outputs only.
No direct base-ratio computation is allowed in this module.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np

from .ratio_engine import RatioEngine
from .ratio_source import UnifiedRatioSource, maybe_guard_ratios_by_year


class AdvancedFinancialAnalysis:
    def __init__(self):
        self.risk_free_rate = 0.04
        self.market_risk_premium = 0.08
        self.ratio_engine = RatioEngine()
        self.ratio_source = UnifiedRatioSource()
        self.base_ratios_by_year: Dict[int, Dict[str, Dict]] = {}
        self.financial_items_by_year: Dict[int, Dict] = {}
        self.lockdown_report_rows: List[Dict] = []
        self.direct_compute_errors: List[Dict] = []

    def load_ratio_context(self, data_by_year: Dict, ratios_by_year: Dict, build_diagnostics_data: bool = True) -> None:
        guarded = maybe_guard_ratios_by_year(ratios_by_year or {})
        # Advanced analysis is intentionally decoupled from UI state; infer sector lightly from ratio keys.
        sector_profile = None
        try:
            for _, row in (guarded or {}).items():
                if not isinstance(row, dict):
                    continue
                if any(k in row for k in ("net_interest_margin", "loan_to_deposit_ratio", "capital_ratio_proxy")):
                    sector_profile = "bank"
                    break
        except Exception:
            sector_profile = None
        self.ratio_source.load("CURRENT", data_by_year or {}, guarded, sector_profile=sector_profile)
        if build_diagnostics_data:
            ratio_engine_output = self.ratio_engine.build(data_by_year or {}, guarded)
            self.base_ratios_by_year = ratio_engine_output.get('ratios', {}) or {}
            self.financial_items_by_year = ratio_engine_output.get('items', {}) or {}
            self.lockdown_report_rows = list(ratio_engine_output.get('lockdown_report', []) or [])
        else:
            self.base_ratios_by_year = {}
            self.financial_items_by_year = {}
            self.lockdown_report_rows = []
        self.direct_compute_errors = []

    def _contract(self, year: int, ratio_id: str) -> Dict:
        return self.ratio_source.get_ratio_contract('CURRENT', year, ratio_id)

    def _value(self, year: int, ratio_id: str) -> Optional[float]:
        c = self._contract(year, ratio_id)
        if c.get('source') != 'ratio_engine':
            self._log_direct_compute_error(ratio_id, 'non_ratio_engine_source_blocked')
            return None
        return c.get('value')

    def _log_direct_compute_error(self, metric: str, reason: str) -> None:
        self.direct_compute_errors.append({'metric': metric, 'reason': reason, 'severity': 'ERROR'})

    # ============================================================
    # Forecasts
    # ============================================================
    def forecast_revenue_growth(self, data_by_year: Dict, sgr_internal: float, years_forward: int = 10) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if len(years) < 2:
            return {}
        latest_year = years[-1]
        latest_revenue = (data_by_year[latest_year].get('Revenues') or data_by_year[latest_year].get('SalesRevenueNet'))
        if not isinstance(latest_revenue, (int, float)) or latest_revenue == 0:
            return {}

        sgr = sgr_internal
        if not isinstance(sgr, (int, float)):
            sgr = self._value(latest_year, 'sgr_internal')
        if not isinstance(sgr, (int, float)):
            self._log_direct_compute_error('sgr_internal', 'missing_ratio_from_engine_no_fallback')
            return {}

        hist = []
        for i in range(1, min(5, len(years))):
            curr = data_by_year[years[-i]].get('Revenues') or data_by_year[years[-i]].get('SalesRevenueNet')
            prev = data_by_year[years[-i - 1]].get('Revenues') or data_by_year[years[-i - 1]].get('SalesRevenueNet')
            if isinstance(curr, (int, float)) and isinstance(prev, (int, float)) and prev != 0:
                hist.append((curr - prev) / abs(prev))
        avg_hist = float(np.mean(hist)) if hist else 0.05
        growth = (float(sgr) + avg_hist) / 2.0

        out = {}
        cur = float(latest_revenue)
        for year in range(1, years_forward + 1):
            adj = growth * (0.95 ** year)
            cur = cur * (1 + adj)
            out[latest_year + year] = {'revenue': cur, 'growth_rate': adj}
        return out

    def forecast_dcf(self, data_by_year: Dict, wacc: float, sgr_internal: float, years_forward: int = 10) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if not years or not isinstance(wacc, (int, float)) or wacc <= 0:
            return {}
        latest_year = years[-1]
        latest_ocf = data_by_year[latest_year].get('NetCashProvidedByUsedInOperatingActivities')
        latest_capex = data_by_year[latest_year].get('PaymentsToAcquirePropertyPlantAndEquipment', 0)
        if not isinstance(latest_ocf, (int, float)):
            return {}

        latest_fcf = float(latest_ocf) - float(latest_capex or 0)
        growth = float(sgr_internal) if isinstance(sgr_internal, (int, float)) else 0.05
        fcf_forecasts = []
        pv_fcf = []
        for year in range(1, years_forward + 1):
            adj = growth * (0.95 ** year)
            fcf = latest_fcf * ((1 + adj) ** year)
            fcf_forecasts.append(fcf)
            pv_fcf.append(fcf / ((1 + wacc) ** year))
        terminal_growth = 0.025
        terminal_fcf = fcf_forecasts[-1] * (1 + terminal_growth)
        terminal_value = terminal_fcf / (wacc - terminal_growth) if wacc > terminal_growth else 0.0
        pv_terminal = terminal_value / ((1 + wacc) ** years_forward)
        return {
            'fcf_forecasts': fcf_forecasts,
            'pv_fcf': pv_fcf,
            'terminal_value': terminal_value,
            'pv_terminal': pv_terminal,
            'enterprise_value': sum(pv_fcf) + pv_terminal,
            'total_pv_fcf': sum(pv_fcf),
        }

    def forecast_operating_income(self, data_by_year: Dict, ratios_by_year: Dict, years_forward: int = 10) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if not years:
            return {}
        latest_year = years[-1]
        latest_op_income = data_by_year[latest_year].get('OperatingIncomeLoss')
        if not isinstance(latest_op_income, (int, float)):
            return {}
        op_margins = []
        for y in years[-3:]:
            v = self._value(y, 'operating_margin')
            if isinstance(v, (int, float)):
                op_margins.append(v)
        if not op_margins:
            self._log_direct_compute_error('operating_margin', 'missing_ratio_from_engine_no_fallback')
            return {}
        avg_op_leverage = float(np.mean(op_margins))
        rev_growth = 0.08
        out = {}
        for year in range(1, years_forward + 1):
            op_growth = rev_growth * avg_op_leverage
            adj = op_growth * (0.95 ** year)
            out[latest_year + year] = float(latest_op_income) * ((1 + adj) ** year)
        return out

    def forecast_reinvestment_rate(self, data_by_year: Dict, retention_ratio: float, years_forward: int = 10) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if not years:
            return {}
        latest_year = years[-1]
        latest_ni = data_by_year[latest_year].get('NetIncomeLoss')
        latest_capex = data_by_year[latest_year].get('PaymentsToAcquirePropertyPlantAndEquipment', 0)
        if not isinstance(latest_ni, (int, float)) or latest_ni == 0:
            return {}
        rr = retention_ratio
        if not isinstance(rr, (int, float)):
            rr = self._value(latest_year, 'retention_ratio')
        if not isinstance(rr, (int, float)):
            self._log_direct_compute_error('retention_ratio', 'missing_ratio_from_engine_no_fallback')
            return {}
        cur = abs(float(latest_capex or 0)) / abs(float(latest_ni))
        out = {}
        for year in range(1, years_forward + 1):
            out[latest_year + year] = min(cur * (float(rr) / 0.7), 0.5)
        return out

    def forecast_ccc(self, data_by_year: Dict, ratios_by_year: Dict, years_forward: int = 10) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if not years:
            return {}
        hist = []
        for y in years[-3:]:
            ccc = self._value(y, 'ccc_days')
            if isinstance(ccc, (int, float)):
                hist.append(ccc)
        if not hist:
            self._log_direct_compute_error('ccc_days', 'missing_ratio_from_engine_no_fallback')
            return {}
        avg_ccc = float(np.mean(hist))
        trend = float(np.polyfit(range(len(hist)), hist, 1)[0]) if len(hist) > 1 else 0.0
        latest_year = years[-1]
        out = {}
        for year in range(1, years_forward + 1):
            future = avg_ccc + (trend * year * 0.5)
            if abs(future) > 3650:
                continue
            out[latest_year + year] = future
        return out

    def calculate_terminal_value(self, data_by_year: Dict, roic: float, wacc: float, fcf_year_10: float) -> Dict:
        if not isinstance(roic, (int, float)) or not isinstance(wacc, (int, float)) or not isinstance(fcf_year_10, (int, float)):
            return {}
        terminal_growth = min(0.025, wacc * 0.5)
        terminal_value_gordon = fcf_year_10 * (1 + terminal_growth) / (wacc - terminal_growth) if wacc > terminal_growth else 0
        exit_multiple = 15 if roic > wacc else (12 if roic > wacc * 0.8 else 10)
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        latest_year = years[-1] if years else None
        latest_ebitda = data_by_year.get(latest_year, {}).get('EBITDA') if latest_year else None
        terminal_value_multiple = (latest_ebitda * 1.5 * exit_multiple) if isinstance(latest_ebitda, (int, float)) else (fcf_year_10 * exit_multiple)
        return {
            'terminal_value_gordon': terminal_value_gordon,
            'terminal_value_multiple': terminal_value_multiple,
            'terminal_value_blended': (terminal_value_gordon + terminal_value_multiple) / 2.0,
            'terminal_growth_rate': terminal_growth,
            'exit_multiple': exit_multiple,
        }

    def forecast_default_probability(self, data_by_year: Dict, ratios_by_year: Dict, years_forward: int = 10) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if not years:
            return {}
        recent = years[-3:]
        z_scores = []
        for y in recent:
            z = self._value(y, 'altman_z_score')
            if isinstance(z, (int, float)):
                z_scores.append(z)
        if not z_scores:
            return {}
        avg_z = float(np.mean(z_scores))
        z_trend = float(np.polyfit(range(len(z_scores)), z_scores, 1)[0]) if len(z_scores) > 1 else 0.0
        out = {}
        latest = years[-1]
        for i in range(1, years_forward + 1):
            future_z = avg_z + (z_trend * i)
            if future_z > 2.99:
                p = 0.05
            elif future_z > 1.80:
                p = 0.15 + (2.99 - future_z) * 0.30
            else:
                p = 0.50 + (1.80 - future_z) * 0.25
            p = min(max(p, 0.01), 0.99)
            out[latest + i] = {
                'z_score': future_z,
                'prob_default': p,
                'risk_rating': 'منخفض' if p < 0.15 else ('متوسط' if p < 0.40 else 'مرتفع'),
            }
        return out

    # ============================================================
    # Scenarios
    # ============================================================
    def scenario_analysis(
        self,
        base_data: Dict,
        base_ratios: Dict,
        revenue_growth_range: Tuple[float, float] = (-0.1, 0.2),
        retention_range: Tuple[float, float] = (0.5, 1.0),
        cost_of_debt_range: Tuple[float, float] = (0.02, 0.08),
    ) -> Dict:
        return {
            'pessimistic': {
                'revenue_growth': revenue_growth_range[0],
                'retention_ratio': retention_range[0],
                'cost_of_debt': cost_of_debt_range[1],
                'impact': self._calculate_scenario_impact(
                    base_data, base_ratios, revenue_growth_range[0], retention_range[0], cost_of_debt_range[1]
                ),
            },
            'base': {
                'revenue_growth': float(np.mean(revenue_growth_range)),
                'retention_ratio': float(np.mean(retention_range)),
                'cost_of_debt': float(np.mean(cost_of_debt_range)),
                'impact': self._calculate_scenario_impact(
                    base_data, base_ratios, float(np.mean(revenue_growth_range)), float(np.mean(retention_range)), float(np.mean(cost_of_debt_range))
                ),
            },
            'optimistic': {
                'revenue_growth': revenue_growth_range[1],
                'retention_ratio': retention_range[1],
                'cost_of_debt': cost_of_debt_range[0],
                'impact': self._calculate_scenario_impact(
                    base_data, base_ratios, revenue_growth_range[1], retention_range[1], cost_of_debt_range[0]
                ),
            },
        }

    def _calculate_scenario_impact(self, data: Dict, ratios: Dict, revenue_growth: float, retention: float, cost_of_debt: float) -> Dict:
        years = sorted([y for y in (data or {}).keys() if isinstance(y, int)])
        if not years:
            return {}
        latest = years[-1]
        latest_revenue = (data[latest].get('Revenues') or data[latest].get('SalesRevenueNet') or 0)
        latest_ni = (data[latest].get('NetIncomeLoss') or 0)
        roe = self._value(latest, 'roe')
        if not isinstance(roe, (int, float)):
            self._log_direct_compute_error('roe', 'missing_ratio_from_engine_no_fallback')
            return {}
        sgr = float(retention) * float(roe)
        return {
            'future_revenue_1y': float(latest_revenue) * (1 + float(revenue_growth)),
            'sgr_internal': sgr,
            'future_ni_1y': float(latest_ni) * (1 + sgr),
            'wacc_estimate': (float(cost_of_debt) * 0.3) + 0.06,
            'revenue_change': float(revenue_growth),
            'ni_change': sgr,
        }

    # ============================================================
    # AI Insights
    # ============================================================
    def ai_fraud_probability(self, data_by_year: Dict, ratios_by_year: Dict) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])[-5:]
        if not years:
            return {}
        red_flags = 0
        score = 0.0
        for y in years:
            accruals = self._value(y, 'accruals_ratio')
            net_margin = self._value(y, 'net_margin')
            ocf = (data_by_year.get(y, {}) or {}).get('NetCashProvidedByUsedInOperatingActivities', 0)
            ni = (data_by_year.get(y, {}) or {}).get('NetIncomeLoss', 0)
            if isinstance(accruals, (int, float)) and abs(accruals) > 0.08:
                red_flags += 1
                score += 0.15
            if isinstance(net_margin, (int, float)) and net_margin > 0.10 and isinstance(ocf, (int, float)) and ocf < 0:
                red_flags += 1
                score += 0.20
            if isinstance(ni, (int, float)) and ni != 0 and isinstance(ocf, (int, float)) and abs((ni - ocf) / ni) > 0.30:
                red_flags += 1
                score += 0.15
        prob = min(score / max(1, len(years)), 0.95)
        return {
            'fraud_probability': prob,
            'red_flags_count': red_flags,
            'risk_level': 'منخفض' if prob < 0.20 else ('متوسط' if prob < 0.50 else 'مرتفع'),
            'recommendation': 'آمن' if prob < 0.20 else ('مراجعة دقيقة' if prob < 0.50 else 'تجنب'),
        }

    def dynamic_failure_prediction(self, data_by_year: Dict, ratios_by_year: Dict) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])[-5:]
        if not years:
            return {}
        z_scores, debt_ebitda, interest_cov = [], [], []
        current_ratios, ocf_margins = [], []
        for y in years:
            z = self._value(y, 'altman_z_score')
            if isinstance(z, (int, float)):
                z_scores.append(z)
            de = self._value(y, 'net_debt_ebitda')
            if isinstance(de, (int, float)):
                debt_ebitda.append(de)
            ic = self._value(y, 'interest_coverage')
            if isinstance(ic, (int, float)):
                interest_cov.append(ic)
            cr = self._value(y, 'current_ratio')
            if isinstance(cr, (int, float)):
                current_ratios.append(cr)
            om = self._value(y, 'ocf_margin')
            if isinstance(om, (int, float)):
                ocf_margins.append(om)

        risk_points = 0.0
        if z_scores:
            z_last = z_scores[-1]
            if z_last < 1.2:
                risk_points += 42.0
            elif z_last < 1.8:
                risk_points += 28.0
            elif z_last < 2.5:
                risk_points += 12.0
            if len(z_scores) >= 2 and (z_scores[-1] - z_scores[0]) < -0.6:
                risk_points += 10.0
        if debt_ebitda:
            avg_de = float(np.mean(debt_ebitda))
            if avg_de < 0:
                risk_points -= 8.0
            elif avg_de > 6:
                risk_points += 24.0
            elif avg_de > 4:
                risk_points += 14.0
            elif avg_de > 2.5:
                risk_points += 7.0
        if interest_cov:
            avg_ic = float(np.mean(interest_cov))
            if avg_ic < 1.5:
                risk_points += 22.0
            elif avg_ic < 3:
                risk_points += 10.0
            elif avg_ic > 8:
                risk_points -= 5.0
        if current_ratios:
            avg_cr = float(np.mean(current_ratios))
            if avg_cr < 1.0:
                risk_points += 10.0
            elif avg_cr > 1.4:
                risk_points -= 4.0
        if ocf_margins:
            avg_om = float(np.mean(ocf_margins))
            if avg_om < 0:
                risk_points += 8.0
            elif avg_om > 0.08:
                risk_points -= 3.0

        risk_points = max(0.0, min(100.0, risk_points))
        p3 = min(0.90, max(0.02, float(1.0 / (1.0 + np.exp(-(risk_points - 35.0) / 11.0)))))
        p5 = min(0.95, max(p3, p3 + (0.08 if p3 < 0.35 else 0.12)))
        return {
            'failure_prob_3y': p3,
            'failure_prob_5y': p5,
            'risk_level': 'منخفض' if p3 < 0.15 else ('متوسط' if p3 < 0.40 else 'مرتفع'),
            'key_concerns': self._identify_failure_concerns(z_scores, debt_ebitda, interest_cov),
        }

    def _identify_failure_concerns(self, z_scores, debt_ratios, interest_cov):
        c = []
        if z_scores and z_scores[-1] < 1.80:
            c.append("Z-Score منخفض جداً")
        if debt_ratios and float(np.mean(debt_ratios)) > 5:
            c.append("ديون مرتفعة مقارنة بالأرباح")
        if interest_cov and float(np.mean(interest_cov)) < 2:
            c.append("صعوبة في تغطية الفوائد")
        return c if c else ["لا توجد مخاوف كبيرة"]

    def growth_sustainability_grade(self, data_by_year: Dict, ratios_by_year: Dict) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if not years:
            return {}
        latest = years[-1]
        sgr = self._value(latest, 'sgr_internal')
        roic = self._value(latest, 'roic')
        retention = self._value(latest, 'retention_ratio')
        if not isinstance(sgr, (int, float)):
            self._log_direct_compute_error('sgr_internal', 'missing_ratio_from_engine_no_fallback')
        if not isinstance(retention, (int, float)):
            self._log_direct_compute_error('retention_ratio', 'missing_ratio_from_engine_no_fallback')

        if len(years) >= 2:
            curr_ni = (data_by_year[years[-1]].get('NetIncomeLoss') or 0)
            prev_ni = (data_by_year[years[-2]].get('NetIncomeLoss') or 1)
            actual_growth = (curr_ni - prev_ni) / abs(prev_ni) if prev_ni != 0 else 0
        else:
            actual_growth = 0

        score = 0.0
        if isinstance(roic, (int, float)):
            if roic >= 0.18:
                score += 26.0
            elif roic >= 0.12:
                score += 20.0
            elif roic >= 0.08:
                score += 12.0
            else:
                score += 5.0
        if isinstance(retention, (int, float)):
            if retention >= 0.85:
                score += 18.0
            elif retention >= 0.65:
                score += 14.0
            elif retention >= 0.45:
                score += 9.0
            else:
                score += 4.0
        if isinstance(sgr, (int, float)):
            growth_gap = float(actual_growth) - float(sgr)
            if growth_gap <= 0.05:
                score += 24.0
            elif growth_gap <= 0.12:
                score += 17.0
            elif growth_gap <= 0.25:
                score += 10.0
            else:
                score += 3.0
        else:
            score += 8.0

        ni_vals = []
        for yy in years[-5:]:
            ni_v = (data_by_year.get(yy, {}) or {}).get('NetIncomeLoss')
            if isinstance(ni_v, (int, float)):
                ni_vals.append(float(ni_v))
        if len(ni_vals) >= 3:
            ni_mean = max(1e-9, abs(float(np.mean(ni_vals))))
            ni_std = float(np.std(ni_vals))
            cv = ni_std / ni_mean
            if cv < 0.25:
                score += 12.0
            elif cv < 0.45:
                score += 8.0
            elif cv < 0.75:
                score += 4.0

        debt_warning = False
        if len(years) >= 2:
            dl = data_by_year[years[-1]].get('Liabilities', 0)
            dp = data_by_year[years[-2]].get('Liabilities', 1)
            debt_growth = (dl - dp) / abs(dp) if dp != 0 else 0
            if debt_growth > max(actual_growth * 1.5, 0.20):
                debt_warning = True
                score -= 14.0

        score = max(0.0, min(100.0, score))
        grade = 'A' if score >= 80 else ('B' if score >= 62 else ('C' if score >= 45 else 'D'))
        return {
            'sustainability_score': round(score, 1),
            'grade': grade,
            'sgr_internal': sgr,
            'actual_growth': actual_growth,
            'roic': roic,
            'retention_ratio': retention,
            'debt_warning': debt_warning,
            'assessment': 'نمو مستدام' if grade in ['A', 'B'] else ('نمو محفوف بالمخاطر' if grade == 'C' else 'نمو غير مستدام'),
        }

    def working_capital_ai_analysis(self, data_by_year: Dict, ratios_by_year: Dict) -> Dict:
        sub_sector = str((ratios_by_year or {}).get('_sub_sector_profile') or (ratios_by_year or {}).get('_sector_profile') or '').strip().lower()
        is_bank_like = sub_sector in {'bank', 'commercial_bank', 'investment_bank', 'universal_bank'} or sub_sector.endswith('_bank')
        if is_bank_like:
            return {
                'latest_ccc': None,
                'ccc_trend': None,
                'liquidity_crisis_prob': None,
                'risk_level': 'غير مطبق',
                'recommendation': 'استخدم NIM وLDR بدل CCC',
                'reason': 'BLOCKED_FOR_BANK_MODEL',
            }
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])[-5:]
        ccc_values = []
        for y in years:
            c = self._value(y, 'ccc_days')
            if isinstance(c, (int, float)):
                ccc_values.append(c)
        if not ccc_values:
            self._log_direct_compute_error('ccc_days', 'missing_ratio_from_engine_no_fallback')
            return {}
        ccc_trend = float(np.polyfit(range(len(ccc_values)), ccc_values, 1)[0]) if len(ccc_values) > 1 else 0.0
        latest_ccc = ccc_values[-1]
        risk = 0.0
        if latest_ccc > 90:
            risk += 0.30
        elif latest_ccc > 60:
            risk += 0.15
        if ccc_trend > 5:
            risk += 0.25
        if len(years) >= 2:
            rl = (data_by_year[years[-1]].get('Revenues') or data_by_year[years[-1]].get('SalesRevenueNet') or 0)
            rp = (data_by_year[years[-2]].get('Revenues') or data_by_year[years[-2]].get('SalesRevenueNet') or 1)
            rg = (rl - rp) / abs(rp) if rp != 0 else 0
            if rg > 0.15 and latest_ccc > 60:
                risk += 0.20
        p = min(risk, 0.90)
        return {
            'latest_ccc': latest_ccc,
            'ccc_trend': ccc_trend,
            'liquidity_crisis_prob': p,
            'risk_level': 'منخفض' if p < 0.20 else ('متوسط' if p < 0.50 else 'مرتفع'),
            'recommendation': 'جيد' if p < 0.20 else ('مراقبة' if p < 0.50 else 'تحسين عاجل'),
        }

    def ai_investment_quality_score(self, data_by_year: Dict, ratios_by_year: Dict, investment_score: float, economic_spread: float, fcf_yield: float) -> Dict:
        years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
        if not years:
            return {}
        latest = years[-1]
        roic = self._value(latest, 'roic') or 0
        roe = self._value(latest, 'roe') or 0
        z = self._value(latest, 'altman_z_score') or 0
        sub_sector = str((ratios_by_year or {}).get('_sub_sector_profile') or (ratios_by_year or {}).get('_sector_profile') or 'industrial').strip().lower()
        is_bank_like = sub_sector in {'bank', 'commercial_bank', 'investment_bank', 'universal_bank'} or sub_sector.endswith('_bank')
        economic_spread = self._get_economic_spread(
            data_by_year=data_by_year,
            ratios_by_year=ratios_by_year,
            latest_year=latest,
            sub_sector=sub_sector,
            spread_input=economic_spread,
        )

        score = 0.0
        if roic >= 0.18:
            score += 18.0
        elif roic >= 0.12:
            score += 14.0
        elif roic >= 0.08:
            score += 10.0
        if roe >= 0.20:
            score += 12.0
        elif roe >= 0.12:
            score += 8.0
        elif roe >= 0.06:
            score += 5.0

        if z > 3.0:
            score += 18.0
        elif z > 2.2:
            score += 13.0
        elif z > 1.8:
            score += 9.0
        nd_eb = self._value(latest, 'net_debt_ebitda')
        if isinstance(nd_eb, (int, float)):
            if nd_eb < 0:
                score += 7.0
            elif nd_eb < 2.5:
                score += 5.0
            elif nd_eb > 5:
                score -= 5.0

        if not is_bank_like and isinstance(fcf_yield, (int, float)):
            if fcf_yield > 0.06:
                score += 14.0
            elif fcf_yield > 0.03:
                score += 10.0
            elif fcf_yield > 0.0:
                score += 6.0
            elif fcf_yield > -0.01:
                score += 2.0
        elif is_bank_like:
            fcf_yield = None
        ocf_margin = self._value(latest, 'ocf_margin')
        if isinstance(ocf_margin, (int, float)):
            if ocf_margin > 0.14:
                score += 6.0
            elif ocf_margin > 0.08:
                score += 4.0
            elif ocf_margin > 0:
                score += 2.0

        if isinstance(economic_spread, (int, float)):
            if economic_spread > 0.10:
                score += 10.0
            elif economic_spread > 0.04:
                score += 7.0
            elif economic_spread > 0:
                score += 4.0
        if is_bank_like:
            nim = self._value(latest, 'nim') or self._value(latest, 'net_interest_margin')
            roe_spread = self._value(latest, 'roe_spread')
            capital_ratio = self._value(latest, 'capital_ratio') or self._value(latest, 'cet1_ratio')
            if isinstance(nim, (int, float)):
                if nim >= 0.03:
                    score += 12.0
                elif nim >= 0.02:
                    score += 8.0
                elif nim > 0:
                    score += 4.0
            if isinstance(roe_spread, (int, float)):
                if roe_spread >= 0.04:
                    score += 12.0
                elif roe_spread >= 0.02:
                    score += 8.0
                elif roe_spread > 0:
                    score += 4.0
            if isinstance(capital_ratio, (int, float)):
                if capital_ratio >= 0.11:
                    score += 8.0
                elif capital_ratio >= 0.09:
                    score += 5.0
        # Sector-aware quality boosts to avoid unfairly penalizing
        # financial/insurance/hyper-quality profiles.
        if sub_sector in {"semiconductor_fabless", "hardware_platform"}:
            if isinstance(roic, (int, float)) and roic >= 0.30:
                score += 10.0
            if isinstance(roe, (int, float)) and roe >= 0.35:
                score += 8.0
            if isinstance(z, (int, float)) and z >= 4.0:
                score += 6.0
            if isinstance(investment_score, (int, float)) and investment_score >= 90:
                score += 8.0
        if sub_sector == "insurance_life":
            if isinstance(roe, (int, float)) and roe >= 0.12:
                score += 12.0
            if isinstance(fcf_yield, (int, float)) and fcf_yield >= 0.10:
                score += 12.0
            if isinstance(economic_spread, (int, float)) and economic_spread > 0:
                score += 8.0
        if investment_score and investment_score > 70:
            score += 8.0
        elif investment_score and investment_score > 50:
            score += 5.0

        rev_growth = self._value(latest, 'revenue_growth') or self._value(latest, 'ni_growth')
        if isinstance(rev_growth, (int, float)):
            if rev_growth > 0.20:
                score += 10.0
            elif rev_growth > 0.10:
                score += 7.0
            elif rev_growth > 0:
                score += 4.0

        score = max(0.0, min(100.0, score))

        if score >= 85:
            verdict, action = "جوهرة مخفية 💎", "شراء قوي"
        elif score >= 70:
            verdict, action = "استثمار ممتاز ⭐", "شراء"
        elif score >= 55:
            verdict, action = "استثمار جيد ✓", "احتفاظ/شراء"
        elif score >= 40:
            verdict, action = "متوسط الجودة ⚠️", "احتفاظ"
        elif score >= 25:
            verdict, action = "فخ قيمة 🚨", "تجنب"
        else:
            verdict, action = "نمو مفرط/خطر عالي ❌", "بيع/تجنب"
        return {
            'quality_score': round(score, 1),
            'verdict': verdict,
            'action': action,
            'percentile': min(round(score, 1), 99),
            'components': {
                'economic_spread': economic_spread,
                'fcf_yield': None if is_bank_like else fcf_yield,
                'investment_score': investment_score,
                'roic': roic,
                'z_score': z,
            },
        }

    def _get_economic_spread(
        self,
        data_by_year: Dict,
        ratios_by_year: Dict,
        latest_year: int,
        sub_sector: str,
        spread_input: Optional[float] = None,
    ) -> float:
        """
        Sector-aware economic spread:
        - Industrial/default: ROIC - WACC
        - Financial/insurance: alternative proxies to avoid false zero scoring
        """
        def _f(v):
            try:
                if v is None:
                    return None
                return float(v)
            except Exception:
                return None

        spread = _f(spread_input)
        if spread is not None and abs(spread) < 5:
            return spread

        roic = _f(self._value(latest_year, 'roic'))
        wacc = _f(self._value(latest_year, 'wacc'))
        roe = _f(self._value(latest_year, 'roe'))
        fcf = _f(self._value(latest_year, 'fcf_yield'))
        net_margin = _f(self._value(latest_year, 'net_margin'))
        combined = _f(self._value(latest_year, 'combined_proxy'))
        if combined is None:
            combined = _f(self._value(latest_year, 'combined_ratio'))

        if isinstance(roic, (int, float)) and isinstance(wacc, (int, float)):
            spread = roic - wacc
            if abs(spread) < 5:
                return spread

        if sub_sector == 'insurance_life':
            if isinstance(roe, (int, float)) and isinstance(wacc, (int, float)):
                return float(roe) - float(wacc)
            if isinstance(fcf, (int, float)):
                return float(fcf) - 0.05
            return 0.02

        if sub_sector == 'insurance_broker':
            if isinstance(net_margin, (int, float)):
                return float(net_margin) - 0.10
            return 0.05

        if sub_sector in ('commercial_bank', 'bank', 'investment_bank'):
            if isinstance(roe, (int, float)):
                return float(roe) - 0.10
            return 0.0

        if sub_sector == 'insurance_pc':
            if isinstance(combined, (int, float)):
                return max(0.0, 1.0 - float(combined))
            return 0.02

        if sub_sector == 'consumer_staples':
            if isinstance(roic, (int, float)) and isinstance(wacc, (int, float)):
                return float(roic) - float(wacc)
            return 0.05

        return 0.0

    def build_diagnostics(self) -> Dict:
        metrics = {}
        for y, row in sorted(self.base_ratios_by_year.items()):
            y_key = str(y)
            metrics[y_key] = {}
            for ratio_id, c in sorted(row.items()):
                metrics[y_key][ratio_id] = {
                    'value': c.get('value'),
                    'reliability': c.get('reliability'),
                    'reason': c.get('reason'),
                    'source': c.get('source'),
                }
        return {
            'metrics': metrics,
            'items': {str(y): v for y, v in sorted(self.financial_items_by_year.items())},
            'lockdown_report_count': len(self.lockdown_report_rows),
            'errors': list(self.direct_compute_errors),
        }


def generate_comprehensive_forecast(
    data_by_year: Dict,
    ratios_by_year: Dict,
    sgr: float,
    wacc: float,
    roic: float,
    include_diagnostics: bool = False,
) -> Dict:
    analyzer = AdvancedFinancialAnalysis()
    analyzer.load_ratio_context(
        data_by_year,
        ratios_by_year,
        build_diagnostics_data=include_diagnostics,
    )
    dcf = analyzer.forecast_dcf(data_by_year, wacc, sgr)
    return {
        'revenue_forecast': analyzer.forecast_revenue_growth(data_by_year, sgr),
        'dcf_analysis': dcf,
        'operating_income_forecast': analyzer.forecast_operating_income(data_by_year, ratios_by_year),
        'reinvestment_forecast': analyzer.forecast_reinvestment_rate(data_by_year, None),
        'ccc_forecast': analyzer.forecast_ccc(data_by_year, ratios_by_year),
        'terminal_value': analyzer.calculate_terminal_value(
            data_by_year,
            roic,
            wacc,
            dcf.get('fcf_forecasts', [0])[-1] if dcf.get('fcf_forecasts') else 0,
        ),
        'default_probability': analyzer.forecast_default_probability(data_by_year, ratios_by_year),
    }


def generate_ai_insights(
    data_by_year: Dict,
    ratios_by_year: Dict,
    investment_score: float,
    economic_spread: float,
    fcf_yield: float,
    sub_sector: Optional[str] = None,
    persist_artifacts: bool = True,
    include_diagnostics: bool = True,
) -> Dict:
    analyzer = AdvancedFinancialAnalysis()
    if isinstance(ratios_by_year, dict) and sub_sector:
        ratios_by_year = dict(ratios_by_year)
        ratios_by_year['_sub_sector_profile'] = sub_sector
    analyzer.load_ratio_context(
        data_by_year,
        ratios_by_year,
        build_diagnostics_data=(persist_artifacts or include_diagnostics),
    )
    insights = {
        'fraud_detection': analyzer.ai_fraud_probability(data_by_year, ratios_by_year),
        'failure_prediction': analyzer.dynamic_failure_prediction(data_by_year, ratios_by_year),
        'growth_sustainability': analyzer.growth_sustainability_grade(data_by_year, ratios_by_year),
        'working_capital_analysis': analyzer.working_capital_ai_analysis(data_by_year, ratios_by_year),
        'investment_quality': analyzer.ai_investment_quality_score(
            data_by_year, ratios_by_year, investment_score, economic_spread, fcf_yield
        ),
    }
    diagnostics = analyzer.build_diagnostics() if (persist_artifacts or include_diagnostics) else {}
    if persist_artifacts:
        out = Path('exports/sector_comparison')
        out.mkdir(parents=True, exist_ok=True)
        (out / 'advanced_analysis_diagnostics.json').write_text(
            json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding='utf-8'
        )
        (out / 'data_integrity_lockdown_report.json').write_text(
            json.dumps(analyzer.lockdown_report_rows, ensure_ascii=False, indent=2), encoding='utf-8'
        )
        # RatioEngine writes these files during build; we also mirror payloads here for deterministic AI tier artifacts.
        if (out / 'data_integrity_diagnostics.json').exists() is False:
            (out / 'data_integrity_diagnostics.json').write_text(
                json.dumps(diagnostics.get('items', {}), ensure_ascii=False, indent=2), encoding='utf-8'
            )
        if (out / 'ratio_reliability_report.json').exists() is False:
            flat = []
            for y, row in sorted(analyzer.base_ratios_by_year.items()):
                for rid, c in sorted((row or {}).items()):
                    flat.append({
                        'year': y,
                        'ratio_id': rid,
                        'value': c.get('value'),
                        'reliability': c.get('reliability'),
                        'reason': c.get('reason'),
                        'source': c.get('source'),
                    })
            (out / 'ratio_reliability_report.json').write_text(
                json.dumps(flat, ensure_ascii=False, indent=2), encoding='utf-8'
            )
    if include_diagnostics:
        insights['advanced_analysis_diagnostics'] = diagnostics
    return insights
