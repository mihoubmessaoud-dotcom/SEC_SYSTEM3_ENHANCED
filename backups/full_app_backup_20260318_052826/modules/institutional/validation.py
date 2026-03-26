from __future__ import annotations

from typing import Dict, List


class FinancialIntegrityEngine:
    """
    Integrity checks and composite confidence/risk scoring.
    """

    def evaluate(
        self,
        computed_by_year: Dict[int, Dict[str, float]],
        ratio_by_year: Dict[int, Dict[str, float]],
        anomaly_flags: List[Dict],
    ) -> Dict:
        years = sorted(computed_by_year.keys())
        findings: List[Dict] = list(anomaly_flags)

        for i in range(1, len(years)):
            y0, y1 = years[i - 1], years[i]
            r0 = computed_by_year[y0]
            r1 = computed_by_year[y1]

            # Sudden margin shifts
            m0 = self._margin(r0)
            m1 = self._margin(r1)
            if m0 is not None and m1 is not None and abs(m1 - m0) > 0.15:
                findings.append({
                    'year': y1,
                    'node_id': 'IS',
                    'flag_type': 'sudden_margin_shift',
                    'details': f'margin changed from {m0:.3f} to {m1:.3f}',
                    'severity': min(1.0, abs(m1 - m0) / 0.3),
                })

            # Cash flow earnings divergence
            ni = r1.get('IS.NET')
            ocf = r1.get('CF.OCF')
            if isinstance(ni, (int, float)) and isinstance(ocf, (int, float)):
                if ni != 0 and abs((ocf - ni) / ni) > 0.50:
                    findings.append({
                        'year': y1,
                        'node_id': 'CF',
                        'flag_type': 'cash_flow_earnings_divergence',
                        'details': f'OCF={ocf}, NI={ni}',
                        'severity': min(1.0, abs((ocf - ni) / ni)),
                    })

            # Artificial smoothing
            if i >= 2:
                y_prev2 = years[i - 2]
                g1 = self._growth(computed_by_year[y_prev2].get('IS.REV'), computed_by_year[y0].get('IS.REV'))
                g2 = self._growth(computed_by_year[y0].get('IS.REV'), computed_by_year[y1].get('IS.REV'))
                if g1 is not None and g2 is not None and abs(g1 - g2) < 0.005:
                    findings.append({
                        'year': y1,
                        'node_id': 'IS.REV',
                        'flag_type': 'possible_artificial_smoothing',
                        'details': f'consecutive near-identical revenue growth ({g1:.4f}, {g2:.4f})',
                        'severity': 0.25,
                    })

        eq_score = self._earnings_quality(computed_by_year)
        stability_score = self._structural_stability(findings)
        risk_score = self._financial_risk(findings, ratio_by_year)
        confidence_score = self._data_confidence(findings, computed_by_year)

        return {
            'findings': findings,
            'earnings_quality_score': eq_score,
            'structural_stability_score': stability_score,
            'financial_risk_score': risk_score,
            'data_confidence_score': confidence_score,
        }

    def _margin(self, row: Dict[str, float]):
        rev = row.get('IS.REV')
        ni = row.get('IS.NET')
        if not isinstance(rev, (int, float)) or rev == 0 or not isinstance(ni, (int, float)):
            return None
        return ni / rev

    def _growth(self, a, b):
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)) or a == 0:
            return None
        return (b - a) / abs(a)

    def _earnings_quality(self, computed_by_year: Dict[int, Dict[str, float]]) -> float:
        vals = []
        for _, row in computed_by_year.items():
            ni = row.get('IS.NET')
            ocf = row.get('CF.OCF')
            if isinstance(ni, (int, float)) and isinstance(ocf, (int, float)) and ni != 0:
                vals.append(max(0.0, 1.0 - abs((ocf - ni) / ni)))
        if not vals:
            return 50.0
        return round(100.0 * sum(vals) / len(vals), 2)

    def _structural_stability(self, findings: List[Dict]) -> float:
        penalties = sum(f.get('severity', 0.1) for f in findings if 'struct' in f.get('flag_type', '') or 'reconciliation' in f.get('flag_type', ''))
        return round(max(0.0, 100.0 - penalties * 20.0), 2)

    def _financial_risk(self, findings: List[Dict], ratio_by_year: Dict[int, Dict[str, float]]) -> float:
        base = 20.0
        sev = sum(f.get('severity', 0.1) for f in findings)
        profile_ratio_penalty = 0.0
        for y, row in ratio_by_year.items():
            for _, v in row.items():
                if isinstance(v, (int, float)) and abs(v) > 2.5:
                    profile_ratio_penalty += 0.05
        return round(min(100.0, base + sev * 25.0 + profile_ratio_penalty * 100.0), 2)

    def _data_confidence(self, findings: List[Dict], computed_by_year: Dict[int, Dict[str, float]]) -> float:
        completeness = []
        for y, row in computed_by_year.items():
            core = ['IS.REV', 'IS.NET', 'BS.ASSETS', 'BS.LIAB', 'BS.EQ', 'CF.OCF']
            present = sum(1 for c in core if c in row and isinstance(row[c], (int, float)))
            completeness.append(present / len(core))
        comp_score = (sum(completeness) / max(1, len(completeness))) * 100.0
        penalty = sum(f.get('severity', 0.1) for f in findings) * 8.0
        return round(max(0.0, comp_score - penalty), 2)
