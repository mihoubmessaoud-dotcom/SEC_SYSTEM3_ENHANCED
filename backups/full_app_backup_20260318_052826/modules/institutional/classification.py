from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


SUPPORTED_SECTORS = ['bank', 'insurance', 'broker_dealer', 'industrial', 'unknown']


@dataclass
class ProfileScore:
    profile: str
    probability: float


class AdaptiveCompanyClassifier:
    """
    Strict, fail-closed sector classifier.

    - Supported sectors only: bank, insurance, broker_dealer, industrial, unknown.
    - Uses SIC/NAICS, XBRL patterns, key concepts, and fact/document patterns.
    - Hard bank gate + strong insurance override + abstain rule.
    """

    MIN_SCORE = 0.28
    MIN_MARGIN = 0.08
    ABS_MIN_USD = 10_000_000.0
    REL_MIN = 0.002

    def __init__(self) -> None:
        self.sector_keywords: Dict[str, List[str]] = {
            'bank': [
                'deposits', 'netinterestincome', 'interestincome', 'interexpense',
                'loans', 'allowanceforcreditloss', 'federalfunds', 'tier1', 'cet1'
            ],
            'insurance': [
                'premium', 'policyholder', 'claims', 'lossadjustment',
                'reinsurance', 'underwriting', 'unearnedpremium'
            ],
            'broker_dealer': [
                'tradingrevenue', 'investmentbanking', 'advisory', 'brokerage',
                'aum', 'assetmanagement', 'principaltransactions'
            ],
            'industrial': [
                'revenue', 'costofrevenue', 'grossprofit', 'operatingincome',
                'inventory', 'propertyplantandequipment', 'sellinggeneralandadministrative'
            ],
        }

        self.sic_naics_weights: Dict[str, List[Tuple[str, float]]] = {
            'bank': [('52', 0.30), ('522', 0.38)],
            'insurance': [('524', 0.42)],
            'broker_dealer': [('523', 0.42)],
            'industrial': [('31', 0.22), ('32', 0.22), ('33', 0.22)],
        }

    def classify(
        self,
        company_meta: Dict,
        xbrl_concepts: Iterable[str],
        data_by_year: Dict[int, Dict[str, float]],
    ) -> Dict:
        concepts = list(xbrl_concepts)
        concepts_l = [c.lower() for c in concepts]
        latest = data_by_year[max(data_by_year.keys())] if data_by_year else {}

        scores = {
            'bank': 0.0,
            'insurance': 0.0,
            'broker_dealer': 0.0,
            'industrial': 0.0,
        }

        # A) Evidence scoring: SIC/NAICS
        sic = str(company_meta.get('sic', '') or '')
        naics = str(company_meta.get('naics', '') or '')
        for sector, pairs in self.sic_naics_weights.items():
            for code, wt in pairs:
                if sic.startswith(code):
                    scores[sector] += wt
                if naics.startswith(code):
                    scores[sector] += wt

        # A) Evidence scoring: taxonomy and concept detection
        for sector, kws in self.sector_keywords.items():
            hits = 0
            for kw in kws:
                if any(kw in c for c in concepts_l):
                    hits += 1
            scores[sector] += min(0.35, (hits / max(1, len(kws))) * 0.35)

        # A) Evidence scoring: document/fact patterns
        pattern_bonus = self._fact_pattern_bonus(latest, concepts_l)
        for s, b in pattern_bonus.items():
            if s in scores:
                scores[s] += b

        indicators = self._detect_indicators(latest, concepts_l)
        materiality = self._materiality_assessment(indicators)

        # Deterministic insurance hard hint for known insurance SIC/NAICS families.
        insurance_code_hit = sic.startswith('63') or naics.startswith('524')
        if insurance_code_hit:
            scores['insurance'] += 0.30

        # C) HARD BANK GATE
        bank_gate = self._hard_bank_gate(indicators, materiality)
        if not bank_gate['passed']:
            scores['bank'] = min(scores['bank'], 0.24)
        else:
            # Real deposit-taking banks can carry broker-like signals.
            # Add a deterministic, bounded boost only when core bank footprint is material.
            bank_strength = self._bank_gate_strength(indicators)
            if bank_strength['material_core_bank_footprint']:
                scores['bank'] += 0.10

        # D) STRONG INSURANCE OVERRIDE
        insurance_override = self._insurance_override(indicators, materiality)
        if insurance_override['triggered']:
            scores['insurance'] += 0.35

        # E) BROKER-DEALER RULE
        broker_rule = self._broker_rule(indicators)
        if broker_rule['triggered']:
            scores['broker_dealer'] += 0.25

        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        top_sector, top_score = ranked[0]
        second_score = ranked[1][1] if len(ranked) > 1 else 0.0

        # B) ABSTAIN RULE
        abstain = (top_score < self.MIN_SCORE) or ((top_score - second_score) < self.MIN_MARGIN)
        if top_sector == 'bank' and not bank_gate['passed']:
            abstain = True

        # insurance override priority where valid
        if insurance_override['triggered'] and (scores['insurance'] - scores['bank']) >= self.MIN_MARGIN:
            top_sector = 'insurance'
            top_score = scores['insurance']
            abstain = False if top_score >= self.MIN_SCORE else True

        sector = 'unknown' if abstain else top_sector
        if insurance_code_hit and sector != 'bank':
            sector = 'insurance'
            abstain = False

        # Normalize into probabilities over supported sectors (unknown gets abstain mass)
        total = sum(max(0.0, v) for v in scores.values())
        probs = {}
        if total <= 0:
            probs = {k: (1.0 / 4.0) for k in scores}
        else:
            probs = {k: max(0.0, v) / total for k, v in scores.items()}

        unknown_prob = 0.0
        if abstain:
            unknown_prob = min(0.85, max(0.35, 1.0 - (top_score + self.MIN_MARGIN)))
            scale = max(1e-9, 1.0 - unknown_prob)
            probs = {k: v * scale for k, v in probs.items()}

        probs['unknown'] = unknown_prob
        # guarantee supported sectors order
        prob_list = [ProfileScore(profile=p, probability=probs.get(p, 0.0)).__dict__ for p in SUPPORTED_SECTORS]
        prob_list = sorted(prob_list, key=lambda d: d['probability'], reverse=True)

        decision_rule = 'abstain_unknown' if abstain else 'max_score'
        if sector == 'insurance' and insurance_override['triggered']:
            decision_rule = 'insurance_override'
        if sector == 'bank' and bank_gate['passed']:
            decision_rule = 'bank_hard_gate_passed'
        if sector == 'insurance' and insurance_code_hit:
            decision_rule = 'insurance_sic_naics_override'

        triggered_rules = []
        if bank_gate['passed']:
            triggered_rules.append('bank_hard_gate')
        if insurance_override['triggered']:
            triggered_rules.append('insurance_override')
        if broker_rule['triggered']:
            triggered_rules.append('broker_rule')
        if insurance_code_hit:
            triggered_rules.append('insurance_sic_naics_override')

        winning_evidence = self._build_sector_evidence(sector, indicators, materiality, bank_gate, insurance_override, broker_rule)
        losing_evidence = {
            s: self._build_sector_evidence(s, indicators, materiality, bank_gate, insurance_override, broker_rule)
            for s in ['bank', 'insurance', 'broker_dealer', 'industrial']
            if s != sector
        }

        diagnostics = {
            'final_sector': sector,
            'sector': sector,
            'confidence_score': round(float(max(0.0, min(1.0, probs.get(sector, 0.0))) * 100.0), 2),
            'decision_rule': decision_rule,
            'triggered_rules': triggered_rules,
            'detected_indicators': indicators,
            'scores': {k: float(round(v, 6)) for k, v in scores.items()},
            'thresholds': {'MIN_SCORE': self.MIN_SCORE, 'MIN_MARGIN': self.MIN_MARGIN},
            'materiality': materiality,
            'bank_gate': bank_gate,
            'bank_gate_strength': bank_strength if bank_gate['passed'] else {
                'material_core_bank_footprint': False,
                'ratios': {
                    'deposits_to_assets': 0.0,
                    'loans_to_assets': 0.0,
                    'nii_to_assets': 0.0,
                },
                'thresholds': {
                    'deposits_to_assets_min': 0.15,
                    'loans_to_assets_min': 0.05,
                    'nii_to_assets_min': 0.001,
                },
            },
            'insurance_override': insurance_override,
            'broker_rule': broker_rule,
            'abstained': abstain,
            'winning_evidence': winning_evidence,
            'losing_evidence': losing_evidence,
        }

        hybrid_profiles = [d['profile'] for d in prob_list if d['profile'] != 'unknown' and d['probability'] >= 0.20]
        return {
            'primary_profile': sector,
            'profile_probabilities': prob_list,
            'is_hybrid': len(hybrid_profiles) > 1,
            'hybrid_profiles': hybrid_profiles,
            'classification_signals': {
                'sic': sic,
                'naics': naics,
                'scores': diagnostics['scores'],
            },
            'classifier_diagnostics': diagnostics,
        }

    def _build_sector_evidence(self, sector: str, indicators: Dict, materiality: Dict, bank_gate: Dict, insurance_override: Dict, broker_rule: Dict) -> Dict:
        if sector == 'bank':
            return {
                'deposits_material': bool((materiality.get('deposits') or {}).get('is_material')),
                'nii_material': bool((materiality.get('nii') or {}).get('is_material')),
                'loans_present': bool((indicators.get('loans') or {}).get('present')),
                'bank_gate_passed': bool((bank_gate or {}).get('passed')),
            }
        if sector == 'insurance':
            return {
                'premiums_material': bool((materiality.get('premiums') or {}).get('is_material')),
                'policyholder_liabilities_material': bool((materiality.get('policyholder_liabilities') or {}).get('is_material')),
                'insurance_override_triggered': bool((insurance_override or {}).get('triggered')),
            }
        if sector == 'broker_dealer':
            return {
                'trading_revenue_present': bool((indicators.get('trading_revenue') or {}).get('present')),
                'ib_fees_present': bool((indicators.get('investment_banking_fees') or {}).get('present')),
                'broker_rule_triggered': bool((broker_rule or {}).get('triggered')),
            }
        return {
            'revenue_present': bool((indicators.get('revenue') or {}).get('present')),
            'inventory_present': bool((indicators.get('inventory') or {}).get('present')) if 'inventory' in indicators else False,
            'deposits_present': bool((indicators.get('deposits') or {}).get('present')),
            'premiums_present': bool((indicators.get('premiums') or {}).get('present')),
        }

    def _fact_pattern_bonus(self, latest: Dict[str, float], concepts_l: List[str]) -> Dict[str, float]:
        out = {'bank': 0.0, 'insurance': 0.0, 'broker_dealer': 0.0, 'industrial': 0.0}
        if not latest:
            return out

        # mild, conservative bonuses
        deposit_hits = sum(1 for c in concepts_l if 'deposit' in c)
        premium_hits = sum(1 for c in concepts_l if 'premium' in c)
        trading_hits = sum(1 for c in concepts_l if 'trading' in c or 'brokerage' in c)
        inventory_hits = sum(1 for c in concepts_l if 'inventory' in c)

        if deposit_hits >= 4:
            out['bank'] += 0.10
        if premium_hits >= 4:
            out['insurance'] += 0.12
        if trading_hits >= 4:
            out['broker_dealer'] += 0.10
        if inventory_hits >= 2:
            out['industrial'] += 0.08

        return out

    def _material_value(self, value) -> bool:
        if not isinstance(value, (int, float)):
            return False
        return abs(float(value)) > 1e-9

    def _sum_matching(self, latest: Dict[str, float], patterns: List[str]) -> float:
        total = 0.0
        for k, v in latest.items():
            lk = str(k).lower()
            if any(p in lk for p in patterns) and isinstance(v, (int, float)):
                total += abs(float(v))
        return total

    def _has_matching(self, concepts_l: List[str], patterns: List[str]) -> bool:
        return any(any(p in c for p in patterns) for c in concepts_l)

    def _detect_indicators(self, latest: Dict[str, float], concepts_l: List[str]) -> Dict:
        assets_val = self._sum_matching(latest, ['assets'])
        revenue_val = self._sum_matching(latest, ['revenue', 'revenues', 'sales'])
        deposits_val = self._sum_matching(latest, ['deposit'])
        nii_val = self._sum_matching(latest, ['netinterestincome', 'interestincomeexpensenet'])
        loans_val = self._sum_matching(latest, ['loan'])
        reg_cap_val = self._sum_matching(latest, ['tier1', 'cet1', 'capitalratio'])

        premiums_val = self._sum_matching(latest, ['premium'])
        policy_liab_val = self._sum_matching(latest, ['policyholder', 'futurepolicybenefit'])
        lae_val = self._sum_matching(latest, ['lossadjustment', 'claims'])
        reins_val = self._sum_matching(latest, ['reinsurance'])
        underwriting_val = self._sum_matching(latest, ['underwriting'])

        trading_val = self._sum_matching(latest, ['tradingrevenue', 'principaltransactions'])
        ib_val = self._sum_matching(latest, ['investmentbanking', 'advisory', 'brokerage'])
        aum_val = self._sum_matching(latest, ['aum', 'assetmanagement'])

        return {
            'assets': {'present': assets_val > 0 or self._has_matching(concepts_l, ['assets']), 'value': assets_val},
            'revenue': {'present': revenue_val > 0 or self._has_matching(concepts_l, ['revenue', 'revenues', 'sales']), 'value': revenue_val},
            'deposits': {'present': deposits_val > 0 or self._has_matching(concepts_l, ['deposit']), 'value': deposits_val},
            'nii': {'present': nii_val > 0 or self._has_matching(concepts_l, ['netinterestincome', 'interestincomeexpensenet']), 'value': nii_val},
            'loans': {'present': loans_val > 0 or self._has_matching(concepts_l, ['loan']), 'value': loans_val},
            'regulatory_capital': {'present': reg_cap_val > 0 or self._has_matching(concepts_l, ['tier1', 'cet1', 'capitalratio']), 'value': reg_cap_val},
            'premiums': {'present': premiums_val > 0 or self._has_matching(concepts_l, ['premium']), 'value': premiums_val},
            'policyholder_liabilities': {'present': policy_liab_val > 0 or self._has_matching(concepts_l, ['policyholder', 'futurepolicybenefit']), 'value': policy_liab_val},
            'lae': {'present': lae_val > 0 or self._has_matching(concepts_l, ['lossadjustment', 'claims']), 'value': lae_val},
            'reinsurance': {'present': reins_val > 0 or self._has_matching(concepts_l, ['reinsurance']), 'value': reins_val},
            'underwriting_income': {'present': underwriting_val > 0 or self._has_matching(concepts_l, ['underwriting']), 'value': underwriting_val},
            'trading_revenue': {'present': trading_val > 0 or self._has_matching(concepts_l, ['tradingrevenue', 'principaltransactions']), 'value': trading_val},
            'investment_banking_fees': {'present': ib_val > 0 or self._has_matching(concepts_l, ['investmentbanking', 'advisory', 'brokerage']), 'value': ib_val},
            'aum': {'present': aum_val > 0 or self._has_matching(concepts_l, ['aum', 'assetmanagement']), 'value': aum_val},
        }

    def _hard_bank_gate(self, indicators: Dict, materiality: Dict) -> Dict:
        cond1 = indicators['deposits']['present'] and bool((materiality.get('deposits') or {}).get('is_material'))
        cond2 = indicators['nii']['present'] and bool((materiality.get('nii') or {}).get('is_material'))
        cond3 = indicators['loans']['present'] or indicators['regulatory_capital']['present']
        passed = bool(cond1 and cond2 and cond3)
        return {
            'passed': passed,
            'conditions': {
                'deposits_evidence': cond1,
                'nii_evidence': cond2,
                'structure_evidence': cond3,
            }
        }

    def _insurance_override(self, indicators: Dict, materiality: Dict) -> Dict:
        keys = ['premiums', 'policyholder_liabilities', 'lae', 'reinsurance', 'underwriting_income']
        material_floor = float(materiality.get('_threshold', 0.0))

        material_hits = 0
        insurance_total = 0.0
        for k in keys:
            v = float(indicators.get(k, {}).get('value') or 0.0)
            insurance_total += abs(v)
            if bool((materiality.get(k) or {}).get('is_material')):
                material_hits += 1

        triggered = material_hits >= 2
        return {
            'triggered': triggered,
            'hits': material_hits,
            'required_hits': 2,
            'material_floor': material_floor,
            'insurance_total': insurance_total,
        }

    def _broker_rule(self, indicators: Dict) -> Dict:
        no_deposits = not indicators['deposits']['present']
        strong = sum(
            1 for k in ['trading_revenue', 'investment_banking_fees', 'aum']
            if indicators.get(k, {}).get('present')
        ) >= 2
        return {'triggered': bool(no_deposits and strong), 'no_deposits': no_deposits, 'strong_market_activity': strong}

    def _materiality_assessment(self, indicators: Dict) -> Dict:
        assets = float((indicators.get('assets') or {}).get('value') or 0.0)
        revenue = float((indicators.get('revenue') or {}).get('value') or 0.0)
        scale_base = max(abs(assets), abs(revenue), 1.0)
        threshold = max(self.ABS_MIN_USD, self.REL_MIN * scale_base)
        out = {'_scale_base': scale_base, '_threshold': threshold, 'ABS_MIN_USD': self.ABS_MIN_USD, 'REL_MIN': self.REL_MIN}
        for k, meta in indicators.items():
            v = float((meta or {}).get('value') or 0.0)
            out[k] = {'value': v, 'is_material': abs(v) >= threshold}
        return out

    def _bank_gate_strength(self, indicators: Dict) -> Dict:
        assets = float(indicators.get('assets', {}).get('value') or 0.0)
        deposits = float(indicators.get('deposits', {}).get('value') or 0.0)
        loans = float(indicators.get('loans', {}).get('value') or 0.0)
        nii = float(indicators.get('nii', {}).get('value') or 0.0)

        base = max(abs(assets), 1.0)
        dep_ratio = deposits / base
        loan_ratio = loans / base
        nii_ratio = nii / base

        dep_min = 0.15
        loan_min = 0.05
        nii_min = 0.001
        material = bool(dep_ratio >= dep_min and loan_ratio >= loan_min and nii_ratio >= nii_min)
        return {
            'material_core_bank_footprint': material,
            'ratios': {
                'deposits_to_assets': dep_ratio,
                'loans_to_assets': loan_ratio,
                'nii_to_assets': nii_ratio,
            },
            'thresholds': {
                'deposits_to_assets_min': dep_min,
                'loans_to_assets_min': loan_min,
                'nii_to_assets_min': nii_min,
            },
        }
