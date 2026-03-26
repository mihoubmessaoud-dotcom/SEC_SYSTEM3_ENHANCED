from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List


class MappingConfidenceEngine:
    """
    Deterministic mapping-confidence and drift scorer.
    """

    REQUIRED_MIN_CONFIDENCE = 80

    def compute(
        self,
        years: List[int],
        mapping_decisions: Iterable,
        node_inputs_by_year: Dict[int, Dict[str, List[Dict]]],
    ) -> Dict:
        by_node = defaultdict(list)
        for d in mapping_decisions:
            by_node[d.ontology_node_id].append(d)

        confidence_by_year: Dict[int, Dict[str, float]] = {}
        traces_by_year: Dict[int, Dict[str, Dict]] = {}

        for y in years:
            confidence_by_year[y] = {}
            traces_by_year[y] = {}
            node_inputs = node_inputs_by_year.get(y, {})
            for node_id, decisions in by_node.items():
                base = self._decision_score(decisions)
                has_inputs = bool(node_inputs.get(node_id))
                unit_ok = all(bool((i or {}).get('unit')) for i in node_inputs.get(node_id, []))
                currency_ok = all(bool((i or {}).get('currency')) for i in node_inputs.get(node_id, []))
                statement_role_bonus = 3 if self._node_role_consistent(node_id, decisions) else 0
                context_bonus = 4 if has_inputs else -10
                quality_adj = 3 if unit_ok and currency_ok else -8
                score = max(0.0, min(100.0, base + statement_role_bonus + context_bonus + quality_adj))
                confidence_by_year[y][node_id] = score
                traces_by_year[y][node_id] = {
                    'base_score': base,
                    'has_inputs': has_inputs,
                    'unit_ok': unit_ok,
                    'currency_ok': currency_ok,
                    'statement_role_bonus': statement_role_bonus,
                    'context_bonus': context_bonus,
                    'quality_adjustment': quality_adj,
                    'final_score': score,
                    'required_min': self.REQUIRED_MIN_CONFIDENCE,
                }

        drift_flags = self._detect_drift(years, node_inputs_by_year)
        return {
            'confidence_by_year': confidence_by_year,
            'traces_by_year': traces_by_year,
            'drift_flags': drift_flags,
            'required_min_confidence': self.REQUIRED_MIN_CONFIDENCE,
        }

    def _decision_score(self, decisions: List) -> float:
        if not decisions:
            return 0.0
        best_source = max((str(getattr(d, 'source', '') or '') for d in decisions), default='')
        if best_source == 'exact':
            return 95.0
        if best_source == 'learned':
            return 88.0
        if best_source == 'keyword':
            return 76.0
        return 65.0

    def _node_role_consistent(self, node_id: str, decisions: List) -> bool:
        if not decisions:
            return False
        prefixes = {'IS': ['income', 'revenue', 'expense', 'profit'], 'BS': ['asset', 'liab', 'equity'], 'CF': ['cash', 'flow']}
        node_prefix = node_id.split('.', 1)[0]
        hints = prefixes.get(node_prefix, [])
        if not hints:
            return True
        concept_join = ' '.join(str(getattr(d, 'raw_concept', '') or '').lower() for d in decisions)
        return any(h in concept_join for h in hints)

    def _detect_drift(self, years: List[int], node_inputs_by_year: Dict[int, Dict[str, List[Dict]]]) -> Dict[int, List[Dict]]:
        flags: Dict[int, List[Dict]] = {}
        for i in range(1, len(years)):
            y0, y1 = years[i - 1], years[i]
            prev_nodes = node_inputs_by_year.get(y0, {})
            curr_nodes = node_inputs_by_year.get(y1, {})
            year_flags = []
            all_nodes = set(prev_nodes.keys()) | set(curr_nodes.keys())
            for n in sorted(all_nodes):
                prev_tags = {str((x or {}).get('source_tag') or '') for x in prev_nodes.get(n, [])}
                curr_tags = {str((x or {}).get('source_tag') or '') for x in curr_nodes.get(n, [])}
                prev_units = {str((x or {}).get('unit') or '') for x in prev_nodes.get(n, [])}
                curr_units = {str((x or {}).get('unit') or '') for x in curr_nodes.get(n, [])}
                if prev_tags and curr_tags and prev_tags != curr_tags:
                    year_flags.append({'node_id': n, 'flag': 'tag_changed', 'from': sorted(prev_tags), 'to': sorted(curr_tags)})
                if prev_units and curr_units and prev_units != curr_units:
                    year_flags.append({'node_id': n, 'flag': 'unit_changed', 'from': sorted(prev_units), 'to': sorted(curr_units)})
            flags[y1] = year_flags
        return flags
