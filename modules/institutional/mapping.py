from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


@dataclass
class MappingDecision:
    raw_concept: str
    ontology_node_id: str
    confidence: float
    source: str


class OntologyMapper:
    """
    Maps raw XBRL concepts to ontology nodes.
    Supports extension learning suggestions.
    """

    def __init__(self) -> None:
        self.learned_map: Dict[str, str] = {}

    def map_concepts(
        self,
        concepts: Iterable[str],
        ontology_nodes: Iterable,
    ) -> Tuple[List[MappingDecision], List[str]]:
        node_by_concept = {}
        for node in ontology_nodes:
            for c in node.xbrl_concepts:
                node_by_concept[c.lower()] = node.id

        decisions: List[MappingDecision] = []
        unknown: List[str] = []

        for concept in concepts:
            low = concept.lower()
            if low in node_by_concept:
                decisions.append(MappingDecision(concept, node_by_concept[low], 0.98, 'exact'))
                continue

            if low in self.learned_map:
                decisions.append(MappingDecision(concept, self.learned_map[low], 0.90, 'learned'))
                continue

            best_node, score = self._fuzzy_pick(low, ontology_nodes)
            if best_node and score >= 0.50:
                decisions.append(MappingDecision(concept, best_node, score, 'keyword'))
            else:
                unknown.append(concept)

        return decisions, unknown

    def _fuzzy_pick(self, concept_low: str, ontology_nodes: Iterable) -> Tuple[str, float]:
        c_tokens = set(self._split_tokens(concept_low))
        best = ('', 0.0)
        for node in ontology_nodes:
            n_tokens = set(self._split_tokens(node.label.lower()))
            if not n_tokens:
                continue
            overlap = len(c_tokens & n_tokens) / len(n_tokens)
            if overlap > best[1]:
                best = (node.id, overlap)
        return best

    def _split_tokens(self, text: str) -> List[str]:
        out = []
        cur = ''
        for ch in text:
            if ch.isalnum():
                cur += ch
            else:
                if cur:
                    out.append(cur)
                    cur = ''
        if cur:
            out.append(cur)
        return out

    def apply_learning_decisions(self, decisions: Iterable[MappingDecision]) -> None:
        for d in decisions:
            if d.confidence >= 0.85:
                self.learned_map[d.raw_concept.lower()] = d.ontology_node_id
