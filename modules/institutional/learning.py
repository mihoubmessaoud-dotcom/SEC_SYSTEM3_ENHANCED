from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


class ExtensionLearningLayer:
    """
    Detect unknown extension tags, cluster by semantic similarity,
    and store mapping decisions for future reuse.
    """

    def __init__(self, learning_file: str = 'institutional_extension_learning.json') -> None:
        self.learning_path = Path(learning_file)
        self.knowledge = self._load()

    def process_unknown_tags(self, unknown_tags: Iterable[str], ontology_nodes: Iterable) -> Dict:
        unknown_tags = list(dict.fromkeys(unknown_tags))
        suggestions = []

        for tag in unknown_tags:
            best_node, score = self._best_match(tag, ontology_nodes)
            suggestions.append({
                'tag': tag,
                'suggested_node_id': best_node,
                'similarity': score,
                'auto_map_candidate': score >= 0.65,
            })

        clusters = self._cluster_unknowns(unknown_tags)
        return {'unknown_tags': unknown_tags, 'suggestions': suggestions, 'clusters': clusters}

    def store_mapping_decisions(self, decisions: Iterable[Dict]) -> None:
        for d in decisions:
            tag = str(d.get('tag', '')).lower()
            node = d.get('mapped_node_id')
            if tag and node:
                self.knowledge[tag] = node
        self._save()

    def get_learned_mapping(self) -> Dict[str, str]:
        return dict(self.knowledge)

    def _best_match(self, tag: str, ontology_nodes: Iterable) -> Tuple[str, float]:
        t = set(self._tokens(tag))
        best = ('', 0.0)
        for n in ontology_nodes:
            n_tokens = set(self._tokens(n.label + ' ' + ' '.join(n.xbrl_concepts)))
            if not n_tokens:
                continue
            sim = len(t & n_tokens) / len(t | n_tokens)
            if sim > best[1]:
                best = (n.id, sim)
        return best

    def _cluster_unknowns(self, tags: List[str]) -> List[Dict]:
        groups: Dict[str, List[str]] = {}
        for t in tags:
            tok = self._tokens(t)
            key = tok[0] if tok else 'misc'
            groups.setdefault(key, []).append(t)
        return [{'cluster_key': k, 'tags': v} for k, v in sorted(groups.items())]

    def _tokens(self, text: str) -> List[str]:
        out, cur = [], ''
        for ch in str(text).lower():
            if ch.isalnum():
                cur += ch
            else:
                if cur:
                    out.append(cur)
                    cur = ''
        if cur:
            out.append(cur)
        return out

    def _load(self) -> Dict[str, str]:
        if self.learning_path.exists():
            try:
                return json.loads(self.learning_path.read_text(encoding='utf-8'))
            except Exception:
                return {}
        return {}

    def _save(self) -> None:
        self.learning_path.write_text(json.dumps(self.knowledge, indent=2, ensure_ascii=False), encoding='utf-8')
