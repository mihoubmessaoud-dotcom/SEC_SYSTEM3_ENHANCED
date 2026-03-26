from __future__ import annotations

import math
from typing import Iterable, Tuple, Optional


def _normalize_label(label: str) -> str:
    if not label:
        return ""
    s = str(label).lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _char_ngrams(text: str, n: int = 3) -> Iterable[str]:
    if not text:
        return []
    if len(text) <= n:
        return [text]
    return [text[i:i + n] for i in range(len(text) - n + 1)]


def _hash_vec(text: str, dims: int = 128) -> list:
    vec = [0.0] * dims
    for gram in _char_ngrams(text, 3):
        h = 0
        for ch in gram:
            h = (h * 31 + ord(ch)) & 0xFFFFFFFF
        idx = h % dims
        vec[idx] += 1.0
    return vec


def _cosine(a: list, b: list) -> float:
    dot = 0.0
    na = 0.0
    nb = 0.0
    for av, bv in zip(a, b):
        dot += av * bv
        na += av * av
        nb += bv * bv
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / math.sqrt(na * nb)


def semantic_best_match(
    raw_label: str,
    candidate_labels: Iterable[str],
    min_score: float = 0.90,
) -> Tuple[Optional[str], float]:
    """
    Lightweight embedding-style match using character n-grams.
    Returns (best_label, score) if above min_score.
    """
    norm = _normalize_label(raw_label)
    if not norm:
        return None, 0.0
    v_raw = _hash_vec(norm)
    best = None
    best_score = 0.0
    for cand in candidate_labels or []:
        c_norm = _normalize_label(cand)
        if not c_norm:
            continue
        score = _cosine(v_raw, _hash_vec(c_norm))
        if score > best_score:
            best_score = score
            best = cand
    if best_score >= min_score:
        return best, best_score
    return None, best_score


def semantic_select_raw_tag(
    raw_keys: Iterable[str],
    known_tags: Iterable[str],
    min_score: float = 0.92,
) -> Tuple[Optional[str], Optional[str], float]:
    """
    Finds the raw tag that is most similar to any known tag.
    Returns (raw_tag, matched_known_tag, score).
    """
    best_raw = None
    best_known = None
    best_score = 0.0
    for raw in raw_keys or []:
        known, score = semantic_best_match(raw, known_tags, min_score=min_score)
        if known and score > best_score:
            best_score = score
            best_raw = raw
            best_known = known
    if best_raw and best_known:
        return best_raw, best_known, best_score
    return None, None, best_score
