from __future__ import annotations

from typing import List, Optional, Tuple

import re
import unicodedata

import pandas as pd


def _norm_cell(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def _row_signature(row: pd.Series, year_cols: List[str]) -> Tuple:
    return tuple(_norm_cell(row.get(c)) for c in year_cols)


def dedupe_labeled_timeseries_df(
    df: pd.DataFrame,
    *,
    label_col: str,
    year_cols: List[str],
    unit_col: Optional[str] = None,
    concept_col: str = "__concept__",
) -> pd.DataFrame:
    """
    Dedupe/resolve collisions in exported Raw_by_Year when multiple concepts map to the same display label.

    Rules:
    - Group by (label, unit). If all rows have identical values across years -> keep 1 row.
    - If values conflict -> keep all rows but disambiguate the label by appending the concept key.
    - Never changes sheet structure: returns same columns (label/year/unit) and drops concept_col.
    """
    if df is None or df.empty:
        return df
    if label_col not in df.columns:
        return df
    safe_year_cols = [c for c in year_cols if c in df.columns]
    if not safe_year_cols:
        return df

    cols = list(df.columns)
    if concept_col not in cols:
        # We can still drop exact duplicates by label+unit+year vector, but cannot disambiguate conflicts.
        key_cols = [label_col] + ([(unit_col)] if unit_col and unit_col in cols else []) + safe_year_cols
        return df.drop_duplicates(subset=key_cols, keep="first")

    unit_key = unit_col if (unit_col and unit_col in cols) else None
    group_cols = [label_col] + ([unit_key] if unit_key else [])

    keep_rows = []
    for _, g in df.groupby(group_cols, dropna=False, sort=False):
        if len(g) <= 1:
            keep_rows.append(g)
            continue
        sigs = [_row_signature(r, safe_year_cols) for _, r in g.iterrows()]
        unique_sigs = list(dict.fromkeys(sigs))
        if len(unique_sigs) == 1:
            # fully identical across years -> keep first
            keep_rows.append(g.iloc[[0]])
            continue
        # conflicting values -> keep all with disambiguated labels
        out_g = g.copy()
        # avoid adding duplicate suffix when concept already present
        for idx, row in out_g.iterrows():
            base = str(row.get(label_col) or "").strip()
            concept = str(row.get(concept_col) or "").strip()
            if concept and concept not in base:
                out_g.at[idx, label_col] = f"{base} ({concept})"
        keep_rows.append(out_g)

    out = pd.concat(keep_rows, ignore_index=True) if keep_rows else df.copy()
    # Drop internal concept col from final export
    if concept_col in out.columns:
        out = out.drop(columns=[concept_col])
    # Preserve original column ordering when possible
    preferred = [c for c in cols if c != concept_col and c in out.columns]
    out = out[preferred + [c for c in out.columns if c not in preferred]]
    return out


def dedupe_timeseries_by_value_vector(
    df: pd.DataFrame,
    *,
    label_col: str,
    year_cols: List[str],
    unit_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Collapse duplicate rows that have identical values across all year columns (and unit),
    even when the display labels differ (common with bilingual/alias labels like:
    "Total Assets", "TotalAssets", "Assets").

    Safety:
    - Only collapses groups where at least one year value is present (non-null).
    - Keeps a single "best" label per identical value-vector group.
    - Preserves column structure and original ordering as much as possible.
    """
    if df is None or df.empty:
        return df
    if label_col not in df.columns:
        return df
    safe_year_cols = [c for c in year_cols if c in df.columns]
    if not safe_year_cols:
        return df

    ucol = unit_col if (unit_col and unit_col in df.columns) else None

    def _has_any_value(row: pd.Series) -> bool:
        for c in safe_year_cols:
            if _norm_cell(row.get(c)) is not None:
                return True
        return False

    def _score_label(lbl: str) -> float:
        s = str(lbl or "")
        score = 0.0
        # Prefer Arabic labels (they are typically the user-facing canonical form in the UI).
        if any("\u0600" <= ch <= "\u06ff" for ch in s):
            score += 5.0
        # Prefer labels that include explicit concept disambiguation in parentheses.
        if "(" in s and ")" in s:
            score += 2.0
        if "إجمالي" in s or "Total" in s:
            score += 0.5
        # Small preference for shorter (less noisy) labels.
        score -= (len(s) / 1000.0)
        return score

    # Build signatures and keep best row per signature.
    signatures: dict[tuple, dict] = {}
    order: list[int] = []
    for idx, row in df.iterrows():
        if not _has_any_value(row):
            # Keep header/section rows as-is.
            signatures[("HEADER", idx)] = {"keep_idx": idx}
            order.append(idx)
            continue
        sig = tuple(_norm_cell(row.get(c)) for c in ([ucol] if ucol else []) + safe_year_cols)
        if sig not in signatures:
            signatures[sig] = {"keep_idx": idx, "score": _score_label(row.get(label_col))}
            order.append(idx)
            continue
        # Compete for "best" label within identical vector.
        cur = signatures[sig]
        sc = _score_label(row.get(label_col))
        if sc > float(cur.get("score") or -1e9):
            cur["keep_idx"] = idx
            cur["score"] = sc

    keep = set()
    for sig, info in signatures.items():
        keep.add(int(info.get("keep_idx")))

    out = df.loc[sorted(list(keep))].copy()
    # Preserve original order by stable sorting on original index positions.
    out["_tmp_order"] = out.index.map(lambda i: order.index(i) if i in order else 1_000_000)
    out = out.sort_values("_tmp_order").drop(columns=["_tmp_order"], errors="ignore")
    out = out.reset_index(drop=True)
    return out


_BIDI_AND_ZW = re.compile(r"[\u200e\u200f\u202a-\u202e\u2066-\u2069\u200b\u200c\u200d\uFEFF]")


def _clean_label_for_tag_extract(text: str) -> str:
    s = str(text or "")
    s = unicodedata.normalize("NFKC", s)
    s = _BIDI_AND_ZW.sub("", s)
    return s.strip()


def _extract_canonical_tag(text: str) -> Optional[str]:
    """
    Extract a canonical SEC/XBRL "tag-like" identifier from a label such as:
      - "إجمالي الأصول (Assets)"
      - "Total Assets (TotalAssets)"
      - "Assets"
      - "إجمالي الأصول (Total Assets)"  -> Assets

    Returns None when no safe canonical tag can be inferred.
    """
    s = _clean_label_for_tag_extract(text)
    if not s:
        return None

    # Direct GAAP-like tag.
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", s):
        direct = s
        direct_norm = {
            "TotalAssets": "Assets",
            "CurrentAssets": "AssetsCurrent",
            "TotalLiabilities": "Liabilities",
            "CurrentLiabilities": "LiabilitiesCurrent",
            "TotalStockholdersEquity": "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "StockholdersEquity",
        }.get(direct, direct)
        return direct_norm

    # Prefer final parentheses token when present.
    m = re.search(r"\(([^()]+)\)\s*$", s)
    token = (m.group(1).strip() if m else "")
    if token:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", token):
            return {
                "TotalAssets": "Assets",
                "CurrentAssets": "AssetsCurrent",
                "TotalLiabilities": "Liabilities",
                "CurrentLiabilities": "LiabilitiesCurrent",
                "TotalStockholdersEquity": "StockholdersEquity",
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "StockholdersEquity",
            }.get(token, token)
        token_compact = re.sub(r"[^A-Za-z0-9]", "", token)
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", token_compact):
            return {
                "TotalAssets": "Assets",
                "CurrentAssets": "AssetsCurrent",
                "TotalLiabilities": "Liabilities",
                "CurrentLiabilities": "LiabilitiesCurrent",
                "TotalStockholdersEquity": "StockholdersEquity",
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "StockholdersEquity",
            }.get(token_compact, token_compact)

    # Known English alias → canonical tag (very conservative list).
    alias = (token or s).strip().lower()
    alias = re.sub(r"\s+", " ", alias)
    alias_map = {
        "total assets": "Assets",
        "totalassets": "Assets",
        "assets": "Assets",
        "current assets": "AssetsCurrent",
        "currentassets": "AssetsCurrent",
        "assets current": "AssetsCurrent",
        "total liabilities": "Liabilities",
        "totalliabilities": "Liabilities",
        "liabilities": "Liabilities",
        "current liabilities": "LiabilitiesCurrent",
        "currentliabilities": "LiabilitiesCurrent",
        "liabilities current": "LiabilitiesCurrent",
        "stockholders equity": "StockholdersEquity",
        "stockholdersequity": "StockholdersEquity",
        "total stockholders equity": "StockholdersEquity",
        "shareholders equity": "StockholdersEquity",
        "shareholdersequity": "StockholdersEquity",
        "equity": "StockholdersEquity",
        "operating income": "OperatingIncomeLoss",
        "income from operations": "OperatingIncomeLoss",
        "net income": "NetIncomeLoss",
        "net income loss": "NetIncomeLoss",
        "revenue": "Revenues",
        "revenues": "Revenues",
        "cost of revenue": "CostOfRevenue",
        "costofrevenue": "CostOfRevenue",
    }
    return alias_map.get(alias)


def dedupe_timeseries_by_canonical_tag(
    df: pd.DataFrame,
    *,
    label_col: str,
    year_cols: List[str],
    unit_col: Optional[str] = None,
    concept_col: str = "__concept__",
) -> pd.DataFrame:
    """
    Drop duplicated rows that represent the same underlying SEC/XBRL concept,
    even when the *display* label differs (common with alias labels).

    This is stricter than `dedupe_timeseries_by_value_vector`:
    - Groups by inferred canonical tag from `concept_col` (fallback to `label_col`)
    - Keeps exactly one row per canonical tag group
    - Chooses the row with best year coverage, then largest magnitude (to avoid
      keeping subtotals when totals are present under the same alias token)

    It never changes the public sheet structure; it preserves all columns.
    """
    if df is None or df.empty:
        return df
    safe_year_cols = [c for c in year_cols if c in df.columns]
    if not safe_year_cols:
        return df

    ucol = unit_col if (unit_col and unit_col in df.columns) else None
    src_col = concept_col if (concept_col and concept_col in df.columns) else label_col

    tags = []
    for _, row in df.iterrows():
        tag = _extract_canonical_tag(row.get(src_col))
        if tag is None:
            tag = _extract_canonical_tag(row.get(label_col))
        tags.append(tag)

    work = df.copy()
    work["__canon_tag__"] = tags

    def _row_score(row: pd.Series) -> Tuple[int, float]:
        coverage = 0
        magnitude = 0.0
        for c in safe_year_cols:
            v = _norm_cell(row.get(c))
            if v is None:
                continue
            coverage += 1
            try:
                magnitude += abs(float(v))
            except Exception:
                pass
        return coverage, magnitude

    keep_idx: list[int] = []
    for tag_val, g in work.groupby("__canon_tag__", dropna=False, sort=False):
        if tag_val is None or (isinstance(tag_val, float) and pd.isna(tag_val)):
            keep_idx.extend(list(g.index))
            continue
        if len(g) == 1:
            keep_idx.append(int(g.index[0]))
            continue
        # Prefer row with most filled years; if tie, prefer larger magnitude.
        best_i = None
        best_score = (-1, -1.0)
        for idx, row in g.iterrows():
            sc = _row_score(row)
            if sc > best_score:
                best_score = sc
                best_i = int(idx)
        if best_i is not None:
            keep_idx.append(best_i)

    out = work.loc[sorted(set(keep_idx))].copy()
    out = out.drop(columns=["__canon_tag__"], errors="ignore")
    # Preserve original ordering (stable).
    out["_tmp_order"] = out.index
    out = out.sort_values("_tmp_order").drop(columns=["_tmp_order"], errors="ignore")
    out = out.reset_index(drop=True)
    return out
