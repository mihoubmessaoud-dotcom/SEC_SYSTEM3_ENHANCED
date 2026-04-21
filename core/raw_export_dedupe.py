from __future__ import annotations

from typing import List, Optional, Tuple

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

