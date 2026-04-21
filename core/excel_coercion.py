from __future__ import annotations

import math
import re
from typing import Any, Iterable, Optional


_NA = {"", "nan", "none", "null", "n/a", "--", "—"}


def coerce_excel_number(value: Any) -> Optional[float]:
    """
    Convert a UI-formatted numeric string into a real number for Excel.
    Handles:
    - thousand separators: "1,630,338,779"
    - dot decimals: "26.0378378378"
    - comma decimals: "26,03"
    - parentheses negatives: "(123.4)"
    - estimated prefix: "˜123.4"
    Returns None if not numeric-like.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            fv = float(value)
            if math.isfinite(fv):
                return fv
            return None
        except Exception:
            return None
    s = str(value).strip()
    if not s:
        return None
    if s.startswith("˜"):
        s = s[1:].strip()
    s_low = s.lower()
    if s_low in _NA:
        return None
    # Strip percent sign if present (we avoid coercing ratios in UI export, but be robust).
    if s.endswith("%"):
        s = s[:-1].strip()
        # keep as numeric percentage (e.g., "4.0" -> 4.0); caller decides scaling.
    # Parentheses negatives
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1].strip()
    # Remove spaces
    s = re.sub(r"\s+", "", s)
    # Arabic decimal separator (U+066B) or comma-like punctuation sometimes appears.
    s = s.replace("٫", ".").replace("٬", ",")
    # Fast path: plain float
    try:
        fv = float(s)
        if math.isfinite(fv):
            return fv
    except Exception:
        pass
    # If both comma and dot exist: assume comma is thousands, dot is decimal.
    if "," in s and "." in s:
        try:
            fv = float(s.replace(",", ""))
            if math.isfinite(fv):
                return fv
        except Exception:
            return None
    # Only commas: decide thousands vs decimal comma.
    if "," in s and "." not in s:
        parts = s.split(",")
        # Many commas => thousands separators
        if len(parts) >= 3 and all(p.isdigit() for p in parts):
            try:
                return float("".join(parts))
            except Exception:
                return None
        if len(parts) == 2 and parts[0].lstrip("-").isdigit() and parts[1].isdigit():
            # If right side is 3 digits, it's probably thousands separator.
            if len(parts[1]) == 3:
                try:
                    return float(parts[0] + parts[1])
                except Exception:
                    return None
            # Otherwise treat as decimal comma.
            try:
                return float(parts[0] + "." + parts[1])
            except Exception:
                return None
    # Only dots but might include thousands using spaces already removed; try again:
    try:
        fv = float(s)
        if math.isfinite(fv):
            return fv
    except Exception:
        return None


def coerce_df_year_columns(df, year_cols: Iterable[str]):
    """
    In-place-ish: returns a copy with specified columns coerced to numeric where possible.
    Non-numeric cells stay as original.
    """
    if df is None or getattr(df, "empty", False):
        return df
    out = df.copy()
    for c in year_cols:
        if c not in out.columns:
            continue

        def _coerce_cell(v):
            fv = coerce_excel_number(v)
            return fv if fv is not None else v

        out[c] = out[c].map(_coerce_cell)
    return out

