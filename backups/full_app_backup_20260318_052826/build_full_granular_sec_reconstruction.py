#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


INPUT_FILE = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\Nouveau Feuille de calcul Microsoft Excel.xlsx")
INPUT_SHEET = "Sheet1"

OUTPUT_MD = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\Intel_Full_Granular_SEC_Table.md")
OUTPUT_XLSX = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\Intel_Full_Granular_SEC_Statements.xlsx")

DATES = ["Dec. 27, 2025", "Dec. 28, 2024", "Dec. 30, 2023"]


def clean_and_scale(v) -> Optional[int]:
    """Return normalized value in Millions.
    Rule:
    - strip $, commas, spaces
    - (xxx) or -xxx -> negative
    - decimal values -> multiply by 1000
    """
    if pd.isna(v):
        return None

    neg = False
    if isinstance(v, (int, float)):
        n = float(v)
        if n < 0:
            neg = True
            n = abs(n)
        if abs(n - int(n)) > 1e-12:
            n = n * 1000.0
        out = int(round(n))
        return -out if neg else out

    s = str(v).strip()
    if not s:
        return None
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    if s.startswith("-"):
        neg = True
        s = s[1:].strip()

    s = s.replace("$", "").replace(",", "").replace(" ", "").replace("\u00a0", "")
    if not s:
        return None
    if not re.fullmatch(r"\d+(\.\d+)?", s):
        return None

    n = float(s)
    if "." in s:
        n *= 1000.0
    out = int(round(n))
    return -out if neg else out


def detect_statement(label: str, current: str) -> str:
    l = label.lower()
    if "consolidated statements of operations" in l or "consolidated statements of income" in l:
        return "Income Statement"
    if "consolidated balance sheets" in l:
        return "Balance Sheet"
    if "consolidated statements of cash flows" in l:
        return "Cash Flow"
    return current


def is_header(label: str) -> bool:
    l = label.strip().lower()
    if not l:
        return False
    return l.endswith(":") or "[abstract]" in l or "12 months ended" in l or "$ in millions" in l or "shares in millions" in l


def find_year_col_map(df: pd.DataFrame) -> Dict[int, int]:
    # The source layout keeps year values in cols 1..3 when available.
    # Map by date text appearances; first hit wins.
    out: Dict[int, int] = {}
    for _, row in df.iterrows():
        for c in [1, 2, 3]:
            if c >= len(df.columns):
                continue
            txt = str(row.get(c, "") or "")
            if "Dec. 27, 2025" in txt:
                out[2025] = c
            elif "Dec. 28, 2024" in txt:
                out[2024] = c
            elif "Dec. 30, 2023" in txt:
                out[2023] = c
        if len(out) == 3:
            return out
    # fallback fixed positions
    return {2025: 1, 2024: 2, 2023: 3}


def reconstruct(df: pd.DataFrame) -> pd.DataFrame:
    year_cols = find_year_col_map(df)
    rows: List[Dict] = []

    current_statement = "Unclassified"
    current_parent = ""

    for i in range(len(df)):
        label_raw = df.iat[i, 0] if 0 < len(df.columns) else None
        if pd.isna(label_raw):
            continue
        label = str(label_raw).strip()
        if not label:
            continue

        current_statement = detect_statement(label, current_statement)
        hdr = is_header(label)
        if hdr:
            current_parent = label

        v2025 = clean_and_scale(df.iat[i, year_cols.get(2025, 1)]) if year_cols.get(2025, 1) < len(df.columns) else None
        v2024 = clean_and_scale(df.iat[i, year_cols.get(2024, 2)]) if year_cols.get(2024, 2) < len(df.columns) else None
        v2023 = clean_and_scale(df.iat[i, year_cols.get(2023, 3)]) if year_cols.get(2023, 3) < len(df.columns) else None

        rows.append(
            {
                "Row": i,
                "Statement": current_statement,
                "Parent": current_parent if not hdr else "",
                "Item": label,
                "Is_Header": "Yes" if hdr else "No",
                "2025": v2025,
                "2024": v2024,
                "2023": v2023,
            }
        )

    out = pd.DataFrame(rows)
    for c in ["2025", "2024", "2023"]:
        out[c] = out[c].astype("Int64")
    return out


def apply_parent_child_checks(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Reconciliation"] = ""

    # Check totals inside each statement using contiguous siblings until next header.
    for stmt in ["Income Statement", "Balance Sheet", "Cash Flow"]:
        sub = out[out["Statement"] == stmt]
        idxs = list(sub.index)
        for pos, idx in enumerate(idxs):
            item = str(out.at[idx, "Item"]).lower()
            if not item.startswith("total "):
                continue

            # collect children above this total until previous header
            child_idxs: List[int] = []
            p = pos - 1
            while p >= 0:
                j = idxs[p]
                if out.at[j, "Is_Header"] == "Yes":
                    break
                if str(out.at[j, "Item"]).lower().startswith("total "):
                    break
                child_idxs.append(j)
                p -= 1
            child_idxs.reverse()

            if not child_idxs:
                continue

            notes = []
            for y in ["2025", "2024", "2023"]:
                parent_val = out.at[idx, y]
                if pd.isna(parent_val):
                    continue
                vals = [out.at[j, y] for j in child_idxs if not pd.isna(out.at[j, y])]
                if not vals:
                    continue
                s = int(sum(int(v) for v in vals))
                if int(parent_val) == s:
                    notes.append(f"{y}:OK")
                else:
                    notes.append(f"{y}:Mismatch({int(parent_val)}!={s})")
            if notes:
                out.at[idx, "Reconciliation"] = " | ".join(notes)

    return out


def to_markdown(df: pd.DataFrame) -> str:
    m = df.copy().astype(object).where(df.notna(), "")
    return m.to_markdown(index=False)


def export_excel(df: pd.DataFrame) -> None:
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        for sheet in ["Income Statement", "Balance Sheet", "Cash Flow"]:
            s = df[df["Statement"] == sheet].copy()
            s = s[["Row", "Parent", "Item", "Is_Header", "2025", "2024", "2023", "Reconciliation"]]
            for y in ["2025", "2024", "2023"]:
                s[y] = s[y].astype("Int64")
            s.to_excel(writer, sheet_name=sheet, index=False)


def main() -> None:
    if not INPUT_FILE.exists():
        raise SystemExit(f"Input file not found: {INPUT_FILE}")

    raw = pd.read_excel(INPUT_FILE, sheet_name=INPUT_SHEET, header=None)
    full = reconstruct(raw)
    full_checked = apply_parent_child_checks(full)

    md = to_markdown(full_checked[["Row", "Statement", "Parent", "Item", "Is_Header", "2025", "2024", "2023", "Reconciliation"]])
    OUTPUT_MD.write_text(md, encoding="utf-8")
    export_excel(full_checked)

    print(f"Markdown: {OUTPUT_MD}")
    print(f"Excel: {OUTPUT_XLSX}")
    print(md)


if __name__ == "__main__":
    main()
