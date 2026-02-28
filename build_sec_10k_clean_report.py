#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


INPUT_XLSX = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\Nouveau Feuille de calcul Microsoft Excel.xlsx")
OUTPUT_XLSX = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\Intel_SEC_10K_Cleaned.xlsx")
SOURCE_SHEET = "Sheet1"
TARGET_YEARS = [2023, 2024, 2025]
MILLION_SCALE = 1_000_000


def parse_years(df: pd.DataFrame) -> Dict[int, int]:
    year_to_col: Dict[int, int] = {}
    for _, row in df.iterrows():
        for col in range(1, min(8, len(df.columns))):
            cell = str(row.get(col, "") or "")
            m = re.search(r"\b(20\d{2})\b", cell)
            if m:
                y = int(m.group(1))
                if y in TARGET_YEARS and y not in year_to_col:
                    year_to_col[y] = col
        if len(year_to_col) == len(TARGET_YEARS):
            break
    return year_to_col


def scrub_to_millions_number(v) -> Optional[int]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None

    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]

    s = s.replace("$", "").replace(",", "").replace(" ", "")
    s = s.replace("\u00a0", "")
    if "." in s:
        left, right = s.split(".", 1)
        if right.isdigit() and left.replace("-", "").isdigit():
            # dot-grouped style where trailing zeros may be dropped by Excel parsing:
            # 17.487 -> 17487, 13.06 -> 13060, 10.46 -> 10460
            if len(right) < 3:
                right = right.ljust(3, "0")
            s = f"{left}{right}"
        else:
            s = s.replace(".", "")

    if not s:
        return None

    if s.startswith("-"):
        negative = True
        s = s[1:]

    if not re.fullmatch(r"\d+", s):
        return None

    n_millions = int(s)
    val = n_millions * MILLION_SCALE
    return -val if negative else val


def find_row_value(df: pd.DataFrame, label_keywords: List[str], year_col: int, exclude_keywords: Optional[List[str]] = None) -> Optional[int]:
    exclude_keywords = [k.lower() for k in (exclude_keywords or [])]
    for _, row in df.iterrows():
        label = str(row.get(0, "") or "").strip().lower()
        if all(k in label for k in label_keywords):
            if any(x in label for x in exclude_keywords):
                continue
            return scrub_to_millions_number(row.get(year_col))
    return None


def extract_metrics(df: pd.DataFrame, year_to_col: Dict[int, int]) -> Dict[int, Dict[str, Optional[int]]]:
    out: Dict[int, Dict[str, Optional[int]]] = {}
    for y, col in year_to_col.items():
        revenue = find_row_value(df, ["net revenue"], col)
        cogs = find_row_value(df, ["total cost of sales"], col)
        gross_profit = find_row_value(df, ["gross profit"], col)

        net_income = find_row_value(df, ["net income"], col)

        total_assets = find_row_value(df, ["total assets"], col)
        total_equity = find_row_value(df, ["total stockholders", "equity"], col)
        total_liabilities = find_row_value(df, ["total liabilities"], col, exclude_keywords=["and stockholders", "and shareholders"])
        if total_liabilities is None and total_assets is not None and total_equity is not None:
            total_liabilities = total_assets - total_equity

        op_cf = find_row_value(df, ["operating activity", "including discontinued operation", "total"], col)
        if op_cf is None:
            op_cf = find_row_value(df, ["net cash provided by operating activities"], col)
        if op_cf is None:
            op_cf = find_row_value(df, ["cash provided by (used in) operating activity", "continuing operation"], col)

        out[y] = {
            "Total Revenue": revenue,
            "COGS": cogs,
            "Gross Profit": gross_profit,
            "Net Income": net_income,
            "Total Assets": total_assets,
            "Total Liabilities": total_liabilities,
            "Total Equity": total_equity,
            "Operating Cash Flow": op_cf,
        }
    return out


def validate(metrics: Dict[int, Dict[str, Optional[int]]]) -> List[str]:
    alerts: List[str] = []
    for y in TARGET_YEARS:
        m = metrics.get(y, {})
        rev = m.get("Total Revenue")
        cogs = m.get("COGS")
        gp = m.get("Gross Profit")
        assets = m.get("Total Assets")
        liab = m.get("Total Liabilities")
        eq = m.get("Total Equity")

        if rev is not None and cogs is not None and gp is not None:
            calc_gp = rev - cogs
            if calc_gp != gp:
                alerts.append(
                    f"[{y}] Gross Profit mismatch at lines: Net revenue / Total cost of sales / Gross profit "
                    f"(expected {calc_gp}, found {gp})"
                )
        else:
            alerts.append(f"[{y}] Missing line for gross-profit validation.")

        if assets is not None and liab is not None and eq is not None:
            calc_assets = liab + eq
            if calc_assets != assets:
                alerts.append(
                    f"[{y}] Balance mismatch at lines: Total assets / Total liabilities / Total stockholders' equity "
                    f"(expected assets {calc_assets}, found {assets})"
                )
        else:
            alerts.append(f"[{y}] Missing line for balance-sheet validation.")
    return alerts


def build_statement_df(metrics: Dict[int, Dict[str, Optional[int]]], rows: List[str]) -> pd.DataFrame:
    data = {"Line Item": rows}
    for y in TARGET_YEARS:
        col_vals = []
        for r in rows:
            v = metrics.get(y, {}).get(r)
            col_vals.append(int(v) if isinstance(v, int) else None)
        data[str(y)] = col_vals
    out = pd.DataFrame(data)
    for y in TARGET_YEARS:
        out[str(y)] = out[str(y)].astype("Int64")
    return out


def main() -> None:
    if not INPUT_XLSX.exists():
        raise SystemExit(f"Input file not found: {INPUT_XLSX}")

    raw = pd.read_excel(INPUT_XLSX, sheet_name=SOURCE_SHEET, header=None)
    year_to_col = parse_years(raw)
    metrics = extract_metrics(raw, year_to_col)
    alerts = validate(metrics)

    income_df = build_statement_df(
        metrics,
        rows=["Total Revenue", "COGS", "Gross Profit", "Net Income"],
    )
    balance_df = build_statement_df(
        metrics,
        rows=["Total Assets", "Total Liabilities", "Total Equity"],
    )
    cash_df = build_statement_df(
        metrics,
        rows=["Operating Cash Flow"],
    )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        income_df.to_excel(writer, sheet_name="Income Statement", index=False)
        balance_df.to_excel(writer, sheet_name="Balance Sheet", index=False)
        cash_df.to_excel(writer, sheet_name="Cash Flow", index=False)

    print(f"Output written: {OUTPUT_XLSX}")
    if alerts:
        print("Validation alerts:")
        for a in alerts:
            print(a)
    else:
        print("Validation OK: no mismatches detected.")


if __name__ == "__main__":
    main()
