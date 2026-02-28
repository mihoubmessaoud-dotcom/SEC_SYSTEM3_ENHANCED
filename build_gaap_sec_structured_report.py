#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


INPUT_FILE = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\Nouveau Feuille de calcul Microsoft Excel.xlsx")
INPUT_SHEET = "Sheet1"
OUTPUT_FILE = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\Intel_GAAP_SEC_Statements.xlsx")

# Dates required by the user (exact SEC header dates)
DATE_COLUMNS = ["Dec. 27, 2025", "Dec. 28, 2024", "Dec. 30, 2023"]


def to_million_int(v) -> Optional[int]:
    """Normalize to million USD integer.
    Rule: decimal values are treated as thousands of millions (x10^3).
    """
    if pd.isna(v):
        return None

    raw = str(v).strip()
    if not raw:
        return None

    neg = False
    if raw.startswith("(") and raw.endswith(")"):
        neg = True
        raw = raw[1:-1].strip()
    if raw.startswith("-"):
        neg = True
        raw = raw[1:].strip()

    cleaned = raw.replace("$", "").replace(",", "").replace(" ", "").replace("\u00a0", "")
    if not cleaned:
        return None

    # Keep only numeric + dot
    if not re.fullmatch(r"\d+(\.\d+)?", cleaned):
        return None

    if "." in cleaned:
        whole, frac = cleaned.split(".", 1)
        frac = frac.ljust(3, "0")[:3]
        n = int(whole) * 1000 + int(frac)
    else:
        n = int(cleaned)

    return -n if neg else n


def find_row(df: pd.DataFrame, start: int, end: int, label_contains: str) -> Optional[pd.Series]:
    needle = label_contains.lower()
    for i in range(start, end):
        label = str(df.iat[i, 0] if i < len(df) else "")
        if needle in label.lower():
            return df.iloc[i]
    return None


def value_by_date(row: Optional[pd.Series], date_to_col: Dict[str, int]) -> Dict[str, Optional[int]]:
    out = {d: None for d in DATE_COLUMNS}
    if row is None:
        return out
    for d, c in date_to_col.items():
        out[d] = to_million_int(row.iloc[c])
    return out


def sum_by_date(*series_maps: Dict[str, Optional[int]]) -> Dict[str, Optional[int]]:
    out = {d: None for d in DATE_COLUMNS}
    for d in DATE_COLUMNS:
        vals = [m.get(d) for m in series_maps]
        if any(v is None for v in vals):
            out[d] = None
        else:
            out[d] = int(sum(int(v) for v in vals))
    return out


def to_df(rows: List[Tuple[str, Dict[str, Optional[int]]]]) -> pd.DataFrame:
    data = {"Line Item": [r[0] for r in rows]}
    for d in DATE_COLUMNS:
        data[d] = [r[1].get(d) for r in rows]
    df = pd.DataFrame(data)
    for d in DATE_COLUMNS:
        df[d] = df[d].astype("Int64")
    return df


def main() -> None:
    if not INPUT_FILE.exists():
        raise SystemExit(f"Input file not found: {INPUT_FILE}")

    df = pd.read_excel(INPUT_FILE, sheet_name=INPUT_SHEET, header=None)

    # Section boundaries in current source sheet
    inc_start, inc_end = 0, 34
    bs_start, bs_end = 41, 77
    cf_start, cf_end = 124, 171

    # Dates from SEC headers
    # Income dates are complete (2025, 2024, 2023)
    income_date_to_col = {str(df.iat[1, c]): c for c in [1, 2, 3] if str(df.iat[1, c]).strip() in DATE_COLUMNS}
    # Balance sheet has 2025/2024 only in this file; keep 2023 as empty
    bs_date_to_col = {str(df.iat[41, c]): c for c in [1, 2] if str(df.iat[41, c]).strip() in DATE_COLUMNS}
    # Cash flow dates are complete
    cf_date_to_col = {str(df.iat[125, c]): c for c in [1, 2, 3] if str(df.iat[125, c]).strip() in DATE_COLUMNS}

    # Income Statement mapping
    revenue = value_by_date(find_row(df, inc_start, inc_end, "net revenue"), income_date_to_col)
    cogs = value_by_date(find_row(df, inc_start, inc_end, "total cost of sales"), income_date_to_col)
    gross_profit = value_by_date(find_row(df, inc_start, inc_end, "gross profit"), income_date_to_col)
    total_opex = value_by_date(find_row(df, inc_start, inc_end, "total operating expenses"), income_date_to_col)
    operating_income = value_by_date(find_row(df, inc_start, inc_end, "operating income"), income_date_to_col)
    net_income = value_by_date(find_row(df, inc_start, inc_end, "net income"), income_date_to_col)

    # Balance Sheet mapping
    current_assets = value_by_date(find_row(df, bs_start, bs_end, "total current assets"), bs_date_to_col)
    ppe = value_by_date(find_row(df, bs_start, bs_end, "property and equipment, net"), bs_date_to_col)
    goodwill = value_by_date(find_row(df, bs_start, bs_end, "goodwill"), bs_date_to_col)
    acq_intang = value_by_date(find_row(df, bs_start, bs_end, "acquisition-related intangibles, net"), bs_date_to_col)
    dta = value_by_date(find_row(df, bs_start, bs_end, "deferred tax assets, net"), bs_date_to_col)
    other_nca = value_by_date(find_row(df, bs_start, bs_end, "other non-current assets"), bs_date_to_col)
    non_current_assets = sum_by_date(ppe, goodwill, acq_intang, dta, other_nca)
    total_assets = value_by_date(find_row(df, bs_start, bs_end, "total assets"), bs_date_to_col)

    current_liab = value_by_date(find_row(df, bs_start, bs_end, "total current liabilities"), bs_date_to_col)
    ltd = value_by_date(find_row(df, bs_start, bs_end, "long-term debt, net of current portion"), bs_date_to_col)
    ltl = value_by_date(find_row(df, bs_start, bs_end, "long-term operating lease liabilities"), bs_date_to_col)
    dtl = value_by_date(find_row(df, bs_start, bs_end, "deferred tax liabilities"), bs_date_to_col)
    other_ltl = value_by_date(find_row(df, bs_start, bs_end, "other long-term liabilities"), bs_date_to_col)
    non_current_liab = sum_by_date(ltd, ltl, dtl, other_ltl)
    total_liabilities = sum_by_date(current_liab, non_current_liab)
    total_equity = value_by_date(find_row(df, bs_start, bs_end, "total stockholders’ equity"), bs_date_to_col)

    # Cash Flow mapping
    op_cf = value_by_date(
        find_row(df, cf_start, cf_end, "cash provided by (used in) operating activity, including discontinued operation, total"),
        cf_date_to_col,
    )
    if all(v is None for v in op_cf.values()):
        op_cf = value_by_date(find_row(df, cf_start, cf_end, "net cash provided by operating activities"), cf_date_to_col)

    alerts: List[str] = []

    # Validation: Operating Income = Gross Profit - Total operating expenses
    for d in DATE_COLUMNS:
        gp, opx, opi = gross_profit.get(d), total_opex.get(d), operating_income.get(d)
        if gp is None or opx is None or opi is None:
            alerts.append(f"[Income][{d}] Missing row(s) for Operating Income validation.")
            continue
        expected = gp - opx
        if expected != opi:
            alerts.append(
                f"[Income][{d}] Operating Income mismatch at rows Gross profit / Total operating expenses / Operating income "
                f"(expected {expected}, found {opi})."
            )
            operating_income[d] = None  # strict reject

    # Validation: Total Assets = Current + Non-current assets
    for d in DATE_COLUMNS:
        ta, ca, nca = total_assets.get(d), current_assets.get(d), non_current_assets.get(d)
        if ta is None or ca is None or nca is None:
            alerts.append(f"[Balance][{d}] Missing row(s) for assets hierarchy validation.")
            continue
        if ta != (ca + nca):
            alerts.append(
                f"[Balance][{d}] Assets hierarchy mismatch at rows Total current assets / Non-current assets block / Total assets "
                f"(expected {ca+nca}, found {ta})."
            )
            total_assets[d] = None  # strict reject

    # Validation: Total Assets = Total Liabilities + Equity
    for d in DATE_COLUMNS:
        ta, tl, te = total_assets.get(d), total_liabilities.get(d), total_equity.get(d)
        if ta is None or tl is None or te is None:
            alerts.append(f"[Balance][{d}] Missing row(s) for accounting equation validation.")
            continue
        expected = tl + te
        if ta != expected:
            alerts.append(
                f"[Balance][{d}] Accounting equation mismatch at rows Total assets / Total liabilities / Total stockholders’ equity "
                f"(expected assets {expected}, found {ta})."
            )
            # strict reject for balance rows of this date
            for m in [current_assets, non_current_assets, total_assets, current_liab, non_current_liab, total_liabilities, total_equity]:
                m[d] = None

    income_df = to_df(
        [
            ("Revenue", revenue),
            ("COGS", cogs),
            ("Gross Profit", gross_profit),
            ("Total operating expenses", total_opex),
            ("Operating Income", operating_income),
            ("Net Income", net_income),
        ]
    )
    balance_df = to_df(
        [
            ("Current Assets", current_assets),
            ("Non-current Assets", non_current_assets),
            ("Total Assets", total_assets),
            ("Current Liabilities", current_liab),
            ("Non-current Liabilities", non_current_liab),
            ("Total Liabilities", total_liabilities),
            ("Total Equity", total_equity),
        ]
    )
    cash_df = to_df(
        [
            ("Net cash provided by operating activities", op_cf),
        ]
    )

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        income_df.to_excel(writer, sheet_name="Income Statement", index=False)
        balance_df.to_excel(writer, sheet_name="Balance Sheet", index=False)
        cash_df.to_excel(writer, sheet_name="Cash Flow", index=False)

    print(f"Output written: {OUTPUT_FILE}")
    if alerts:
        print("Validation alerts:")
        for a in alerts:
            print(a)
    else:
        print("Validation OK.")


if __name__ == "__main__":
    main()
