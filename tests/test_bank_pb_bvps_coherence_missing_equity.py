import math

import pandas as pd
import pytest
import tkinter as tk
import tkinter.messagebox as mb

from main import SECFinancialSystem


def _make_app():
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        pytest.skip(f"Tk unavailable in test environment: {exc}")
    root.withdraw()
    mb.showerror = lambda *a, **k: None
    mb.showinfo = lambda *a, **k: None
    mb.showwarning = lambda *a, **k: None
    return root, SECFinancialSystem(root)


def _close_app(root):
    try:
        root.destroy()
    except Exception:
        pass


def test_bank_pb_bvps_aligns_to_market_pb_when_equity_missing():
    """
    Regression guard:
    If a bank's equity anchors are missing in a year, the system must not emit
    contradictory PB/BVPS across the workbook paths. PB should follow market PB,
    and BVPS should align to price/PB.
    """
    root, app = _make_app()
    try:
        year = 2025
        market_pb = 2.397080197799537
        price = 322.2200012207031
        implied_bvps = price / market_pb

        ratios_by_year = {
            2024: {},
            # Legacy-broken inputs: BVPS implies PB ≈ 1.087, which conflicts with market PB.
            2025: {
                "pb_ratio": 1.087415188351573,
                "book_value_per_share": 296.3173631123919,
                "eps_basic": 20.55043227665706,
                "pe_ratio": 15.67947558877913,
                "market_cap": 868792.9527300684,
            },
        }

        data_by_year = {
            2024: {
                # Equity present in prior year (million USD units in this system).
                "StockholdersEquity": 362438.0,
            },
            2025: {
                # Equity intentionally missing to simulate JPM 2025 gap.
            },
        }

        data_layers = {
            "layer2_by_year": {
                2024: {
                    "market:pb_ratio": 1.850296,
                    "market:price": 239.7100067138672,
                    "market:market_cap": 670617.6051065618,
                    "market:shares_outstanding": 2797.620401,
                },
                2025: {
                    "market:pb_ratio": market_pb,
                    "market:price": price,
                    "market:market_cap": 868792.9527300684,
                    "market:shares_outstanding": 2696.272576,
                },
            },
            "layer4_by_year": {},
        }

        app.current_data = {
            "company_info": {"ticker": "JPM", "name": "JPM"},
            "sector_gating": {"profile": "commercial_bank", "sub_profile": "commercial_bank"},
            "data_by_year": data_by_year,
            "financial_ratios": ratios_by_year,
            "data_layers": data_layers,
        }

        issues = app._apply_pre_export_quality_gate(
            years=[2024, 2025],
            data_by_year=data_by_year,
            ratios_by_year=ratios_by_year,
            data_layers=data_layers,
        )

        # After gate: PB must follow market PB and BVPS must match price/PB.
        pb_used = ratios_by_year[year].get("pb_ratio_used", ratios_by_year[year].get("pb_ratio"))
        assert pb_used is not None
        assert abs(float(pb_used) - market_pb) < 1e-6

        bvps = ratios_by_year[year].get("book_value_per_share")
        assert bvps is not None
        assert math.isfinite(float(bvps))
        assert abs(float(bvps) - implied_bvps) < 1e-6

        # Investor_Verdict must not reintroduce the old conflicting PB value.
        critical_df = pd.DataFrame([{"Severity": "INFO", "Type": "NONE", "Metric": "No critical issues", "Year": "", "Details": ""}])
        forecasts_df = pd.DataFrame()
        verdict_df = app._build_investor_verdict_df(
            years=[2024, 2025],
            ratios_by_year=ratios_by_year,
            gate_issues=issues,
            critical_df=critical_df,
            forecasts_df=forecasts_df,
            sector_profile="commercial_bank",
            blocked_ratios=set(),
        )
        v2025 = verdict_df[verdict_df["Year"] == 2025]
        assert not v2025.empty
        pb_verdict = float(v2025.iloc[0]["PB_Ratio"])
        assert abs(pb_verdict - market_pb) < 1e-6
    finally:
        _close_app(root)

