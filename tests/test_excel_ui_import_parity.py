import tempfile
from pathlib import Path

import pandas as pd
import pytest
import tkinter as tk
import tkinter.filedialog as fd
import tkinter.messagebox as mb

from main import SECFinancialSystem


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NVDA_FILE = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\test 3\TEQST\NVDA_analysis_20260324_191515.xlsx")
KO_FILE = PROJECT_ROOT / "outputs" / "test_regen2" / "KO_regen2.xlsx"
NVDA_LATEST_FILE = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\test 3\TEQST\NVDA_analysis_20260325_113840.xlsx")
KO_LATEST_FILE = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\test 3\TEQST\KO_analysis_20260325_114028.xlsx")
JPM_LATEST_FILE = Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\test 3\TEQST\JPM_analysis_20260325_114240.xlsx")


def _make_app():
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        pytest.skip(f"Tk unavailable in test environment: {exc}")
    root.withdraw()
    mb.showerror = lambda *a, **k: None
    mb.showinfo = lambda *a, **k: None
    return root, SECFinancialSystem(root)


def _close_app(root):
    try:
        root.destroy()
    except Exception:
        pass


@pytest.mark.skipif(not NVDA_FILE.exists(), reason="NVDA Excel fixture not available")
def test_nvda_ui_tables_have_no_short_or_duplicate_labels():
    root, app = _make_app()
    try:
        assert app.load_results_from_excel(file_path=str(NVDA_FILE), show_success_message=False)
        app.display_raw_data()
        app.display_ratios()
        app.display_strategic_analysis()

        frames = [
            app._snapshot_raw_tree_df(),
            app._snapshot_ratios_tree_df(),
            app._snapshot_strategic_tree_df(),
        ]
        for df in frames:
            assert df is not None and not df.empty
            labels = df.iloc[:, 0].astype(str).tolist()
            duplicates = [x for x in sorted(set(labels)) if labels.count(x) > 1 and x and not x.startswith("---")]
            short = [x for x in labels if len(str(x).strip()) <= 3 and str(x).strip() and x not in {"ROE", "EPS"}]
            assert not duplicates, f"duplicate labels detected: {duplicates[:10]}"
            assert not short, f"short/truncated labels detected: {short[:10]}"
    finally:
        _close_app(root)


@pytest.mark.skipif(not KO_FILE.exists(), reason="KO Excel fixture not available")
def test_ko_excel_import_keeps_consumer_profile_and_filters_bank_concepts():
    root, app = _make_app()
    try:
        assert app.load_results_from_excel(file_path=str(KO_FILE), show_success_message=False)
        sector_gating = app.current_data.get("sector_gating", {}) or {}
        assert sector_gating.get("sub_profile") == "consumer_staples"
        assert sector_gating.get("profile") == "industrial"

        bank_markers = ("Deposits", "DepositLiabilities", "LoansReceivable", "NetInterestIncome")
        for year in (2015, 2020, 2025):
            row = ((app.current_data.get("data_by_year", {}) or {}).get(year, {}) or {})
            leaked = [k for k in row.keys() if any(marker in str(k) for marker in bank_markers)]
            assert not leaked, f"bank concepts leaked into KO year {year}: {leaked}"
    finally:
        _close_app(root)


@pytest.mark.skipif(not NVDA_FILE.exists(), reason="NVDA Excel fixture not available")
def test_minimal_excel_export_matches_visible_ui_shapes():
    root, app = _make_app()
    try:
        assert app.load_results_from_excel(file_path=str(NVDA_FILE), show_success_message=False)
        app.display_raw_data()
        app.display_ratios()
        app.display_strategic_analysis()

        raw_ui = app._snapshot_raw_tree_df()
        ratios_ui = app._snapshot_ratios_tree_df()
        strat_ui = app._snapshot_strategic_tree_df()

        out = Path(tempfile.gettempdir()) / "pytest_nvda_ui_parity_export.xlsx"
        fd.asksaveasfilename = lambda **k: str(out)
        app._export_to_excel_minimal()
        assert out.exists()

        raw_x = pd.read_excel(out, sheet_name="Raw_by_Year")
        ratios_x = pd.read_excel(out, sheet_name="Ratios")
        strat_x = pd.read_excel(out, sheet_name="Strategic")

        assert tuple(raw_x.shape) == tuple(raw_ui.shape)
        assert tuple(ratios_x.shape) == tuple(ratios_ui.shape)
        assert tuple(strat_x.shape) == tuple(strat_ui.shape)
    finally:
        _close_app(root)


@pytest.mark.skipif(not NVDA_LATEST_FILE.exists(), reason="Latest NVDA Excel fixture not available")
def test_nvda_loaded_file_repairs_absurd_pe_and_zero_pb():
    root, app = _make_app()
    try:
        assert app.load_results_from_excel(file_path=str(NVDA_LATEST_FILE), show_success_message=False)
        ratios = app.current_data.get("financial_ratios", {}) or {}
        pe_2015 = (ratios.get(2015, {}) or {}).get("pe_ratio")
        pb_2024 = (ratios.get(2024, {}) or {}).get("pb_ratio")
        pb_2025 = (ratios.get(2025, {}) or {}).get("pb_ratio")
        assert pe_2015 is not None and float(pe_2015) >= 2.0
        assert pb_2024 is not None and float(pb_2024) > 0.10
        assert pb_2025 is not None and float(pb_2025) > 0.10
    finally:
        _close_app(root)


@pytest.mark.skipif(not KO_LATEST_FILE.exists(), reason="Latest KO Excel fixture not available")
def test_ko_loaded_file_uses_reasonable_dpo():
    root, app = _make_app()
    try:
        assert app.load_results_from_excel(file_path=str(KO_LATEST_FILE), show_success_message=False)
        ratios = app.current_data.get("financial_ratios", {}) or {}
        for year in range(2015, 2026):
            dpo = (ratios.get(year, {}) or {}).get("ap_days")
            if dpo is not None:
                assert float(dpo) <= 365.0, f"KO {year} DPO still implausible: {dpo}"
    finally:
        _close_app(root)


@pytest.mark.skipif(not JPM_LATEST_FILE.exists(), reason="Latest JPM Excel fixture not available")
def test_jpm_ai_snapshot_blocks_fcf_yield_for_bank():
    root, app = _make_app()
    try:
        assert app.load_results_from_excel(file_path=str(JPM_LATEST_FILE), show_success_message=False)
        ai = app._build_ai_insights_snapshot()
        iq = (ai or {}).get("investment_quality", {}) or {}
        comps = iq.get("components", {}) or {}
        assert comps.get("fcf_yield") is None
    finally:
        _close_app(root)
