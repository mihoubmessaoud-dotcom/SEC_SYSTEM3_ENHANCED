from __future__ import annotations

from pathlib import Path
import sys


def run_smoke_export() -> Path:
    import tkinter as tk

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    import main as app_mod

    root = tk.Tk()
    root.withdraw()

    # Patch UI popups / file dialogs for non-interactive smoke run.
    try:
        app_mod.messagebox.showinfo = lambda *a, **k: None
        app_mod.messagebox.showwarning = lambda *a, **k: None
        app_mod.messagebox.showerror = lambda *a, **k: None
    except Exception:
        pass

    out_path = Path("outputs") / "_smoke_export.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        app_mod.filedialog.asksaveasfilename = lambda **k: str(out_path)
    except Exception:
        pass

    app = app_mod.SECFinancialSystem(root)

    # Minimal-but-valid structure to exercise the full exporter without real SEC fetch.
    years = list(range(2019, 2022))
    app.current_data = {
        "company_info": {"ticker": "SMOKE"},
        "data_by_year": {y: {} for y in years},
        "data_layers": {
            "layer1_by_year": {
                2019: {"إجمالي الأصول (Assets)": 1000.0, "الأصول المتداولة (AssetsCurrent)": 400.0},
                2020: {"إجمالي الأصول (Assets)": 1100.0, "الأصول المتداولة (AssetsCurrent)": 450.0},
                2021: {"إجمالي الأصول (Assets)": 1200.0, "الأصول المتداولة (AssetsCurrent)": 500.0},
            },
            "layer2_by_year": {y: {"market:price": 10.0, "market:shares_outstanding": 100.0} for y in years},
        },
        "financial_ratios": {y: {"pb_ratio": 1.2, "pe_ratio": 15.0} for y in years},
        "strategic_analysis": {y: {"Investment_View": "WATCH_ONLY"} for y in years},
        "sector_gating": {"blocked_ratios": [], "blocked_strategic_metrics": []},
    }

    app.export_to_excel_safe()
    try:
        root.destroy()
    except Exception:
        pass
    return out_path


if __name__ == "__main__":
    p = run_smoke_export()
    print(str(p))
