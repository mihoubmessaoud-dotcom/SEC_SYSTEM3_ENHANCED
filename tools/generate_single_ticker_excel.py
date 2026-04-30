from __future__ import annotations

from pathlib import Path
import sys


def generate(ticker: str, start_year: int, end_year: int, out_path: Path) -> Path:
    import tkinter as tk

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    import main as app_mod

    root = tk.Tk()
    root.withdraw()

    # Patch UI popups / file dialogs for non-interactive run.
    try:
        app_mod.messagebox.showinfo = lambda *a, **k: None
        app_mod.messagebox.showwarning = lambda *a, **k: None
        app_mod.messagebox.showerror = lambda *a, **k: None
        app_mod.messagebox.askyesno = lambda *a, **k: True
    except Exception:
        pass

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        app_mod.filedialog.asksaveasfilename = lambda **k: str(out_path)
    except Exception:
        pass

    app = app_mod.SECFinancialSystem(root)
    try:
        # Ensure exporter uses the intended year window (export_to_excel relies on UI vars).
        try:
            app.start_year_var.set(str(int(start_year)))
            app.end_year_var.set(str(int(end_year)))
        except Exception:
            pass
        res = app.fetcher.fetch_company_data(
            ticker,
            int(start_year),
            int(end_year),
            filing_type="10-K",
            callback=None,
            include_all_concepts=False,
        )
        if not isinstance(res, dict) or not res.get("success"):
            raise RuntimeError(str((res or {}).get("error") or "Unknown fetch failure"))
        app.current_data = res
        app.export_to_excel_safe()
    finally:
        try:
            root.destroy()
        except Exception:
            pass

    return out_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python tools/generate_single_ticker_excel.py TICKER [START] [END] [OUT]")
    ticker = sys.argv[1]
    start = int(sys.argv[2]) if len(sys.argv) > 2 else 2015
    end = int(sys.argv[3]) if len(sys.argv) > 3 else 2025
    out = Path(sys.argv[4]) if len(sys.argv) > 4 else Path("outputs") / f"{ticker}_headless.xlsx"
    p = generate(ticker=ticker, start_year=start, end_year=end, out_path=out)
    print(str(p))
