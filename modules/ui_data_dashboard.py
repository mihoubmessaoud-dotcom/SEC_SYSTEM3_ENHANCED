from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import math
import tkinter as tk


@dataclass
class DashboardMetric:
    icon: str
    label: str
    key: str
    fmt: str  # 'pct' | 'num' | 'money_m'


def _rounded_rect(canvas: tk.Canvas, x1: float, y1: float, x2: float, y2: float, r: float, **kwargs):
    r = max(0, float(r))
    if r <= 0:
        return canvas.create_rectangle(x1, y1, x2, y2, **kwargs)
    points = [
        x1 + r, y1,
        x2 - r, y1,
        x2, y1,
        x2, y1 + r,
        x2, y2 - r,
        x2, y2,
        x2 - r, y2,
        x1 + r, y2,
        x1, y2,
        x1, y2 - r,
        x1, y1 + r,
        x1, y1,
    ]
    return canvas.create_polygon(points, smooth=True, **kwargs)


class DataDashboard(tk.Frame):
    """
    Canvas-based premium dashboard for the Data tab.
    UI-only: caller provides value_getter and formatting functions.
    """

    def __init__(self, parent: tk.Misc, *, palette: Dict[str, str], fonts: Dict[str, Any], lang: str = "ar"):
        super().__init__(parent, bg=palette["dash_bg"])
        self.palette = palette
        self.fonts = fonts
        self.lang = lang

        self.table = _DashboardCanvasTable(self, palette=palette, fonts=fonts, lang=lang)
        self.table.pack(fill="both", expand=True)

        self.cards = _DashboardCardsStrip(self, palette=palette, fonts=fonts, lang=lang)
        self.cards.pack(fill="x", pady=(10, 0))

    def render(
        self,
        *,
        years: List[int],
        groups: List[tuple[str, List[DashboardMetric]]],
        value_getter: Callable[[int, str], Optional[float]],
        fmtters: Dict[str, Callable[[Optional[float]], str]],
        kpi_payload: Dict[str, Optional[float]],
    ) -> None:
        rows: List[dict] = []
        for title, metrics in groups:
            rows.append({"type": "group", "label": title})
            for m in metrics:
                rows.append({"type": "metric", "icon": m.icon, "label": m.label, "key": m.key, "fmt": m.fmt})
        self.table.render(years=years, rows=rows, value_getter=value_getter, fmtters=fmtters)
        self.cards.update_cards(kpi_payload)


class _DashboardCanvasTable(tk.Frame):
    def __init__(self, parent: tk.Misc, *, palette: Dict[str, str], fonts: Dict[str, Any], lang: str = "ar"):
        super().__init__(parent, bg=palette["dash_panel"])
        self.palette = palette
        self.fonts = fonts
        self.lang = lang
        self._years: List[int] = []
        self._rows: List[dict] = []
        self._value_getter: Optional[Callable[[int, str], Optional[float]]] = None
        self._fmtters: Dict[str, Callable[[Optional[float]], str]] = {}

        self.canvas = tk.Canvas(
            self,
            bg=palette["dash_bg"],
            highlightthickness=1,
            highlightbackground=palette["dash_border"],
            bd=0,
        )
        self.vs = tk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.hs = tk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
        self.canvas.configure(yscrollcommand=self.vs.set, xscrollcommand=self.hs.set)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.vs.grid(row=0, column=1, sticky="ns")
        self.hs.grid(row=1, column=0, sticky="ew")
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        self.canvas.bind("<Configure>", lambda e: self._redraw())

    def render(
        self,
        *,
        years: List[int],
        rows: List[dict],
        value_getter: Callable[[int, str], Optional[float]],
        fmtters: Dict[str, Callable[[Optional[float]], str]],
    ) -> None:
        self._years = list(years or [])
        self._rows = list(rows or [])
        self._value_getter = value_getter
        self._fmtters = dict(fmtters or {})
        self._redraw()

    def _redraw(self) -> None:
        c = self.canvas
        c.delete("all")
        years = self._years
        rows = self._rows
        if not years or not rows or not self._value_getter:
            c.configure(scrollregion=(0, 0, 1, 1))
            return

        palette = self.palette

        # Layout (tuned toward the provided design)
        pad_x = 14
        pad_y = 10
        row_h = 42
        header_h = 46
        label_w = 420
        year_w = 190
        icon_w = 48

        total_w = pad_x * 2 + label_w + (year_w * len(years))
        total_h = pad_y * 2 + header_h + (row_h * len(rows))

        # Header bar
        c.create_rectangle(0, 0, total_w, header_h + pad_y, fill=palette["dash_header2"], outline="")

        x0 = pad_x
        y0 = pad_y

        # Label header
        label_head = "البيانات" if self.lang == "ar" else "Data"
        c.create_text(
            x0 + label_w / 2,
            y0 + header_h / 2,
            text=label_head,
            fill=palette["dash_text"],
            font=self.fonts["label"],
        )
        # Year headers
        for i, y in enumerate(years):
            cx = x0 + label_w + (i * year_w) + year_w / 2
            c.create_text(cx, y0 + header_h / 2, text=str(y), fill=palette["dash_text"], font=self.fonts["label"])

        start_y = y0 + header_h
        for ridx, r in enumerate(rows):
            top = start_y + (ridx * row_h)
            bot = top + row_h
            if r.get("type") == "group":
                c.create_rectangle(0, top, total_w, bot, fill=palette["dash_header"], outline=palette["dash_border"])
                c.create_text(
                    x0 + 16,
                    (top + bot) / 2,
                    text=str(r.get("label") or ""),
                    fill=palette["dash_text"],
                    font=self.fonts["label"],
                    anchor="w",
                )
                continue

            bg = palette["dash_panel2"] if (ridx % 2 == 0) else palette["dash_panel"]
            c.create_rectangle(0, top, total_w, bot, fill=bg, outline=palette["dash_border"])

            icon = str(r.get("icon") or "")
            label = str(r.get("label") or "")
            key = str(r.get("key") or "")
            fmt = str(r.get("fmt") or "")

            # Premium indicator chip (varies by metric type, like the reference design).
            chip_x = x0 + 10
            chip_y = (top + bot) / 2
            chip_w = 30
            chip_h = 26
            x1 = chip_x
            y1 = chip_y - chip_h / 2
            x2 = chip_x + chip_w
            y2 = chip_y + chip_h / 2

            def indicator_kind() -> str:
                k = (key or "").lower()
                if fmt == "money_m":
                    return "bars"
                if "debt" in k or "wacc" in k or "cost_of_debt" in k:
                    return "pct_badge"
                return "icon"

            kind = indicator_kind()
            if kind == "bars":
                _rounded_rect(c, x1, y1, x2, y2, 8, fill=palette["dash_panel"], outline=palette["dash_teal"], width=2)
                bx = x1 + 8
                by = y2 - 6
                bar_color = palette["dash_teal"]
                c.create_rectangle(bx, by - 6, bx + 3, by, fill=bar_color, outline="")
                c.create_rectangle(bx + 6, by - 10, bx + 9, by, fill=bar_color, outline="")
                c.create_rectangle(bx + 12, by - 14, bx + 15, by, fill=bar_color, outline="")
            elif kind == "pct_badge":
                _rounded_rect(c, x1, y1, x2, y2, 8, fill=palette["dash_panel"], outline=palette["dash_red"], width=2)
                c.create_text((x1 + x2) / 2, (y1 + y2) / 2 + 0.5, text="%", fill=palette["dash_red"], font=self.fonts["label"])
            else:
                _rounded_rect(c, x1, y1, x2, y2, 8, fill=palette["dash_panel"], outline=palette["dash_teal"], width=2)
                c.create_text((x1 + x2) / 2, (y1 + y2) / 2 + 0.5, text=icon, fill=palette["dash_teal"], font=self.fonts["label"])

            c.create_text(x0 + icon_w, (top + bot) / 2, text=label, fill=palette["dash_text"], font=self.fonts["tree"], anchor="w")

            fmtter = self._fmtters.get(fmt, lambda v: str(v) if v is not None else "—")

            prev_val: Optional[float] = None
            for i, y in enumerate(years):
                v = self._value_getter(y, key)
                disp = fmtter(v)
                cx1 = x0 + label_w + (i * year_w)
                cx2 = cx1 + year_w
                cy = (top + bot) / 2
                c.create_text((cx1 + cx2) / 2 - 10, cy, text=disp, fill=palette["dash_text"], font=self.fonts["tree"])

                arrow = ""
                color = palette["dash_muted"]
                if v is not None and prev_val is not None:
                    try:
                        if float(v) > float(prev_val) + 1e-12:
                            arrow = "▲"
                            color = palette["dash_green"]
                        elif float(v) < float(prev_val) - 1e-12:
                            arrow = "▼"
                            color = palette["dash_red"]
                    except Exception:
                        arrow = ""
                if arrow:
                    c.create_text(cx2 - 24, cy, text=arrow, fill=color, font=self.fonts["label"])
                if v is not None:
                    prev_val = v

        c.configure(scrollregion=(0, 0, total_w, total_h))


class _DashboardCardsStrip(tk.Frame):
    def __init__(self, parent: tk.Misc, *, palette: Dict[str, str], fonts: Dict[str, Any], lang: str = "ar"):
        super().__init__(parent, bg=palette["dash_bg"])
        self.palette = palette
        self.fonts = fonts
        self.lang = lang
        self._labels: List[tuple[tk.Label, tk.Label, tk.Label]] = []

        for _ in range(6):
            f = tk.Frame(self, bg=palette["dash_panel"], highlightthickness=1, highlightbackground=palette["dash_border"], bd=0)
            f.pack(side="left", padx=8, pady=6, fill="both", expand=True)
            t = tk.Label(f, text="—", bg=palette["dash_panel"], fg=palette["dash_muted"], font=fonts["caption"], anchor="w")
            t.pack(fill="x", padx=10, pady=(8, 0))
            v = tk.Label(f, text="—", bg=palette["dash_panel"], fg=palette["dash_text"], font=fonts["title"], anchor="w")
            v.pack(fill="x", padx=10, pady=(2, 0))
            s = tk.Label(f, text="—", bg=palette["dash_panel"], fg=palette["dash_muted"], font=fonts["subtitle"], anchor="w")
            s.pack(fill="x", padx=10, pady=(0, 8))
            self._labels.append((t, v, s))

    def update_cards(self, payload: Dict[str, Optional[float]]) -> None:
        # payload keys: cagr, assets, equity, current_ratio, net_margin, fcf
        def _fmt_pct(v: Optional[float]) -> str:
            return "—" if v is None else f"{v*100:.1f}%"

        def _fmt_money(v: Optional[float]) -> str:
            if v is None:
                return "—"
            # Assume "million USD" in UI context; display as B when large.
            return f"{v/1000.0:,.1f}B" if abs(v) >= 1000 else f"{v:,.1f}M"

        def _fmt_num(v: Optional[float]) -> str:
            return "—" if v is None else f"{v:,.2f}"

        cards = [
            ("معدل النمو السنوي (CAGR)", _fmt_pct(payload.get("cagr")), "آخر 5 سنوات"),
            ("إجمالي الأصول", _fmt_money(payload.get("assets")), "آخر سنة مالية"),
            ("إجمالي حقوق الملكية", _fmt_money(payload.get("equity")), "آخر سنة مالية"),
            ("نسبة السيولة الجارية", _fmt_num(payload.get("current_ratio")), "آخر سنة مالية"),
            ("هامش الربح الصافي", _fmt_pct(payload.get("net_margin")), "آخر سنة مالية"),
            ("التدفق النقدي الحر", _fmt_money(payload.get("fcf")), "آخر سنة مالية"),
        ]
        for (t, v, s), (lt, lv, ls) in zip(cards, self._labels):
            lt.config(text=t)
            lv.config(text=v)
            ls.config(text=s)


def default_formatters() -> Dict[str, Callable[[Optional[float]], str]]:
    def _fmt_pct(v: Optional[float]) -> str:
        return "—" if v is None else f"{v*100:.2f}%"

    def _fmt_num(v: Optional[float]) -> str:
        return "—" if v is None else f"{v:,.2f}"

    def _fmt_money_m(v: Optional[float]) -> str:
        return "—" if v is None else f"{v:,.2f}"

    return {"pct": _fmt_pct, "num": _fmt_num, "money_m": _fmt_money_m}


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        fv = float(v)
        if fv != fv or math.isinf(fv):
            return None
        return fv
    except Exception:
        return None
