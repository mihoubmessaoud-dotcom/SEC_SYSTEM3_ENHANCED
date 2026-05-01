# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Callable, Optional, Tuple

import tkinter as tk

from PIL import Image, ImageDraw, ImageFilter, ImageTk


Color = str  # "#rrggbb"


def _hex_to_rgb(color: Color) -> Tuple[int, int, int]:
    c = (color or "").strip()
    if not c:
        return (0, 0, 0)
    if c.startswith("#"):
        c = c[1:]
    if len(c) == 3:
        c = "".join(ch * 2 for ch in c)
    if len(c) != 6:
        return (0, 0, 0)
    return (int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16))


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _mix(a: Tuple[int, int, int], b: Tuple[int, int, int], t: float) -> Tuple[int, int, int]:
    t = _clamp(float(t), 0.0, 1.0)
    return (
        int(round(a[0] + (b[0] - a[0]) * t)),
        int(round(a[1] + (b[1] - a[1]) * t)),
        int(round(a[2] + (b[2] - a[2]) * t)),
    )


@lru_cache(maxsize=512)
def _render_button_rgba(
    width: int,
    height: int,
    *,
    fill_top: Color,
    fill_bottom: Color,
    border: Color,
    glow: Color,
    radius: int,
    glow_radius: int,
    border_width: int,
) -> Image.Image:
    w = max(1, int(width))
    h = max(1, int(height))
    r = max(0, int(radius))
    bw = max(0, int(border_width))
    gr = max(0, int(glow_radius))

    base = Image.new("RGBA", (w, h), (0, 0, 0, 0))

    # Glow layer
    if gr > 0:
        glow_layer = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        gd = ImageDraw.Draw(glow_layer)
        inset = max(1, bw)
        rect = [inset, inset, w - inset - 1, h - inset - 1]
        gcol = (*_hex_to_rgb(glow), 160)
        gd.rounded_rectangle(rect, radius=max(0, r - 1), fill=gcol)
        glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(radius=gr))
        base.alpha_composite(glow_layer)

    # Gradient fill clipped to rounded rect via a mask
    mask = Image.new("L", (w, h), 0)
    md = ImageDraw.Draw(mask)
    rect = [bw, bw, w - bw - 1, h - bw - 1]
    md.rounded_rectangle(rect, radius=r, fill=255)

    grad = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    top = _hex_to_rgb(fill_top)
    bot = _hex_to_rgb(fill_bottom)
    for y in range(h):
        t = 0.0 if h <= 1 else (y / (h - 1))
        col = _mix(top, bot, t)
        ImageDraw.Draw(grad).line([(0, y), (w, y)], fill=(*col, 255))

    # Apply gradient clipped by mask.
    base.paste(grad, (0, 0), mask)

    # Border
    if bw > 0:
        bd = ImageDraw.Draw(base)
        bcol = (*_hex_to_rgb(border), 230)
        bd.rounded_rectangle(rect, radius=r, outline=bcol, width=bw)

    return base


@lru_cache(maxsize=512)
def _render_panel_rgba(
    width: int,
    height: int,
    *,
    fill: Color,
    border: Color,
    radius: int,
    border_width: int,
) -> Image.Image:
    w = max(1, int(width))
    h = max(1, int(height))
    r = max(0, int(radius))
    bw = max(0, int(border_width))
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    rect = [bw, bw, w - bw - 1, h - bw - 1]
    d.rounded_rectangle(rect, radius=r, fill=(*_hex_to_rgb(fill), 255))
    if bw > 0:
        d.rounded_rectangle(rect, radius=r, outline=(*_hex_to_rgb(border), 255), width=bw)
    return img


def _as_photo(img: Image.Image) -> ImageTk.PhotoImage:
    return ImageTk.PhotoImage(img)


@lru_cache(maxsize=128)
def render_vertical_gradient_rgba(width: int, height: int, top: Color, bottom: Color) -> Image.Image:
    w = max(1, int(width))
    h = max(1, int(height))
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    t_rgb = _hex_to_rgb(top)
    b_rgb = _hex_to_rgb(bottom)
    d = ImageDraw.Draw(img)
    for y in range(h):
        t = 0.0 if h <= 1 else (y / (h - 1))
        col = _mix(t_rgb, b_rgb, t)
        d.line([(0, y), (w, y)], fill=(*col, 255))
    return img


@lru_cache(maxsize=128)
def render_header_bg_rgba(
    width: int,
    height: int,
    *,
    top: Color,
    mid: Color,
    bottom: Color,
    glow: Color,
    glow_height: int = 36,
) -> Image.Image:
    w = max(1, int(width))
    h = max(1, int(height))
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))

    # 2-stage gradient (top->mid, mid->bottom) to match reference feel.
    mid_y = int(h * 0.55)
    g1 = render_vertical_gradient_rgba(w, max(1, mid_y), top, mid)
    g2 = render_vertical_gradient_rgba(w, max(1, h - mid_y), mid, bottom)
    img.alpha_composite(g1, (0, 0))
    img.alpha_composite(g2, (0, mid_y))

    # Subtle glow line near the bottom edge.
    gh = max(8, int(glow_height))
    glow_layer = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(glow_layer)
    gcol = (*_hex_to_rgb(glow), 180)
    y0 = max(0, h - gh - 1)
    d.rectangle([0, y0, w, h], fill=gcol)
    glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(radius=12))
    img.alpha_composite(glow_layer)

    return img


@dataclass(frozen=True)
class GlowButtonStyle:
    fill_top: Color
    fill_bottom: Color
    border: Color
    glow: Color


class GlowButton(tk.Frame):
    """
    Premium button: rounded gradient + glow + hover/pressed states.
    Keeps commands identical to tkinter Button (no logic change).
    """

    def __init__(
        self,
        master,
        *,
        text: str,
        command: Callable[[], None],
        font=None,
        fg: Color = "#f8f8f8",
        width: int = 150,
        height: int = 44,
        radius: int = 14,
        border_width: int = 2,
        glow_radius: int = 10,
        style: GlowButtonStyle,
        hover_boost: float = 0.10,
        pressed_boost: float = -0.08,
        cursor: str = "hand2",
    ):
        super().__init__(master, width=width, height=height, bg=master.cget("bg"))
        self.pack_propagate(False)
        self._cmd = command
        self._text = text
        self._font = font
        self._fg = fg
        # NOTE: tkinter uses `self._w` for the widget pathname; never overwrite it.
        self._width_px = int(width)
        self._height_px = int(height)
        self._radius = int(radius)
        self._bw = int(border_width)
        self._gr = int(glow_radius)
        self._style = style
        self._hover_boost = float(hover_boost)
        self._pressed_boost = float(pressed_boost)

        self._label = tk.Label(self, text=text, fg=fg, bg=self.cget("bg"), font=font, compound="center")
        self._label.place(x=0, y=0, width=self._width_px, height=self._height_px)
        self._label.configure(cursor=cursor)

        self._img_normal = None
        self._img_hover = None
        self._img_pressed = None
        self._state = "normal"

        self._redraw()
        self._apply_state("normal")

        for w in (self, self._label):
            w.bind("<Enter>", self._on_enter)
            w.bind("<Leave>", self._on_leave)
            w.bind("<ButtonPress-1>", self._on_press)
            w.bind("<ButtonRelease-1>", self._on_release)

    def _boost(self, c: Color, amt: float) -> Color:
        r, g, b = _hex_to_rgb(c)
        # boost towards white for positive, towards black for negative
        if amt >= 0:
            rr, gg, bb = _mix((r, g, b), (255, 255, 255), amt)
        else:
            rr, gg, bb = _mix((r, g, b), (0, 0, 0), -amt)
        return f"#{rr:02x}{gg:02x}{bb:02x}"

    def _redraw(self):
        st = self._style
        self._img_normal = _as_photo(
            _render_button_rgba(
                self._width_px,
                self._height_px,
                fill_top=st.fill_top,
                fill_bottom=st.fill_bottom,
                border=st.border,
                glow=st.glow,
                radius=self._radius,
                glow_radius=self._gr,
                border_width=self._bw,
            )
        )
        self._img_hover = _as_photo(
            _render_button_rgba(
                self._width_px,
                self._height_px,
                fill_top=self._boost(st.fill_top, self._hover_boost),
                fill_bottom=self._boost(st.fill_bottom, self._hover_boost),
                border=self._boost(st.border, self._hover_boost),
                glow=self._boost(st.glow, self._hover_boost),
                radius=self._radius,
                glow_radius=self._gr,
                border_width=self._bw,
            )
        )
        self._img_pressed = _as_photo(
            _render_button_rgba(
                self._width_px,
                self._height_px,
                fill_top=self._boost(st.fill_top, self._pressed_boost),
                fill_bottom=self._boost(st.fill_bottom, self._pressed_boost),
                border=self._boost(st.border, self._pressed_boost),
                glow=self._boost(st.glow, self._pressed_boost),
                radius=self._radius,
                glow_radius=max(2, self._gr - 3),
                border_width=self._bw,
            )
        )

    def _apply_state(self, state: str):
        self._state = state
        if state == "pressed":
            self._label.configure(image=self._img_pressed)
        elif state == "hover":
            self._label.configure(image=self._img_hover)
        else:
            self._label.configure(image=self._img_normal)

    def _on_enter(self, _e=None):
        if self._state != "pressed":
            self._apply_state("hover")

    def _on_leave(self, _e=None):
        if self._state != "pressed":
            self._apply_state("normal")

    def _on_press(self, _e=None):
        self._apply_state("pressed")

    def _on_release(self, _e=None):
        # click only if mouse is still over the widget
        try:
            x, y = self.winfo_pointerxy()
            w = self.winfo_containing(x, y)
            if w in (self, self._label):
                self._cmd()
        finally:
            self._apply_state("hover")


class RoundedPanel(tk.Frame):
    def __init__(
        self,
        master,
        *,
        width: int,
        height: int,
        radius: int,
        fill: Color,
        border: Color,
        border_width: int = 2,
    ):
        super().__init__(master, width=width, height=height, bg=master.cget("bg"))
        self.pack_propagate(False)
        img = _render_panel_rgba(width, height, fill=fill, border=border, radius=radius, border_width=border_width)
        self._photo = _as_photo(img)
        self._label = tk.Label(self, image=self._photo, bd=0, bg=master.cget("bg"))
        self._label.place(x=0, y=0, width=width, height=height)
