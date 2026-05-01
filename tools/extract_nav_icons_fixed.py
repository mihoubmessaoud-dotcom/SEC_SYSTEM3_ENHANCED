from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from PIL import Image, ImageDraw


@dataclass(frozen=True)
class CropBox:
    name: str
    box: tuple[int, int, int, int]  # (left, top, right, bottom)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _find_sidebar_x0(img: Image.Image) -> int:
    """
    Heuristic: find the x where the right-side nav rail starts by scanning
    for a strong vertical edge in the right half of the image.
    """
    w, h = img.size
    px = img.convert("RGB").load()
    scan_y0, scan_y1 = int(h * 0.15), int(h * 0.85)
    best_x, best_score = int(w * 0.75), -1.0
    for x in range(int(w * 0.65), int(w * 0.95)):
        # Edge score: avg absolute diff between column x-1 and x.
        if x <= 0:
            continue
        s = 0.0
        n = 0
        for y in range(scan_y0, scan_y1, 3):
            r1, g1, b1 = px[x - 1, y]
            r2, g2, b2 = px[x, y]
            s += abs(r2 - r1) + abs(g2 - g1) + abs(b2 - b1)
            n += 1
        score = s / max(1, n)
        if score > best_score:
            best_score = score
            best_x = x
    return best_x


def _auto_icon_boxes(img: Image.Image, sidebar_x0: int) -> list[CropBox]:
    """
    Detect icon clusters inside the right sidebar by looking for high-saturation
    pixels and clustering them by y position.

    Returns a list of (y_center, bbox) candidates. We then map them to names in
    fixed order (home, ai, smart, financials, metrics_pct, strategic, forecasts,
    comparison, reports, fav_star, fav_heart).
    """
    w, h = img.size
    rgb = img.convert("RGB")
    px = rgb.load()

    # Sidebar icon strip region: only the far-right area where the icons are.
    x0 = w - 120
    x1 = w - 10
    y0 = int(h * 0.02)
    y1 = int(h * 0.94)

    def is_icon_px(x: int, y: int) -> bool:
        r, g, b = px[x, y]
        mx = max(r, g, b)
        mn = min(r, g, b)
        sat = 0 if mx == 0 else (mx - mn) / mx
        # Colorful glyphs (high saturation) OR bright grayscale glyphs (e.g., snowflake).
        return (mx > 110 and sat > 0.30) or (mx > 120 and (sat > 0.02 or (mx - mn) < 35))

    # Build a vertical "activity" profile to find icon rows robustly (even for white glyphs).
    profile = [0] * h
    for y in range(y0, y1):
        c = 0
        for x in range(x0, x1, 2):
            if is_icon_px(x, y):
                c += 1
        profile[y] = c

    # Smooth with a small window.
    win = 9
    half = win // 2
    smooth = [0] * h
    for y in range(y0, y1):
        s = 0
        n = 0
        for yy in range(max(y0, y - half), min(y1, y + half + 1)):
            s += profile[yy]
            n += 1
        smooth[y] = s / max(1, n)

    # Find peaks.
    raw_peaks: list[int] = []
    thresh = max(2.0, max(smooth[y0:y1]) * 0.10)
    for y in range(y0 + 1, y1 - 1):
        if smooth[y] >= thresh and smooth[y] >= smooth[y - 1] and smooth[y] >= smooth[y + 1]:
            raw_peaks.append(y)

    # Merge close peaks.
    peaks: list[int] = []
    for y in raw_peaks:
        if not peaks or y - peaks[-1] > 18:
            peaks.append(y)
        else:
            # keep the higher one
            if smooth[y] > smooth[peaks[-1]]:
                peaks[-1] = y

    peaks.sort()

    # Keep the 11 peaks closest to the expected sidebar block if there are extras.
    expected = 11
    if len(peaks) > expected:
        # Select the expected count by taking the highest peaks (by smooth value), then sort by y.
        peaks = sorted(peaks, key=lambda yy: smooth[yy], reverse=True)[:expected]
        peaks.sort()

    if len(peaks) < expected:
        # Best effort: still proceed, but the caller will see fewer icons.
        pass

    # Build bboxes around each peak.
    candidates: list[tuple[int, tuple[int, int, int, int]]] = []
    for yc in peaks:
        ry0 = max(y0, yc - 36)
        ry1 = min(y1, yc + 36)
        pts: list[tuple[int, int]] = []
        for y in range(ry0, ry1, 2):
            for x in range(x0, x1, 2):
                if is_icon_px(x, y):
                    pts.append((x, y))
        if not pts:
            continue
        xs = sorted(p[0] for p in pts)
        ys = sorted(p[1] for p in pts)

        def q(vs: list[int], pct: float) -> int:
            idx = int(round((len(vs) - 1) * pct))
            idx = max(0, min(len(vs) - 1, idx))
            return vs[idx]

        left, right = q(xs, 0.04), q(xs, 0.96)
        top, bottom = q(ys, 0.04), q(ys, 0.96)
        candidates.append(((top + bottom) // 2, (left, top, right, bottom)))

    candidates.sort(key=lambda t: t[0])

    # Map to names by expected order from top to bottom.
    names = [
        "home",
        "ai",
        "smart",
        "financials",
        "metrics_pct",
        "strategic",
        "forecasts",
        "comparison",
        "reports",
        "fav_star",
        "fav_heart",
    ]

    # Trim/pad to expected count.
    if len(candidates) > len(names):
        candidates = candidates[: len(names)]

    boxes: list[CropBox] = []
    for i, (yc, (l, t, r, b)) in enumerate(candidates[: len(names)]):
        # Expand with padding and normalize to square-ish crop.
        pad = 10
        l2 = max(0, l - pad)
        t2 = max(0, t - pad)
        r2 = min(w, r + pad)
        b2 = min(h, b + pad)
        # Square
        cw, ch = r2 - l2, b2 - t2
        side = max(cw, ch)
        cx = (l2 + r2) // 2
        cy = (t2 + b2) // 2
        l3 = max(0, cx - side // 2)
        t3 = max(0, cy - side // 2)
        r3 = min(w, l3 + side)
        b3 = min(h, t3 + side)
        l3 = max(0, r3 - side)
        t3 = max(0, b3 - side)
        boxes.append(CropBox(names[i], (l3, t3, r3, b3)))
    return boxes


def _render_debug(img: Image.Image, sidebar_x0: int, boxes: Iterable[CropBox], out_path: Path) -> None:
    dbg = img.copy().convert("RGBA")
    d = ImageDraw.Draw(dbg)
    w, h = img.size
    d.rectangle((sidebar_x0, 0, w - 1, h - 1), outline=(0, 255, 255, 180), width=3)
    for b in boxes:
        l, t, r, bt = b.box
        d.rectangle((l, t, r, bt), outline=(255, 0, 255, 200), width=3)
        d.text((l + 3, t + 3), b.name, fill=(255, 255, 255, 220))
    dbg.save(out_path)


def _make_bg_transparent(icon_rgba: Image.Image) -> Image.Image:
    """
    Turn the (nearly-uniform) dark icon-chip background transparent while keeping
    the glyph pixels intact. This is intentionally conservative to avoid cutting
    thin strokes.
    """
    im = icon_rgba.convert("RGBA")
    w, h = im.size
    px = im.load()

    # Estimate background color from border pixels with low saturation.
    samples = []
    for x in range(w):
        for y in (0, 1, h - 2, h - 1):
            r, g, b, a = px[x, y]
            mx, mn = max(r, g, b), min(r, g, b)
            sat = 0 if mx == 0 else (mx - mn) / mx
            if a > 0 and mx < 120 and sat < 0.25:
                samples.append((r, g, b))
    for y in range(h):
        for x in (0, 1, w - 2, w - 1):
            r, g, b, a = px[x, y]
            mx, mn = max(r, g, b), min(r, g, b)
            sat = 0 if mx == 0 else (mx - mn) / mx
            if a > 0 and mx < 120 and sat < 0.25:
                samples.append((r, g, b))

    if not samples:
        return im

    # Median background color.
    rs = sorted(s[0] for s in samples)
    gs = sorted(s[1] for s in samples)
    bs = sorted(s[2] for s in samples)
    bg = (rs[len(rs) // 2], gs[len(gs) // 2], bs[len(bs) // 2])

    def dist(c1, c2) -> int:
        return abs(c1[0] - c2[0]) + abs(c1[1] - c2[1]) + abs(c1[2] - c2[2])

    # Build a glyph mask: keep only pixels that are clearly "icon glyph" (bright or saturated),
    # which avoids bringing along button borders and background chips.
    mask = Image.new("L", (w, h), 0)
    mpx = mask.load()
    for y in range(h):
        for x in range(w):
            r, g, b, a = px[x, y]
            if a == 0:
                continue
            mx, mn = max(r, g, b), min(r, g, b)
            sat = 0 if mx == 0 else (mx - mn) / mx
            d = dist((r, g, b), bg)
            # glyph pixels: either bright (white-ish), or reasonably saturated, or very far from bg.
            if mx >= 210 or (mx >= 120 and sat >= 0.22) or (d >= 180 and mx >= 110):
                mpx[x, y] = 255

    # Keep only the right side of the crop where the glyph is (prevents capturing sidebar text/selection strip).
    cut_x = int(w * 0.45)
    for y in range(h):
        for x in range(0, cut_x):
            mpx[x, y] = 0

    # Slight dilation to keep thin edges.
    try:
        from PIL import ImageFilter

        mask = mask.filter(ImageFilter.MaxFilter(size=3))
    except Exception:
        pass

    out = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    out_px = out.load()
    mpx = mask.load()
    for y in range(h):
        for x in range(w):
            if mpx[x, y] == 0:
                continue
            r, g, b, a = px[x, y]
            out_px[x, y] = (r, g, b, a)
    return out


def _tighten_alpha_to_square(im: Image.Image, pad: int = 6) -> Image.Image:
    im = im.convert("RGBA")
    alpha = im.split()[-1]
    bbox = alpha.getbbox()
    if not bbox:
        return im
    l, t, r, b = bbox
    l = max(0, l - pad)
    t = max(0, t - pad)
    r = min(im.size[0], r + pad)
    b = min(im.size[1], b + pad)
    cropped = im.crop((l, t, r, b))
    cw, ch = cropped.size
    side = max(cw, ch)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    canvas.alpha_composite(cropped, ((side - cw) // 2, (side - ch) // 2))
    return canvas


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", required=False, help="Path to the reference PNG")
    ap.add_argument(
        "--out-dir",
        default=str(Path("assets/ui/nav_icons")),
        help="Output directory for icons",
    )
    ap.add_argument("--size", type=int, default=56, help="Square icon size (px)")
    ap.add_argument(
        "--keep-bg",
        action="store_true",
        help="Keep icon chip background (disable transparency keying).",
    )
    ap.add_argument(
        "--write-debug",
        action="store_true",
        help="Write outputs/nav_icons_debug.png with detected boxes",
    )
    args = ap.parse_args()

    ref = args.ref or os.environ.get("UI_REFERENCE_PNG")
    if not ref:
        raise SystemExit("Missing --ref or UI_REFERENCE_PNG")

    img = Image.open(ref).convert("RGBA")
    sidebar_x0 = _find_sidebar_x0(img)
    boxes = _auto_icon_boxes(img, sidebar_x0)
    if not boxes:
        raise SystemExit("Could not detect icon boxes (no candidates found).")

    out_dir = Path(args.out_dir)
    _ensure_dir(out_dir)

    # Crop at native size, optionally key out dark bg, then resize.
    for b in boxes:
        crop = img.crop(b.box)
        if not args.keep_bg:
            crop = _make_bg_transparent(crop)
            crop = _tighten_alpha_to_square(crop, pad=6)
        crop = crop.resize((args.size, args.size), Image.LANCZOS)
        crop.save(out_dir / f"{b.name}.png")

    if args.write_debug:
        dbg_path = Path("outputs/nav_icons_debug.png")
        _ensure_dir(dbg_path.parent)
        _render_debug(img, sidebar_x0, boxes, dbg_path)

    print(f"sidebar_x0={sidebar_x0} boxes={len(boxes)} written={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
