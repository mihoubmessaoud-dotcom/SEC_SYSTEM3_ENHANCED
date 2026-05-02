from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageFilter


def _glyph_only(im: Image.Image) -> Image.Image:
    """
    Given a full button screenshot, extract the right-side glyph region and
    return a glyph-only RGBA image with transparent background.
    """
    im = im.convert("RGBA")
    w, h = im.size

    # Crop the right portion where the icon lives (tuned for the provided button crops).
    crop_w = max(120, int(w * 0.38))
    x0 = max(0, w - crop_w)
    icon = im.crop((x0, 0, w, h))

    # Focus vertically on the central area.
    icon = icon.crop((0, int(h * 0.05), icon.size[0], int(h * 0.95)))

    rgb = icon.convert("RGB")
    px = rgb.load()
    w2, h2 = icon.size

    # Estimate background as median of dark pixels.
    samples = []
    for x in range(0, w2, 2):
        for y in (0, 1, h2 - 2, h2 - 1):
            r, g, b = px[x, y]
            mx, mn = max(r, g, b), min(r, g, b)
            sat = 0 if mx == 0 else (mx - mn) / mx
            if mx < 90 and sat < 0.25:
                samples.append((r, g, b))
    for y in range(0, h2, 2):
        for x in (0, 1, w2 - 2, w2 - 1):
            r, g, b = px[x, y]
            mx, mn = max(r, g, b), min(r, g, b)
            sat = 0 if mx == 0 else (mx - mn) / mx
            if mx < 90 and sat < 0.25:
                samples.append((r, g, b))
    if not samples:
        samples = [(10, 10, 10)]
    rs = sorted(s[0] for s in samples)
    gs = sorted(s[1] for s in samples)
    bs = sorted(s[2] for s in samples)
    bg = (rs[len(rs) // 2], gs[len(gs) // 2], bs[len(bs) // 2])

    def dist(c1, c2) -> int:
        return abs(c1[0] - c2[0]) + abs(c1[1] - c2[1]) + abs(c1[2] - c2[2])

    # Build glyph mask: keep bright or saturated or far-from-bg pixels.
    mask = Image.new("L", (w2, h2), 0)
    mpx = mask.load()
    for y in range(h2):
        for x in range(w2):
            r, g, b = px[x, y]
            mx, mn = max(r, g, b), min(r, g, b)
            sat = 0 if mx == 0 else (mx - mn) / mx
            d = dist((r, g, b), bg)
            if mx >= 200 or sat >= 0.30 or (d >= 160 and mx >= 120):
                mpx[x, y] = 255

    mask = mask.filter(ImageFilter.MaxFilter(size=3))

    icon_px = icon.load()
    out = Image.new("RGBA", (w2, h2), (0, 0, 0, 0))
    out_px = out.load()
    for y in range(h2):
        for x in range(w2):
            if mpx[x, y]:
                out_px[x, y] = icon_px[x, y]

    # Keep largest connected component to drop stray decorations.
    alpha = out.split()[-1]
    apx = alpha.load()
    visited = set()
    best = []

    def neigh(xx: int, yy: int):
        for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            nx, ny = xx + dx, yy + dy
            if 0 <= nx < w2 and 0 <= ny < h2:
                yield nx, ny

    for yy in range(h2):
        for xx in range(w2):
            if apx[xx, yy] == 0 or (xx, yy) in visited:
                continue
            stack = [(xx, yy)]
            visited.add((xx, yy))
            comp = []
            while stack:
                cx, cy = stack.pop()
                comp.append((cx, cy))
                for nx, ny in neigh(cx, cy):
                    if apx[nx, ny] != 0 and (nx, ny) not in visited:
                        visited.add((nx, ny))
                        stack.append((nx, ny))
            if len(comp) > len(best):
                best = comp

    if best:
        clean = Image.new("RGBA", (w2, h2), (0, 0, 0, 0))
        cpx = clean.load()
        for xx, yy in best:
            cpx[xx, yy] = out_px[xx, yy]
        out = clean

    # Tight crop + square pad.
    bbox = out.split()[-1].getbbox()
    if bbox:
        l, t, r, b = bbox
        pad = 6
        l = max(0, l - pad)
        t = max(0, t - pad)
        r = min(out.size[0], r + pad)
        b = min(out.size[1], b + pad)
        out = out.crop((l, t, r, b))
    side = max(out.size)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    canvas.alpha_composite(out, ((side - out.size[0]) // 2, (side - out.size[1]) // 2))
    return canvas


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-dir", required=True, help="Folder containing button PNGs")
    ap.add_argument("--out-dir", default="assets/ui/nav_icons_master", help="Output folder")
    ap.add_argument("--size", type=int, default=56, help="Final square size")
    args = ap.parse_args()

    src_dir = Path(args.src_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Expected filenames in src-dir -> output name (glyph-only)
    mapping = {
        "btn_home.png": "home.png",
        "btn_ai.png": "snowflake.png",
        "btn_smart.png": "chart.png",
        "btn_financials.png": "bars_purple.png",
        "btn_metrics.png": "percent.png",
        "btn_strategic.png": "target.png",
        "btn_forecast.png": "orb.png",
        "btn_comparison.png": "scales.png",
        "btn_reports.png": "doc.png",
    }

    for src_name, out_name in mapping.items():
        p = src_dir / src_name
        if not p.exists():
            continue
        im = Image.open(p)
        glyph = _glyph_only(im)
        glyph = glyph.resize((args.size, args.size), Image.LANCZOS)
        glyph.save(out_dir / out_name)
        print(f"wrote {out_dir/out_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

