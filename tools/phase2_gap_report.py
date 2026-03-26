#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_load_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_campaign_file(outputs_dir: Path) -> Path:
    files = sorted(outputs_dir.glob("institutional_campaign_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError("No institutional_campaign_*.json found in outputs/")
    return files[0]


def _ratio_to_canonical_dependencies(ratio_id: str) -> List[str]:
    # Heuristic dependency map used only for prioritization in gap report.
    m = {
        "gross_margin": ["annual_revenue", "annual_cogs"],
        "operating_margin": ["annual_revenue"],
        "net_margin": ["annual_revenue"],
        "roe": ["total_equity"],
        "roa": ["total_assets"],
        "days_sales_outstanding": ["annual_revenue", "accounts_receivable"],
        "ap_days": ["annual_cogs", "accounts_payable"],
        "inventory_days": ["annual_cogs", "inventory"],
        "ccc_days": ["annual_revenue", "annual_cogs", "accounts_receivable", "accounts_payable", "inventory"],
    }
    return m.get(str(ratio_id or "").strip().lower(), [])


def build_gap_report(campaign: Dict, source_path: Path) -> Dict:
    results = list(campaign.get("all_results") or campaign.get("results") or [])
    if not results and isinstance(campaign.get("chunks"), list):
        parent = source_path.parent
        merged = []
        for ch in campaign.get("chunks") or []:
            jp = ch.get("json")
            if not jp:
                continue
            p = Path(str(jp))
            if not p.is_absolute():
                p = parent / p.name if (parent / p.name).exists() else parent / str(jp)
            if not p.exists():
                continue
            try:
                batch_payload = _safe_load_json(p)
                merged.extend(list(batch_payload.get("results") or []))
            except Exception:
                continue
        results = merged
    by_sector = defaultdict(
        lambda: {
            "count": 0,
            "status": Counter(),
            "missing_keys": Counter(),
            "top_reasons": Counter(),
            "gate_warnings": Counter(),
        }
    )
    global_missing = Counter()
    global_reasons = Counter()
    global_warnings = Counter()

    for row in results:
        sector = str(row.get("sector_profile") or "unknown").lower()
        st = str(row.get("status") or "UNKNOWN").upper()
        crh = row.get("core_ratio_health") or {}
        missing_keys = list(crh.get("missing_keys") or [])
        top_reasons = list(crh.get("top_reasons") or [])
        gate_warnings = list(row.get("gate_warnings") or [])

        d = by_sector[sector]
        d["count"] += 1
        d["status"][st] += 1

        for k in missing_keys:
            kk = str(k)
            d["missing_keys"][kk] += 1
            global_missing[kk] += 1
        for pair in top_reasons:
            if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                reason = str(pair[0])
                count = int(pair[1] or 0)
            else:
                reason = str(pair)
                count = 1
            d["top_reasons"][reason] += count
            global_reasons[reason] += count
        for w in gate_warnings:
            ww = str(w)
            d["gate_warnings"][ww] += 1
            global_warnings[ww] += 1

    prioritized_missing = []
    for k, c in global_missing.most_common():
        prioritized_missing.append(
            {
                "ratio_id": k,
                "missing_count": c,
                "canonical_dependencies": _ratio_to_canonical_dependencies(k),
            }
        )

    sector_payload = {}
    for sec, d in sorted(by_sector.items(), key=lambda kv: kv[0]):
        sector_payload[sec] = {
            "count": d["count"],
            "status": dict(d["status"]),
            "missing_keys_top": d["missing_keys"].most_common(20),
            "top_reasons": d["top_reasons"].most_common(20),
            "gate_warnings": d["gate_warnings"].most_common(20),
        }

    return {
        "generated_at_utc": _now_utc(),
        "source_campaign_file": str(source_path),
        "source_summary": campaign.get("summary") or {},
        "results_count": len(results),
        "global": {
            "missing_keys_top": global_missing.most_common(30),
            "reasons_top": global_reasons.most_common(30),
            "gate_warnings_top": global_warnings.most_common(30),
            "prioritized_missing_for_phase2_1": prioritized_missing[:30],
        },
        "by_sector": sector_payload,
    }


def write_markdown(report: Dict, out_md: Path) -> None:
    lines = []
    lines.append("# Phase 2.2 Gap Report")
    lines.append("")
    lines.append(f"- Generated: {report.get('generated_at_utc')}")
    lines.append(f"- Source: `{report.get('source_campaign_file')}`")
    lines.append(f"- Results count: {report.get('results_count')}")
    lines.append("")
    lines.append("## Global Top Missing Keys")
    for rid, cnt in report.get("global", {}).get("missing_keys_top", []):
        lines.append(f"- `{rid}`: {cnt}")
    lines.append("")
    lines.append("## Global Top Reasons")
    for reason, cnt in report.get("global", {}).get("reasons_top", []):
        lines.append(f"- `{reason}`: {cnt}")
    lines.append("")
    lines.append("## Prioritized Missing (Phase 2.1 Mapping Targets)")
    for row in report.get("global", {}).get("prioritized_missing_for_phase2_1", []):
        deps = ", ".join(row.get("canonical_dependencies") or []) or "-"
        lines.append(
            f"- `{row.get('ratio_id')}`: missing={row.get('missing_count')} | canonical deps: {deps}"
        )
    lines.append("")
    lines.append("## Sector Breakdown")
    for sec, payload in (report.get("by_sector") or {}).items():
        lines.append(f"### {sec}")
        lines.append(f"- Count: {payload.get('count')}")
        lines.append(f"- Status: {json.dumps(payload.get('status') or {}, ensure_ascii=False)}")
        mk = payload.get("missing_keys_top") or []
        if mk:
            lines.append("- Missing keys top:")
            for rid, cnt in mk[:10]:
                lines.append(f"  - `{rid}`: {cnt}")
        rs = payload.get("top_reasons") or []
        if rs:
            lines.append("- Reasons top:")
            for reason, cnt in rs[:10]:
                lines.append(f"  - `{reason}`: {cnt}")
        gw = payload.get("gate_warnings") or []
        if gw:
            lines.append("- Gate warnings:")
            for w, cnt in gw[:10]:
                lines.append(f"  - `{w}`: {cnt}")
        lines.append("")
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description="Generate Phase 2.2 canonical gap report from campaign output.")
    ap.add_argument("--campaign-json", default="", help="Path to institutional_campaign_*.json (optional).")
    ap.add_argument("--outputs-dir", default="outputs")
    args = ap.parse_args()

    outputs_dir = Path(args.outputs_dir)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    src = Path(args.campaign_json) if args.campaign_json else _latest_campaign_file(outputs_dir)
    campaign = _safe_load_json(src)

    report = build_gap_report(campaign, src)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = outputs_dir / f"phase2_gap_report_{ts}.json"
    out_md = outputs_dir / f"phase2_gap_report_{ts}.md"
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(report, out_md)

    print(str(out_json))
    print(str(out_md))


if __name__ == "__main__":
    main()
