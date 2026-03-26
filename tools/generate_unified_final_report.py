#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from financial_analyzer.core.pipeline_orchestrator import PipelineOrchestrator


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_ticker_from_name(name: str) -> str:
    m = re.match(r"^([A-Za-z0-9]+)_analysis", name)
    if not m:
        return ""
    return m.group(1).upper()


def _collect_latest_files() -> dict:
    candidates = []
    roots = [
        ROOT / "exports" / "manual_exports",
        Path(r"c:\Users\user\OneDrive\Bureau\MS PROD\test 3\TEQST"),
    ]
    for folder in roots:
        if not folder.exists():
            continue
        candidates.extend(folder.glob("*_analysis_*.xlsx"))

    latest_by_ticker = {}
    for p in candidates:
        ticker = _extract_ticker_from_name(p.name)
        if not ticker:
            continue
        prev = latest_by_ticker.get(ticker)
        if prev is None or p.stat().st_mtime > prev.stat().st_mtime:
            latest_by_ticker[ticker] = p
    return latest_by_ticker


def _run_pipeline(files_by_ticker: dict) -> list:
    orch = PipelineOrchestrator()
    cache = {}
    rows = []

    for ticker in sorted(files_by_ticker.keys()):
        f = files_by_ticker[ticker]
        try:
            r = orch.run(str(f), cache)
            cache[r.ticker] = r
            latest = max(r.valid_years) if r.valid_years else None
            latest_verdict = r.verdicts.get(latest, {}).get("verdict") if latest is not None else None

            na_with_reason = 0
            na_without_reason = 0
            blocked_by_sector = 0
            for y in r.valid_years:
                for _, rv in (r.ratios.get(y, {}) or {}).items():
                    if hasattr(rv, "value") and rv.value is None:
                        reason = str(getattr(rv, "reason", "") or "")
                        if reason:
                            na_with_reason += 1
                        else:
                            na_without_reason += 1
                        if reason == "BLOCKED_BY_SECTOR":
                            blocked_by_sector += 1

            rows.append(
                {
                    "ticker": r.ticker,
                    "source_file": str(f),
                    "sub_sector": r.sub_sector,
                    "status": r.status,
                    "valid_years": r.valid_years,
                    "blocked_years": r.blocked_years,
                    "quality_score": r.quality_score,
                    "professional_score": r.professional_score,
                    "latest_year": latest,
                    "latest_verdict": latest_verdict,
                    "na_with_reason": na_with_reason,
                    "na_without_reason": na_without_reason,
                    "blocked_by_sector_count": blocked_by_sector,
                    "audit_summary": r.audit.summary() if r.audit else {},
                }
            )
        except Exception as e:
            rows.append(
                {
                    "ticker": ticker,
                    "source_file": str(f),
                    "status": "CRASH",
                    "error": str(e),
                }
            )
    return rows


def _run_tests() -> dict:
    test_path = str(ROOT / "financial_analyzer" / "tests")
    code = pytest.main([test_path, "-q", "--tb=short"])
    return {"pytest_exit_code": int(code), "pytest_passed": int(code) == 0}


def _build_summary(results: list, test_meta: dict) -> dict:
    ok = [r for r in results if r.get("status") == "OK"]
    crashes = [r for r in results if r.get("status") == "CRASH"]
    verdict_counter = Counter((r.get("latest_verdict") or "N/A") for r in ok)
    sub_sector_counter = Counter((r.get("sub_sector") or "unknown") for r in ok)

    avg_quality = round(sum(float(r.get("quality_score", 0) or 0) for r in ok) / max(1, len(ok)), 2)
    avg_prof = round(sum(float(r.get("professional_score", 0) or 0) for r in ok) / max(1, len(ok)), 2)
    total_na_without_reason = sum(int(r.get("na_without_reason", 0) or 0) for r in ok)
    total_blocked_by_sector = sum(int(r.get("blocked_by_sector_count", 0) or 0) for r in ok)

    risk_flags = defaultdict(list)
    for r in ok:
        if (r.get("latest_verdict") or "").upper() == "FAIL":
            risk_flags["fail_verdicts"].append(r["ticker"])
        if float(r.get("quality_score", 0) or 0) < 50:
            risk_flags["quality_below_50"].append(r["ticker"])
        if int(r.get("na_without_reason", 0) or 0) > 0:
            risk_flags["silent_na"].append(r["ticker"])
        if len(r.get("blocked_years") or []) > 0:
            risk_flags["has_blocked_years"].append(r["ticker"])

    return {
        "generated_at_utc": _now_utc(),
        "companies_total": len(results),
        "companies_ok": len(ok),
        "companies_crash": len(crashes),
        "avg_quality_score": avg_quality,
        "avg_professional_score": avg_prof,
        "verdict_distribution": dict(verdict_counter),
        "sub_sector_distribution": dict(sub_sector_counter),
        "total_na_without_reason": total_na_without_reason,
        "total_blocked_by_sector": total_blocked_by_sector,
        "pytest": test_meta,
        "risk_flags": {k: v for k, v in risk_flags.items()},
        "gate_final": (
            "PASS"
            if (len(crashes) == 0 and total_na_without_reason == 0 and test_meta.get("pytest_passed"))
            else "REVIEW"
        ),
    }


def _write_markdown(path: Path, summary: dict, results: list):
    lines = []
    lines.append("# Unified Final Report")
    lines.append("")
    lines.append(f"- Generated (UTC): {summary['generated_at_utc']}")
    lines.append(f"- Companies: {summary['companies_ok']}/{summary['companies_total']} OK")
    lines.append(f"- Final Gate: **{summary['gate_final']}**")
    lines.append(f"- pytest passed: {summary['pytest'].get('pytest_passed')}")
    lines.append(f"- Avg Quality Score: {summary['avg_quality_score']}")
    lines.append(f"- Avg Professional Score: {summary['avg_professional_score']}")
    lines.append(f"- Silent N/A count: {summary['total_na_without_reason']}")
    lines.append("")
    lines.append("## Verdict Distribution")
    for k, v in (summary.get("verdict_distribution") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Company Details")
    lines.append("")
    lines.append("| Ticker | Sub-Sector | Status | Latest Verdict | Quality | Professional | Valid Years | Blocked Years | Silent N/A |")
    lines.append("|---|---|---|---|---:|---:|---:|---:|---:|")
    for r in sorted(results, key=lambda x: x.get("ticker", "")):
        lines.append(
            "| {ticker} | {sub} | {status} | {verdict} | {q} | {p} | {vy} | {by} | {na} |".format(
                ticker=r.get("ticker", "N/A"),
                sub=r.get("sub_sector", "N/A"),
                status=r.get("status", "N/A"),
                verdict=r.get("latest_verdict", "N/A"),
                q=r.get("quality_score", "N/A"),
                p=r.get("professional_score", "N/A"),
                vy=len(r.get("valid_years") or []),
                by=len(r.get("blocked_years") or []),
                na=r.get("na_without_reason", 0),
            )
        )
    lines.append("")
    lines.append("## Risk Flags")
    flags = summary.get("risk_flags") or {}
    if not flags:
        lines.append("- None")
    else:
        for k, v in flags.items():
            lines.append(f"- {k}: {', '.join(v)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    out_dir = ROOT / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"unified_final_report_{ts}.json"
    out_md = out_dir / f"unified_final_report_{ts}.md"

    files_by_ticker = _collect_latest_files()
    results = _run_pipeline(files_by_ticker)
    test_meta = _run_tests()
    summary = _build_summary(results, test_meta)
    payload = {"summary": summary, "results": results}

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(out_md, summary, results)

    print(f"outputs/{out_json.name}")
    print(f"outputs/{out_md.name}")
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
