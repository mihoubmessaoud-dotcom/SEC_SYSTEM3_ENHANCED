#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def evaluate(report: dict, min_light_pct: float, min_deep_pct: float, max_deep_balance_fail: int):
    summary = report.get("summary") or {}
    light_pct = float(summary.get("light_coverage_success_pct") or 0.0)
    deep_pct = float(summary.get("deep_coverage_success_pct") or 0.0)
    deep_balance_fail = int(summary.get("deep_balance_fail") or 0)

    checks = {
        "light_coverage": {
            "value": light_pct,
            "threshold": min_light_pct,
            "pass": light_pct >= min_light_pct,
        },
        "deep_coverage": {
            "value": deep_pct,
            "threshold": min_deep_pct,
            "pass": deep_pct >= min_deep_pct,
        },
        "deep_balance_fail": {
            "value": deep_balance_fail,
            "threshold": max_deep_balance_fail,
            "pass": deep_balance_fail <= max_deep_balance_fail,
        },
    }

    overall_pass = all(v["pass"] for v in checks.values())
    return {
        "generated_at_utc": _now_utc(),
        "status": "PASS" if overall_pass else "FAIL",
        "checks": checks,
        "top_not_computable_reasons": summary.get("top_not_computable_reasons") or [],
        "notes": [
            "PASS means platform is deployment-ready under configured thresholds.",
            "Top NOT_COMPUTABLE reasons are optimization opportunities, not immediate blockers unless policy requires.",
        ],
    }


def main():
    p = argparse.ArgumentParser(description="Commercial readiness gate from mass validation report")
    p.add_argument("--report", required=True, help="Path to mass_validation_100plus_*.json")
    p.add_argument("--out", default=None, help="Output JSON path")
    p.add_argument("--min-light-pct", type=float, default=98.0)
    p.add_argument("--min-deep-pct", type=float, default=95.0)
    p.add_argument("--max-deep-balance-fail", type=int, default=0)
    args = p.parse_args()

    report_path = Path(args.report)
    with report_path.open("r", encoding="utf-8") as f:
        report = json.load(f)

    gate = evaluate(
        report=report,
        min_light_pct=args.min_light_pct,
        min_deep_pct=args.min_deep_pct,
        max_deep_balance_fail=args.max_deep_balance_fail,
    )

    out_path = Path(args.out) if args.out else report_path.with_name(
        f"commercial_readiness_gate_{report_path.stem}.json"
    )
    out_path.write_text(json.dumps(gate, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out_path))
    print(json.dumps(gate, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

