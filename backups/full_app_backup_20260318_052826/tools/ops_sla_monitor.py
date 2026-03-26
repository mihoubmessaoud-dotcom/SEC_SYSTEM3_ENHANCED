#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _percentile(values, p):
    if not values:
        return None
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def _latest_batch_files(outputs_dir: Path, limit: int):
    files = sorted(outputs_dir.glob("institutional_batch_gate_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[: max(1, int(limit or 1))]


def build_sla_report(batch_payloads):
    results = []
    for payload in batch_payloads:
        results.extend(payload.get("results") or [])

    count = len(results)
    statuses = {"PASS", "PASS_WITH_WARNING", "REVIEW", "FAIL"}
    by_status = {s: 0 for s in statuses}
    elapsed = []
    for r in results:
        st = str(r.get("status") or "").upper()
        if st in by_status:
            by_status[st] += 1
        e = r.get("elapsed_sec")
        if isinstance(e, (int, float)):
            elapsed.append(float(e))

    success = by_status["PASS"] + by_status["PASS_WITH_WARNING"]
    success_rate = round((success / count) * 100.0, 2) if count else 0.0
    p50 = _percentile(elapsed, 50)
    p95 = _percentile(elapsed, 95)
    p99 = _percentile(elapsed, 99)

    return {
        "generated_at_utc": _now_utc(),
        "samples": count,
        "status_counts": by_status,
        "success_rate_pct": success_rate,
        "latency_sec": {
            "p50": round(p50, 2) if p50 is not None else None,
            "p95": round(p95, 2) if p95 is not None else None,
            "p99": round(p99, 2) if p99 is not None else None,
            "max": round(max(elapsed), 2) if elapsed else None
        }
    }


def main():
    ap = argparse.ArgumentParser(description="Build operational SLA report from institutional batch outputs.")
    ap.add_argument("--outputs-dir", default="outputs")
    ap.add_argument("--latest-n", type=int, default=10)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    outputs_dir = Path(args.outputs_dir)
    files = _latest_batch_files(outputs_dir, args.latest_n)
    payloads = []
    for f in files:
        try:
            payloads.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            continue

    report = build_sla_report(payloads)
    out_path = Path(args.out) if args.out else outputs_dir / f"ops_sla_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out_path))
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()

