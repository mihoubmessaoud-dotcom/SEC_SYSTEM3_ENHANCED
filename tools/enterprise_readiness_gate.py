#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _latest(pattern: str, base: Path):
    files = sorted(base.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No file for pattern: {pattern}")
    return files[0]


def _doc_check(required_docs):
    checks = {}
    for p in required_docs or []:
        checks[p] = Path(p).exists()
    return checks


def _quality_check(summary: dict, policy: dict):
    eff = float(summary.get("effective_pass_pct") or 0.0)
    fail = float(summary.get("fail_pct") or 0.0)
    review = float(summary.get("review_pct") or 0.0)
    return {
        "effective_pass_pct": {"value": eff, "min": float(policy.get("min_effective_pass_pct") or 0.0), "pass": eff >= float(policy.get("min_effective_pass_pct") or 0.0)},
        "fail_pct": {"value": fail, "max": float(policy.get("max_fail_pct") or 100.0), "pass": fail <= float(policy.get("max_fail_pct") or 100.0)},
        "review_pct": {"value": review, "max": float(policy.get("max_review_pct") or 100.0), "pass": review <= float(policy.get("max_review_pct") or 100.0)},
    }


def _audit_check(results: list, policy: dict):
    prov = []
    for r in results or []:
        p = (r.get("core_ratio_health") or {}).get("provenance_pct")
        if isinstance(p, (int, float)):
            prov.append(float(p))
    avg = (sum(prov) / len(prov)) if prov else 0.0
    threshold = float(policy.get("min_avg_provenance_pct") or 0.0)
    return {
        "avg_provenance_pct": {"value": round(avg, 2), "min": threshold, "pass": avg >= threshold}
    }


def _ops_check(sla: dict, policy: dict):
    p95 = ((sla.get("latency_sec") or {}).get("p95"))
    success = float(sla.get("success_rate_pct") or 0.0)
    p95_limit = float(policy.get("max_p95_elapsed_sec") or 10**9)
    success_min = float(policy.get("min_success_rate_pct") or 0.0)
    p95_ok = (isinstance(p95, (int, float)) and float(p95) <= p95_limit)
    return {
        "success_rate_pct": {"value": success, "min": success_min, "pass": success >= success_min},
        "p95_elapsed_sec": {"value": p95, "max": p95_limit, "pass": p95_ok},
    }


def _all_pass(check_group: dict):
    return all(bool(v.get("pass")) for v in (check_group or {}).values())


def main():
    ap = argparse.ArgumentParser(description="Enterprise sell-readiness gate (Quality / Governance / Ops / Auditability).")
    ap.add_argument("--policy", default="config/commercial_readiness_policy.json")
    ap.add_argument("--campaign-json", default="")
    ap.add_argument("--sla-json", default="")
    ap.add_argument("--outputs-dir", default="outputs")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    outputs_dir = Path(args.outputs_dir)
    policy = _load_json(Path(args.policy))

    campaign_path = Path(args.campaign_json) if args.campaign_json else _latest("institutional_campaign_*.json", outputs_dir)
    campaign = _load_json(campaign_path)

    if args.sla_json:
        sla_path = Path(args.sla_json)
    else:
        sla_path = _latest("ops_sla_report_*.json", outputs_dir)
    sla = _load_json(sla_path)

    summary = campaign.get("summary") or {}
    results = list(campaign.get("results") or [])
    if not results and isinstance(campaign.get("chunks"), list):
        for ch in campaign.get("chunks") or []:
            jp = ch.get("json")
            if not jp:
                continue
            p = Path(jp)
            if not p.exists():
                continue
            try:
                payload = _load_json(p)
                results.extend(payload.get("results") or [])
            except Exception:
                continue

    quality = _quality_check(summary, policy.get("quality") or {})
    governance_docs = _doc_check((policy.get("governance") or {}).get("required_docs") or [])
    governance = {k: {"pass": bool(v)} for k, v in governance_docs.items()}
    ops = _ops_check(sla, policy.get("operations") or {})
    ops_docs = _doc_check((policy.get("operations") or {}).get("required_docs") or [])
    for k, v in ops_docs.items():
        ops[f"doc:{k}"] = {"pass": bool(v)}
    audit = _audit_check(results, policy.get("auditability") or {})
    audit_docs = _doc_check((policy.get("auditability") or {}).get("required_docs") or [])
    for k, v in audit_docs.items():
        audit[f"doc:{k}"] = {"pass": bool(v)}

    dimensions = {
        "quality": {"checks": quality, "pass": _all_pass(quality)},
        "governance": {"checks": governance, "pass": _all_pass(governance)},
        "operations": {"checks": ops, "pass": _all_pass(ops)},
        "auditability": {"checks": audit, "pass": _all_pass(audit)},
    }

    overall = all(dim.get("pass") for dim in dimensions.values())
    payload = {
        "generated_at_utc": _now_utc(),
        "status": "PASS" if overall else "FAIL",
        "policy_file": str(Path(args.policy)),
        "campaign_file": str(campaign_path),
        "sla_file": str(sla_path),
        "dimensions": dimensions,
    }

    out = Path(args.out) if args.out else outputs_dir / f"enterprise_readiness_gate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out))
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()

