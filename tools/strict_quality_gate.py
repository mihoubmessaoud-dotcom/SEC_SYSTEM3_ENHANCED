#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Strict Quality Gate (Phase 1)
-----------------------------
Fail-closed release gate that blocks integration when any mandatory
reference checks fail.
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List


@dataclass
class GateCheck:
    name: str
    command: List[str]
    passed: bool = False
    return_code: int = -1
    stdout: str = ""
    stderr: str = ""


def _run(check: GateCheck) -> GateCheck:
    proc = subprocess.run(
        check.command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    check.return_code = int(proc.returncode)
    check.stdout = proc.stdout or ""
    check.stderr = proc.stderr or ""
    check.passed = proc.returncode == 0
    return check


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    out_dir = root / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    checks = [
        GateCheck(
            name="Compile critical modules",
            command=[
                sys.executable,
                "-m",
                "py_compile",
                "main.py",
                "modules/sec_fetcher.py",
                "modules/ratio_engine.py",
                "modules/advanced_analysis.py",
                "modules/verdict_engine.py",
                "modules/investment_quality_engine.py",
            ],
        ),
        GateCheck(
            name="Acceptance basket (must all pass)",
            command=[
                sys.executable,
                "-m",
                "pytest",
                "tests/test_acceptance.py",
                "-q",
                "--tb=short",
            ],
        ),
        GateCheck(
            name="Institutional engine regression",
            command=[
                sys.executable,
                "-m",
                "pytest",
                "tests/test_institutional_engine.py",
                "-q",
                "--tb=short",
            ],
        ),
    ]

    executed = [_run(c) for c in checks]
    overall_pass = all(c.passed for c in executed)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PASS" if overall_pass else "FAIL",
        "checks": [
            {
                "name": c.name,
                "passed": c.passed,
                "return_code": c.return_code,
                "stdout_tail": c.stdout[-2000:],
                "stderr_tail": c.stderr[-2000:],
            }
            for c in executed
        ],
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"strict_quality_gate_{stamp}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    safe_path = str(out_path).encode("ascii", errors="backslashreplace").decode("ascii")
    print(safe_path)
    print(json.dumps({"status": payload["status"]}, ensure_ascii=True))
    return 0 if overall_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
