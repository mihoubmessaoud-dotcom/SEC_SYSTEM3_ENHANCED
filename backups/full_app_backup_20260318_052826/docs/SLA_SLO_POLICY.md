# SLA / SLO Policy

## Service Objectives
- Availability target: >= 99.0%
- Successful run rate: >= 99.0%
- P95 batch latency target: <= 420 seconds per ticker

## Operational Monitoring
- Generate periodic SLA reports from recent batch outputs.
- Track:
1. Success rate (`PASS` + `PASS_WITH_WARNING`)
2. P50/P95/P99 latency
3. Fail trend by sector and reason

## Release Gate
- If SLO thresholds are violated, release is blocked until remediation.

