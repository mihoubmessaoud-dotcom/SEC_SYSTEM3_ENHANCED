# Model Risk Policy

## Scope
This policy defines model risk controls for ratio computation, item mapping, and strategic analytics.

## Controls
- Fail-closed behavior for missing critical inputs.
- Mandatory provenance fields for computed ratios.
- Plausibility bounds for high-risk ratios.
- Sector-specific gating where economic structure differs.
- Versioned mappings and deterministic fallback rules.

## Change Management
- Every release must include:
1. Regression test execution.
2. Campaign validation report.
3. Gap report for missing concepts/reasons.
4. Release notes with behavior deltas.

## Escalation
- If effective pass rate drops below target, release is blocked.
- If critical ratio drift appears in regression set, release is blocked.

