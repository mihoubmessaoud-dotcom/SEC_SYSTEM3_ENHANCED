# Methodology and Audit Trail

## Ratio Construction
- Ratios are computed from canonicalized accounting concepts.
- Each computed ratio must expose:
1. Formula used
2. Input concepts/tags
3. Raw values used
4. Reason/status and reliability metadata

## Item Mapping Logic
- Parent/child statement tree matching is preferred when available.
- Sector-aware canonical mapping is applied for renamed concepts.
- Explicit fallback paths are tagged for auditability.

## Non-Computable Handling
- Not-computable status must include reason code.
- Critical release mode can enforce no missing required ratios.

## Audit Artifacts
- Institutional batch gate JSON/MD.
- Campaign JSON/MD.
- Gap report JSON/MD.
- SLA report JSON.
- Enterprise readiness gate JSON.

