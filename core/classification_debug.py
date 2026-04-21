from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def write_classification_debug(
    *,
    canonical_classification: Dict[str, Any],
    outputs_dir: str = "outputs",
    ticker: str = "",
) -> Optional[str]:
    """
    Write canonical classification debug payload to outputs for auditability.
    Returns the written filepath (string) or None on failure.
    """
    try:
        out_dir = Path(outputs_dir) / "classification_debug"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        t = str(ticker or canonical_classification.get("diagnostics", {}).get("ticker") or "").upper() or "TICKER"
        path = out_dir / f"classification_{t}_{ts}.json"
        # keep the file compact but explicit
        payload = {
            "ticker": t,
            "sector_family": canonical_classification.get("sector_family"),
            "sector_template": canonical_classification.get("sector_template"),
            "operating_sub_sector": canonical_classification.get("operating_sub_sector"),
            "peer_group": canonical_classification.get("peer_group"),
            "entity_type": canonical_classification.get("entity_type"),
            "classification_confidence": canonical_classification.get("classification_confidence"),
            "classification_source": canonical_classification.get("classification_source"),
            "diagnostics": canonical_classification.get("diagnostics", {}),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)
    except Exception:
        return None

