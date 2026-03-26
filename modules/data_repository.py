"""Production-grade single source of truth repository for financial data.

This repository is intentionally strict:
- no duplicate writes
- no backfill behavior
- explicit NO_SOURCE_DATA on missing keys
- immutable/read-only snapshots for consumers
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Dict, List


class DataRepositoryError(Exception):
    """Base exception for DataRepository errors."""


class DuplicateWriteError(DataRepositoryError):
    """Raised when a write is attempted for an existing key."""


class DataRepository:
    """Single source of truth for raw/clean/metadata data layers."""

    NO_SOURCE_DATA = "NO_SOURCE_DATA"
    DUPLICATE_WRITE = "DUPLICATE_WRITE"

    def __init__(self) -> None:
        self._raw_data: Dict[str, Any] = {}
        self._clean_data: Dict[str, Any] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}
        self._audit_log: List[Dict[str, Any]] = []

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _validate_key(self, key: str) -> None:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("key must be a non-empty string")

    def _ensure_not_exists(self, store: Dict[str, Any], key: str) -> None:
        if key in store:
            raise DuplicateWriteError(f"{self.DUPLICATE_WRITE}: {key}")

    def _audit(self, action: str, key: str, reason: str | None = None) -> None:
        self._audit_log.append(
            {
                "timestamp_utc": self._now(),
                "action": action,
                "key": key,
                "reason": reason or "",
            }
        )

    def set_raw(self, key: str, value: Any) -> None:
        """Write raw data once only; duplicate writes are rejected."""
        self._validate_key(key)
        self._ensure_not_exists(self._raw_data, key)
        self._raw_data[key] = deepcopy(value)

        meta = self._metadata.setdefault(key, {})
        meta["raw"] = {
            "written_at_utc": self._now(),
            "reason": "RAW_SOURCE_DATA",
        }
        self._audit("set_raw", key, reason="RAW_SOURCE_DATA")

    def get_raw(self, key: str) -> Dict[str, Any]:
        """Return raw value or explicit NO_SOURCE_DATA response."""
        self._validate_key(key)
        if key not in self._raw_data:
            return {"value": None, "reason": self.NO_SOURCE_DATA}
        return {"value": deepcopy(self._raw_data[key]), "reason": "RAW_SOURCE_DATA"}

    def set_clean(self, key: str, value: Any, reason: str) -> None:
        """Write clean data once only; reason is mandatory and stored."""
        self._validate_key(key)
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        self._ensure_not_exists(self._clean_data, key)
        self._clean_data[key] = deepcopy(value)

        meta = self._metadata.setdefault(key, {})
        meta["clean"] = {
            "written_at_utc": self._now(),
            "reason": reason,
        }
        self._audit("set_clean", key, reason=reason)

    def get_clean(self, key: str) -> Dict[str, Any]:
        """Return clean value and clean reason or NO_SOURCE_DATA."""
        self._validate_key(key)
        if key not in self._clean_data:
            return {"value": None, "reason": self.NO_SOURCE_DATA}
        clean_meta = (self._metadata.get(key, {}) or {}).get("clean", {})
        return {
            "value": deepcopy(self._clean_data[key]),
            "reason": clean_meta.get("reason", self.NO_SOURCE_DATA),
        }

    @property
    def raw_data(self) -> MappingProxyType:
        """Read-only snapshot of raw data."""
        return MappingProxyType(deepcopy(self._raw_data))

    @property
    def clean_data(self) -> MappingProxyType:
        """Read-only snapshot of clean data."""
        return MappingProxyType(deepcopy(self._clean_data))

    @property
    def metadata(self) -> MappingProxyType:
        """Read-only snapshot of metadata."""
        return MappingProxyType(deepcopy(self._metadata))

    @property
    def audit_log(self) -> List[Dict[str, Any]]:
        """Read-only (copy) audit log for every write action."""
        return deepcopy(self._audit_log)


if __name__ == "__main__":
    repo = DataRepository()
    repo.set_raw("revenue_2025", 416_161)
    repo.set_clean("gross_margin_2025", 0.462, reason="computed_from_revenue_minus_cogs")
    print(repo.get_raw("revenue_2025"))
    print(repo.get_clean("gross_margin_2025"))
    print(repo.get_clean("missing_metric"))
