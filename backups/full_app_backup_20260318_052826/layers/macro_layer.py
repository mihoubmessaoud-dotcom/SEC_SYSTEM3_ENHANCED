"""FRED-only macro layer for analytics-required macro fields."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class LayerOutput:
    """Standardized payload wrapper for layer outputs."""

    payload: Dict[str, Any]


class MacroLayer:
    """Fetches macroeconomic series exclusively from FRED API."""

    SERIES = {
        "risk_free_rate": "DGS10",
        "two_year_yield": "DGS2",
        "cpi_index": "CPIAUCSL",
        "gdp_growth": "A191RL1Q225SBEA",
        "interest_rate": "FEDFUNDS",
        "unemployment_rate": "UNRATE",
        "industrial_production": "INDPRO",
    }

    def __init__(
        self,
        api_key: Optional[str],
        output_dir: str = "outputs",
        source_endpoint: str = "https://api.stlouisfed.org/fred/series/observations",
    ) -> None:
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.source_endpoint = source_endpoint

    def fetch(self, start_year: int, end_year: int) -> LayerOutput:
        """Return structured macro data for the requested year range."""
        observations_by_series = {
            key: self._fetch_series(series_id, start_year - 1, end_year, prefer_api=bool(self.api_key))
            for key, series_id in self.SERIES.items()
        }

        periods = {str(y): {"fields": {}} for y in range(start_year, end_year + 1)}

        for year in range(start_year, end_year + 1):
            year_end = f"{year}-12-31"
            fields: Dict[str, Any] = {}

            risk_free = self._value_for_year(observations_by_series["risk_free_rate"], year, as_percent=True)
            if risk_free is not None:
                fields["macro:risk_free_rate"] = {
                    "value": risk_free,
                    "unit": "ratio",
                    "period_end": year_end,
                    "series": self.SERIES["risk_free_rate"],
                }

            interest = self._value_for_year(observations_by_series["interest_rate"], year, as_percent=True)
            if interest is not None:
                fields["macro:interest_rate"] = {
                    "value": interest,
                    "unit": "ratio",
                    "period_end": year_end,
                    "series": self.SERIES["interest_rate"],
                }

            two_year = self._value_for_year(observations_by_series["two_year_yield"], year, as_percent=True)
            if two_year is not None:
                fields["macro:two_year_yield"] = {
                    "value": two_year,
                    "unit": "ratio",
                    "period_end": year_end,
                    "series": self.SERIES["two_year_yield"],
                }

            gdp_growth = self._value_for_year(observations_by_series["gdp_growth"], year, as_percent=True)
            if gdp_growth is not None:
                fields["macro:gdp_growth"] = {
                    "value": gdp_growth,
                    "unit": "ratio",
                    "period_end": year_end,
                    "series": self.SERIES["gdp_growth"],
                }

            inflation = self._cpi_yoy(observations_by_series["cpi_index"], year)
            if inflation is not None:
                fields["macro:inflation_rate"] = {
                    "value": inflation,
                    "unit": "ratio",
                    "period_end": year_end,
                    "series": self.SERIES["cpi_index"],
                }

            unemployment = self._value_for_year(
                observations_by_series["unemployment_rate"],
                year,
                as_percent=True,
            )
            if unemployment is not None:
                fields["macro:unemployment_rate"] = {
                    "value": unemployment,
                    "unit": "ratio",
                    "period_end": year_end,
                    "series": self.SERIES["unemployment_rate"],
                }

            ind_production = self._value_for_year(
                observations_by_series["industrial_production"],
                year,
                as_percent=False,
            )
            if ind_production is not None:
                fields["macro:industrial_production_index"] = {
                    "value": ind_production,
                    "unit": "index",
                    "period_end": year_end,
                    "series": self.SERIES["industrial_production"],
                }

            if risk_free is not None and two_year is not None:
                fields["macro:term_spread_10y_2y"] = {
                    "value": risk_free - two_year,
                    "unit": "ratio",
                    "period_end": year_end,
                    "series": f"{self.SERIES['risk_free_rate']}-{self.SERIES['two_year_yield']}",
                }

            periods[str(year)]["fields"] = fields

        populated_fields = 0
        for p in periods.values():
            populated_fields += len((p or {}).get("fields", {}))
        status = "OK" if populated_fields > 0 else "NO_DATA"
        source_label = "FRED-API" if self.api_key else "FRED-CSV-FALLBACK"

        payload = {
            "layer": "MACRO",
            "status": status,
            "source": source_label,
            "source_endpoint": self.source_endpoint,
            "periods": periods,
            "data_source_trace": {
                "endpoint": self.source_endpoint,
                "mode": "api" if self.api_key else "csv_fallback",
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"depends_on": [], "provides": ["MACRO"]},
        }
        if status == "NO_DATA" and not self.api_key:
            payload["warning"] = "FRED API key is missing and CSV fallback returned no rows."
        self._write_output(payload)
        return LayerOutput(payload=payload)

    def _fetch_series(
        self,
        series_id: str,
        start_year: int,
        end_year: int,
        prefer_api: bool,
    ) -> list[dict[str, str]]:
        if prefer_api:
            rows = self._fetch_series_api(series_id, start_year, end_year)
            if rows:
                return rows
        return self._fetch_series_csv(series_id, start_year, end_year)

    def _fetch_series_api(
        self,
        series_id: str,
        start_year: int,
        end_year: int,
    ) -> list[dict[str, str]]:
        try:
            response = requests.get(
                self.source_endpoint,
                params={
                    "series_id": series_id,
                    "api_key": self.api_key,
                    "file_type": "json",
                    "observation_start": f"{start_year}-01-01",
                    "observation_end": f"{end_year}-12-31",
                },
                timeout=25,
            )
            response.raise_for_status()
            data = response.json()
            obs = data.get("observations", [])
            return obs if isinstance(obs, list) else []
        except Exception:
            return []

    def _fetch_series_csv(
        self,
        series_id: str,
        start_year: int,
        end_year: int,
    ) -> list[dict[str, str]]:
        """
        Public CSV endpoint fallback that does not require API key:
        https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES_ID>
        """
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        try:
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            text = response.text
            reader = csv.DictReader(text.splitlines())
            rows: list[dict[str, str]] = []
            for row in reader:
                date_str = str(
                    row.get("DATE")
                    or row.get("date")
                    or row.get("observation_date")
                    or ""
                ).strip()
                if len(date_str) < 4:
                    continue
                try:
                    year = int(date_str[:4])
                except Exception:
                    continue
                if year < start_year or year > end_year:
                    continue
                value = (
                    row.get(series_id)
                    or row.get(series_id.upper())
                    or row.get(series_id.lower())
                )
                rows.append({"date": date_str, "value": value})
            return rows
        except Exception:
            return []

    def _value_for_year(
        self,
        observations: list[dict[str, str]],
        year: int,
        as_percent: bool,
    ) -> Optional[float]:
        candidates = [o for o in observations if str(o.get("date", "")).startswith(str(year))]
        if not candidates:
            return None
        latest = candidates[-1]
        raw = self._to_float(latest.get("value"))
        if raw is None:
            return None
        return raw / 100.0 if as_percent else raw

    def _cpi_yoy(self, observations: list[dict[str, str]], year: int) -> Optional[float]:
        current = self._value_for_year(observations, year, as_percent=False)
        previous = self._value_for_year(observations, year - 1, as_percent=False)
        if current is None or previous is None or previous == 0:
            return None
        return (current / previous) - 1.0

    def _disabled_payload(self, reason: str, start_year: int, end_year: int) -> Dict[str, Any]:
        periods = {str(y): {"fields": {}} for y in range(start_year, end_year + 1)}
        return {
            "layer": "MACRO",
            "status": reason,
            "source": "FRED",
            "source_endpoint": self.source_endpoint,
            "periods": periods,
            "data_source_trace": {"endpoint": self.source_endpoint},
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"depends_on": [], "provides": ["MACRO"]},
        }

    def _write_output(self, payload: Dict[str, Any]) -> None:
        out = self.output_dir / "structured_macro_data.json"
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            if value is None or value == ".":
                return None
            return float(value)
        except Exception:
            return None
