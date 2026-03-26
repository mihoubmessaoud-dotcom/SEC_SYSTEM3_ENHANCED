from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional

import pandas as pd

from .engine import EngineConfig, InstitutionalFinancialIntelligenceEngine


class InstitutionalEngineAPI:
    """
    Programmatic API wrapper returning JSON-safe payloads for dashboards and ML.

    Integrations:
    - XBRL parser output adapter (expects concept-value structures)
    - Dashboard clients (returns compact JSON tables)
    - ML modules (returns AI-ready dataset rows)
    """

    def __init__(self, config: Optional[EngineConfig] = None) -> None:
        self.engine = InstitutionalFinancialIntelligenceEngine(config=config)

    def process_company(
        self,
        company_meta: Dict,
        data_by_year: Dict[int, Dict[str, float]],
        data_by_period: Optional[Dict[str, Dict[str, float]]] = None,
        fx_rates: Optional[Dict[int, float]] = None,
        scenario: Optional[Dict] = None,
        save_outputs: bool = False,
        output_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        outputs = self.engine.run(
            company_meta=company_meta,
            data_by_year=data_by_year,
            data_by_period=data_by_period,
            fx_rates=fx_rates,
            scenario=scenario,
        )
        payload = self.to_json_payload(outputs)
        if save_outputs:
            payload['saved_files'] = self.engine.save_outputs(outputs, output_dir=output_dir)
        return payload

    def process_from_xbrl_parser(self, parser_result: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Adapter for typical XBRL parser outputs.

        Expected minimal parser_result schema:
        {
          "company_meta": {...},
          "facts": [{"year": 2024, "concept": "Revenues", "value": 1000.0}, ...],
          "period_facts": [{"period": "2024-FY", "concept": "Revenues", "value": 1000.0}, ...]
        }
        """
        company_meta = dict(parser_result.get('company_meta') or {})
        facts = parser_result.get('facts') or []
        period_facts = parser_result.get('period_facts') or []

        data_by_year: Dict[int, Dict[str, float]] = {}
        for f in facts:
            year = int(f.get('year'))
            concept = str(f.get('concept'))
            value = f.get('value')
            if isinstance(value, (int, float)):
                data_by_year.setdefault(year, {})[concept] = float(value)

        data_by_period: Dict[str, Dict[str, float]] = {}
        for f in period_facts:
            period = str(f.get('period'))
            concept = str(f.get('concept'))
            value = f.get('value')
            if isinstance(value, (int, float)):
                data_by_period.setdefault(period, {})[concept] = float(value)

        return self.process_company(
            company_meta=company_meta,
            data_by_year=data_by_year,
            data_by_period=data_by_period,
            **kwargs,
        )

    def to_json_payload(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for key, value in outputs.items():
            payload[key] = self._to_jsonable(value)
        return payload

    def _to_jsonable(self, value: Any) -> Any:
        if isinstance(value, pd.DataFrame):
            return value.to_dict(orient='records')
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, dict):
            return {str(k): self._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._to_jsonable(v) for v in value]
        return value
