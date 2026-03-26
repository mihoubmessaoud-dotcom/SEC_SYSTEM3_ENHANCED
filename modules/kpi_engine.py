from __future__ import annotations

from typing import Dict, List


class KPIEngine:
    """
    Dynamic KPI assignment engine (rule-based).

    Returns KPI priorities per business model:
      - primary
      - secondary
      - ignored
    """

    KPI_MAP: Dict[str, Dict[str, List[str]]] = {
        "commercial_bank": {
            "primary": [
                "nim",
                "roe_spread",
                "roa",
                "tier1_capital_ratio",
                "efficiency_ratio",
            ],
            "secondary": [
                "net_interest_income_growth",
                "loan_loss_provision_ratio",
                "deposit_growth",
                "pb_ratio",
                "pe_ratio",
            ],
            "ignored": [
                "gross_margin",
                "inventory_days",
                "ccc_days",
                "fcf_yield",
            ],
        },
        "semiconductor_fabless": {
            "primary": [
                "roic",
                "gross_margin",
                "operating_margin",
                "rd_to_revenue",
                "revenue_cagr",
            ],
            "secondary": [
                "fcf_yield",
                "net_margin",
                "asset_turnover",
                "pe_ratio",
                "pb_ratio",
            ],
            "ignored": [
                "nim",
                "loan_loss_provision_ratio",
                "tier1_capital_ratio",
            ],
        },
        "semiconductor_idm": {
            "primary": [
                "roic",
                "gross_margin",
                "operating_margin",
                "capex_to_revenue",
                "inventory_days",
            ],
            "secondary": [
                "rd_to_revenue",
                "fcf_yield",
                "net_margin",
                "asset_turnover",
            ],
            "ignored": [
                "nim",
                "roe_spread",
                "loan_loss_provision_ratio",
            ],
        },
        "asset_light": {
            "primary": [
                "roic",
                "fcf_margin",
                "gross_margin",
                "operating_margin",
                "asset_turnover",
            ],
            "secondary": [
                "capex_to_revenue",
                "net_margin",
                "revenue_cagr",
                "debt_to_equity",
            ],
            "ignored": [
                "inventory_days",
                "nim",
                "loan_loss_provision_ratio",
            ],
        },
        "consumer_staples": {
            "primary": [
                "roic",
                "fcf_yield",
                "gross_margin",
                "operating_margin",
                "dividend_sustainability",
            ],
            "secondary": [
                "net_margin",
                "inventory_days",
                "debt_to_equity",
                "revenue_cagr",
            ],
            "ignored": [
                "nim",
                "roe_spread",
                "loan_loss_provision_ratio",
            ],
        },
    }

    def _normalize_model(self, model: str) -> str:
        return str(model or "").strip().lower()

    @staticmethod
    def _ordered_unique(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for item in items:
            k = str(item).strip()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    def _combine_hybrid(self, models: List[str]) -> Dict[str, List[str]]:
        """
        Merge priorities for hybrid models:
        - primary: union of primary from all models
        - secondary: union of secondary minus primary
        - ignored: intersection of ignored sets minus primary/secondary
        """
        valid_models = [m for m in models if m in self.KPI_MAP]
        if not valid_models:
            return {"primary": [], "secondary": [], "ignored": []}

        primary: List[str] = []
        secondary: List[str] = []
        ignored_sets = []
        for m in valid_models:
            conf = self.KPI_MAP[m]
            primary.extend(conf["primary"])
            secondary.extend(conf["secondary"])
            ignored_sets.append(set(conf["ignored"]))

        primary_u = self._ordered_unique(primary)
        secondary_u = self._ordered_unique([k for k in secondary if k not in set(primary_u)])
        ignored = set.intersection(*ignored_sets) if ignored_sets else set()
        ignored = ignored - set(primary_u) - set(secondary_u)

        return {
            "primary": primary_u,
            "secondary": secondary_u,
            "ignored": sorted(ignored),
        }

    def get_kpis_for_model(self, model: str) -> Dict[str, List[str]]:
        normalized = self._normalize_model(model)
        if normalized.startswith("hybrid:"):
            parts = [p.strip() for p in normalized.replace("hybrid:", "").split("+") if p.strip()]
            return self._combine_hybrid(parts)

        conf = self.KPI_MAP.get(normalized)
        if conf is None:
            return {"primary": [], "secondary": [], "ignored": []}

        return {
            "primary": list(conf["primary"]),
            "secondary": list(conf["secondary"]),
            "ignored": list(conf["ignored"]),
        }

    def _contextualize_with_alternatives(
        self,
        kpis: Dict[str, List[str]],
        alternatives: List[Dict[str, object]],
        confidence: float,
    ) -> Dict[str, List[str]]:
        """
        Context-aware tuning without hard blocking:
        - For low-confidence classification, blend top alternative primary KPIs into secondary.
        - `ignored` remains "de-prioritized" not blocked.
        """
        primary = self._ordered_unique(list(kpis.get("primary", [])))
        secondary = self._ordered_unique(list(kpis.get("secondary", [])))
        ignored = self._ordered_unique(list(kpis.get("ignored", [])))

        if confidence < 0.70 and alternatives:
            top_alt_model = str((alternatives[0] or {}).get("model", "")).strip().lower()
            if top_alt_model in self.KPI_MAP:
                alt_primary = self.KPI_MAP[top_alt_model].get("primary", [])
                for metric in alt_primary:
                    if metric not in primary and metric not in secondary:
                        secondary.append(metric)

        # No hard blocking: keep ignored only as de-prioritized hints.
        secondary = [m for m in secondary if m not in primary]
        ignored = [m for m in ignored if m not in primary and m not in secondary]
        return {
            "primary": primary,
            "secondary": secondary,
            "ignored": ignored,
        }

    def assign(self, business_model_result: Dict[str, object]) -> Dict[str, object]:
        """
        Accepts output from BusinessModelEngine.classify(...)
        and returns dynamic KPI mapping.
        """
        model = self._normalize_model(str((business_model_result or {}).get("model", "")))
        conf_raw = (business_model_result or {}).get("confidence")
        try:
            confidence = float(conf_raw) if conf_raw is not None else 0.0
        except (TypeError, ValueError):
            confidence = 0.0
        alternatives = list((business_model_result or {}).get("alternatives", []) or [])
        base_kpis = self.get_kpis_for_model(model)
        kpis = self._contextualize_with_alternatives(base_kpis, alternatives, confidence)
        return {
            "model": model,
            "confidence": round(confidence, 2),
            "kpis": kpis,
        }

    def assign_dynamic(self, business_model_result: Dict[str, object]) -> Dict[str, List[str]]:
        """
        Compact dynamic KPI assignment required by phase-7 contract.

        Output shape:
        {
          "primary": [...],
          "secondary": [...]
        }
        """
        assigned = self.assign(business_model_result)
        kpis = assigned.get("kpis", {}) if isinstance(assigned, dict) else {}
        return {
            "primary": list(kpis.get("primary", [])),
            "secondary": list(kpis.get("secondary", [])),
        }


__all__ = ["KPIEngine"]
