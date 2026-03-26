from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple


class FinancialSignatureEngine:
    """
    Generates model-aware financial signature using 3-year trends.

    Output dimensions:
      - growth_profile
      - profitability
      - capital_intensity
      - financial_risk
    """

    MODEL_WEIGHTS: Dict[str, Dict[str, float]] = {
        "commercial_bank": {
            "growth_profile": 0.20,
            "profitability": 0.35,
            "capital_intensity": 0.10,
            "financial_risk": 0.35,
        },
        "semiconductor_fabless": {
            "growth_profile": 0.30,
            "profitability": 0.35,
            "capital_intensity": 0.15,
            "financial_risk": 0.20,
        },
        "semiconductor_idm": {
            "growth_profile": 0.25,
            "profitability": 0.30,
            "capital_intensity": 0.25,
            "financial_risk": 0.20,
        },
        "consumer_staples": {
            "growth_profile": 0.20,
            "profitability": 0.35,
            "capital_intensity": 0.20,
            "financial_risk": 0.25,
        },
        "asset_light": {
            "growth_profile": 0.25,
            "profitability": 0.35,
            "capital_intensity": 0.15,
            "financial_risk": 0.25,
        },
    }

    FALLBACK_MODEL = "asset_light"

    @staticmethod
    def _safe_float(v: Optional[float]) -> Optional[float]:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _normalize_model(self, model: str) -> str:
        m = str(model or "").strip().lower()
        if m.startswith("hybrid:"):
            m = m.replace("hybrid:", "").split("+")[0].strip()
        return m or self.FALLBACK_MODEL

    def _last_3_values(self, series: Dict[int, Optional[float]]) -> Optional[List[float]]:
        pairs = sorted((int(y), self._safe_float(v)) for y, v in series.items())
        clean = [v for _, v in pairs if v is not None]
        if len(clean) < 3:
            return None
        return clean[-3:]

    @staticmethod
    def _trend_score(values: Sequence[float], *, higher_is_better: bool = True, bounded: bool = True) -> float:
        """
        Trend score [0..100] from 3 values using direction + stability.
        """
        if len(values) < 3:
            return 0.0
        a, b, c = values[-3], values[-2], values[-1]
        slope = c - a
        noise = abs((b - a) - (c - b))
        base = 50.0

        if bounded:
            # normalize slope by magnitude context
            scale = max(abs(a), abs(b), abs(c), 1e-6)
            slope_norm = slope / scale
        else:
            slope_norm = slope

        if higher_is_better:
            base += max(min(slope_norm * 120.0, 40.0), -40.0)
        else:
            base -= max(min(slope_norm * 120.0, 40.0), -40.0)

        # stability reward/penalty
        base += max(0.0, 10.0 - min(noise * 100.0, 10.0))
        return max(0.0, min(100.0, round(base, 2)))

    @staticmethod
    def _level_score(value: Optional[float], lo: float, hi: float) -> float:
        if value is None:
            return 0.0
        v = float(value)
        if v <= lo:
            return 0.0
        if v >= hi:
            return 100.0
        return (v - lo) / (hi - lo) * 100.0

    def _growth_profile(self, model: str, series: Dict[str, Dict[int, Optional[float]]]) -> float:
        rev = self._last_3_values(series.get("revenue_growth", {}))
        ni = self._last_3_values(series.get("net_income_growth", {}))
        if rev is None or ni is None:
            return 0.0
        rev_s = self._trend_score(rev, higher_is_better=True, bounded=True)
        ni_s = self._trend_score(ni, higher_is_better=True, bounded=True)
        if model == "commercial_bank":
            return round(rev_s * 0.4 + ni_s * 0.6, 2)
        return round(rev_s * 0.55 + ni_s * 0.45, 2)

    def _profitability(self, model: str, series: Dict[str, Dict[int, Optional[float]]]) -> float:
        roic = self._last_3_values(series.get("roic", {}))
        op_margin = self._last_3_values(series.get("operating_margin", {}))
        roe_spread = self._last_3_values(series.get("roe_spread", {}))
        nim = self._last_3_values(series.get("nim", {}))

        if model == "commercial_bank":
            if roe_spread is None or nim is None:
                return 0.0
            return round(
                self._trend_score(roe_spread, higher_is_better=True) * 0.65
                + self._trend_score(nim, higher_is_better=True) * 0.35,
                2,
            )

        if roic is None or op_margin is None:
            return 0.0
        return round(
            self._trend_score(roic, higher_is_better=True) * 0.6
            + self._trend_score(op_margin, higher_is_better=True) * 0.4,
            2,
        )

    def _capital_intensity(self, model: str, series: Dict[str, Dict[int, Optional[float]]]) -> float:
        capex = self._last_3_values(series.get("capex_to_revenue", {}))
        asset_turn = self._last_3_values(series.get("asset_turnover", {}))
        if capex is None:
            return 0.0

        if model == "semiconductor_idm":
            # high capex normal for IDM: prefer stable/efficient, not necessarily low
            capex_mid = [abs(v - 0.30) for v in capex]
            capex_score = 100.0 - min(sum(capex_mid) / 3.0 * 300.0, 100.0)
        elif model == "commercial_bank":
            capex_score = self._trend_score(capex, higher_is_better=False)
        else:
            capex_score = self._trend_score(capex, higher_is_better=False)

        if asset_turn is None:
            return round(capex_score, 2)
        at_score = self._trend_score(asset_turn, higher_is_better=True)
        return round(capex_score * 0.6 + at_score * 0.4, 2)

    def _financial_risk(self, model: str, series: Dict[str, Dict[int, Optional[float]]]) -> float:
        leverage = self._last_3_values(series.get("leverage", {}))
        interest_cov = self._last_3_values(series.get("interest_coverage", {}))
        zscore = self._last_3_values(series.get("altman_z", {}))

        if leverage is None:
            return 0.0

        lev_score = self._trend_score(leverage, higher_is_better=False)
        ic_score = self._trend_score(interest_cov, higher_is_better=True) if interest_cov else 50.0
        z_score = self._trend_score(zscore, higher_is_better=True) if zscore else 50.0

        if model == "commercial_bank":
            # leverage structurally higher in banks; reduce penalty emphasis
            lev_score = min(100.0, lev_score + 15.0)
            return round(lev_score * 0.35 + ic_score * 0.35 + z_score * 0.30, 2)
        return round(lev_score * 0.45 + ic_score * 0.30 + z_score * 0.25, 2)

    def generate_signature(self, model: str, trend_data: Dict[str, Dict[int, Optional[float]]]) -> Dict[str, object]:
        """
        trend_data expects metric -> {year -> value}
        Uses last 3 available values per metric.
        """
        model_key = self._normalize_model(model)
        weights = self.MODEL_WEIGHTS.get(model_key, self.MODEL_WEIGHTS[self.FALLBACK_MODEL])

        growth_profile = self._growth_profile(model_key, trend_data)
        profitability = self._profitability(model_key, trend_data)
        capital_intensity = self._capital_intensity(model_key, trend_data)
        financial_risk = self._financial_risk(model_key, trend_data)

        overall = (
            growth_profile * weights["growth_profile"]
            + profitability * weights["profitability"]
            + capital_intensity * weights["capital_intensity"]
            + financial_risk * weights["financial_risk"]
        )

        return {
            "model_used": model_key,
            "growth_profile": round(growth_profile, 2),
            "profitability": round(profitability, 2),
            "capital_intensity": round(capital_intensity, 2),
            "financial_risk": round(financial_risk, 2),
            "overall_signature_score": round(max(0.0, min(100.0, overall)), 2),
        }


__all__ = ["FinancialSignatureEngine"]

