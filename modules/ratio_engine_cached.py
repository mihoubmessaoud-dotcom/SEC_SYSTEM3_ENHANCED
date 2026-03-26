from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


class DuplicateRatioCalculationError(Exception):
    """Raised when strict mode blocks repeated ratio computation requests."""


@dataclass(frozen=True)
class RatioRecord:
    ratio: str
    year: int
    value: Optional[float]
    reason: str
    inputs: Dict[str, Optional[float]]
    cached: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RatioRegistry:
    """
    Cache system that guarantees each ratio/year pair is written once.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, RatioRecord] = {}

    @staticmethod
    def _key(ratio: str, year: int) -> str:
        return f"{str(ratio).strip().lower()}:{int(year)}"

    def has(self, ratio: str, year: int) -> bool:
        return self._key(ratio, year) in self._cache

    def get(self, ratio: str, year: int) -> Optional[RatioRecord]:
        rec = self._cache.get(self._key(ratio, year))
        if rec is None:
            return None
        return RatioRecord(
            ratio=rec.ratio,
            year=rec.year,
            value=rec.value,
            reason=rec.reason,
            inputs=dict(rec.inputs),
            cached=True,
        )

    def set(self, record: RatioRecord) -> None:
        key = self._key(record.ratio, record.year)
        if key in self._cache:
            raise KeyError(f"DUPLICATE_RATIO_WRITE: {key}")
        self._cache[key] = record

    def as_dict(self) -> Dict[str, Dict[str, Any]]:
        return {k: v.to_dict() for k, v in self._cache.items()}


class RatioEngine:
    """
    Single-pass ratio calculator with strict cache behavior.

    Rules:
    - Each ratio/year is calculated once.
    - Data source is DataRepository clean_data only.
    - No mutation of source data.
    """

    SUPPORTED = {"ebitda", "roic", "roe", "peg", "gross_margin"}

    def __init__(self, repository: Any, registry: Optional[RatioRegistry] = None) -> None:
        self.repository = repository
        self.registry = registry or RatioRegistry()
        self._compute_counter: Dict[str, int] = {}

    def _count_compute(self, ratio: str) -> None:
        key = str(ratio).lower().strip()
        current = self._compute_counter.get(key)
        self._compute_counter[key] = (int(current) + 1) if current is not None else 1

    def get_compute_count(self, ratio: str) -> int:
        return self._compute_counter.get(str(ratio).lower().strip(), 0)

    def _fetch_clean(self, metric: str, year: int) -> tuple[Optional[float], str]:
        key = f"{str(metric).strip().lower()}:{int(year)}"
        got = self.repository.get_clean(key)
        value = got.get("value")
        if value is None:
            return None, got.get("reason") or "NO_SOURCE_DATA"
        try:
            return float(value), got.get("reason") or "CLEAN_SOURCE"
        except (TypeError, ValueError):
            return None, "NON_NUMERIC_SOURCE"

    @staticmethod
    def _safe_div(num: Optional[float], den: Optional[float]) -> Optional[float]:
        if num is None or den is None:
            return None
        if abs(float(den)) < 1e-12:
            return None
        return float(num) / float(den)

    @staticmethod
    def _normalize_growth_percent_points(growth: float) -> float:
        # PEG uses growth as percentage points.
        # If growth stored as decimal (0.15), convert to 15.
        g = float(growth)
        if -1.0 <= g <= 1.0:
            return g * 100.0
        return g

    def _calc_roic(self, year: int) -> RatioRecord:
        op_income, r1 = self._fetch_clean("operating_income", year)
        tax_rate, r2 = self._fetch_clean("tax_rate", year)
        invested_capital, r3 = self._fetch_clean("invested_capital", year)
        inputs = {
            "operating_income": op_income,
            "tax_rate": tax_rate,
            "invested_capital": invested_capital,
        }
        if op_income is None or tax_rate is None or invested_capital is None:
            return RatioRecord("roic", year, None, "NO_SOURCE_DATA", inputs)
        nopat = op_income * (1.0 - tax_rate)
        roic = self._safe_div(nopat, invested_capital)
        if roic is None:
            return RatioRecord("roic", year, None, "ZERO_DENOMINATOR", inputs)
        return RatioRecord("roic", year, roic, "", inputs)

    def _calc_ebitda(self, year: int) -> RatioRecord:
        op_income, _ = self._fetch_clean("operating_income", year)
        dep_amort, _ = self._fetch_clean("depreciation_amortization", year)
        inputs = {
            "operating_income": op_income,
            "depreciation_amortization": dep_amort,
        }
        if op_income is None or dep_amort is None:
            return RatioRecord("ebitda", year, None, "NO_SOURCE_DATA", inputs)
        return RatioRecord("ebitda", year, op_income + dep_amort, "", inputs)

    def _calc_roe(self, year: int) -> RatioRecord:
        net_income, _ = self._fetch_clean("net_income", year)
        total_equity, _ = self._fetch_clean("total_equity", year)
        inputs = {"net_income": net_income, "total_equity": total_equity}
        if net_income is None or total_equity is None:
            return RatioRecord("roe", year, None, "NO_SOURCE_DATA", inputs)
        roe = self._safe_div(net_income, total_equity)
        if roe is None:
            return RatioRecord("roe", year, None, "ZERO_DENOMINATOR", inputs)
        return RatioRecord("roe", year, roe, "", inputs)

    def _calc_peg(self, year: int) -> RatioRecord:
        pe_ratio, _ = self._fetch_clean("pe_ratio", year)
        eps_growth, _ = self._fetch_clean("earnings_growth", year)
        inputs = {"pe_ratio": pe_ratio, "earnings_growth": eps_growth}
        if pe_ratio is None or eps_growth is None:
            return RatioRecord("peg", year, None, "NO_SOURCE_DATA", inputs)
        growth_pp = self._normalize_growth_percent_points(eps_growth)
        peg = self._safe_div(pe_ratio, growth_pp)
        if peg is None:
            return RatioRecord("peg", year, None, "ZERO_DENOMINATOR", inputs)
        return RatioRecord("peg", year, peg, "", inputs)

    def _calc_gross_margin(self, year: int) -> RatioRecord:
        gross_profit, _ = self._fetch_clean("gross_profit", year)
        revenue, _ = self._fetch_clean("revenue", year)
        inputs = {"gross_profit": gross_profit, "revenue": revenue}
        if gross_profit is None or revenue is None:
            return RatioRecord("gross_margin", year, None, "NO_SOURCE_DATA", inputs)
        gm = self._safe_div(gross_profit, revenue)
        if gm is None:
            return RatioRecord("gross_margin", year, None, "ZERO_DENOMINATOR", inputs)
        return RatioRecord("gross_margin", year, gm, "", inputs)

    def calculate(self, ratio: str, year: int, *, strict_no_repeat: bool = False) -> Dict[str, Any]:
        ratio_name = str(ratio).strip().lower()
        y = int(year)
        if ratio_name not in self.SUPPORTED:
            raise ValueError(f"UNSUPPORTED_RATIO: {ratio_name}")

        cached = self.registry.get(ratio_name, y)
        if cached is not None:
            if strict_no_repeat:
                raise DuplicateRatioCalculationError(
                    f"DUPLICATE_RATIO_CALCULATION: {ratio_name}:{y}"
                )
            return cached.to_dict()

        self._count_compute(ratio_name)
        if ratio_name == "ebitda":
            rec = self._calc_ebitda(y)
        elif ratio_name == "roic":
            rec = self._calc_roic(y)
        elif ratio_name == "roe":
            rec = self._calc_roe(y)
        elif ratio_name == "peg":
            rec = self._calc_peg(y)
        else:
            rec = self._calc_gross_margin(y)

        self.registry.set(rec)
        return rec.to_dict()


__all__ = [
    "RatioEngine",
    "RatioRegistry",
    "RatioRecord",
    "DuplicateRatioCalculationError",
]
