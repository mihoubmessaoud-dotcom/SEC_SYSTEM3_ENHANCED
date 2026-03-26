"""Polygon.io-only data layer for market metrics required by analytics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class LayerOutput:
    """Standardized payload wrapper for layer outputs."""

    payload: Dict[str, Any]


class MarketLayer:
    """Fetches market-only data from Polygon and exposes required market fields."""

    def __init__(
        self,
        api_key: Optional[str],
        output_dir: str = "outputs",
        source_endpoint: str = "https://api.polygon.io",
    ) -> None:
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.source_endpoint = source_endpoint.rstrip("/")

    def fetch(self, ticker: str, start_year: int, end_year: int) -> LayerOutput:
        """Return structured market data for a ticker and year range."""
        if not self.api_key:
            payload = self._disabled_payload(ticker, "MISSING_API_KEY", start_year, end_year)
            self._write_output(payload)
            return LayerOutput(payload=payload)

        periods: Dict[str, Any] = {}
        for year in range(start_year, end_year + 1):
            periods[str(year)] = {
                "fields": self._fetch_year_fields(ticker=ticker, year=year)
            }

        payload = {
            "layer": "MARKET",
            "status": "OK",
            "ticker": ticker,
            "source": "Polygon.io",
            "source_endpoint": self.source_endpoint,
            "periods": periods,
            "data_source_trace": {"ticker": ticker, "endpoint": self.source_endpoint},
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"depends_on": [], "provides": ["MARKET"]},
        }
        self._write_output(payload)
        return LayerOutput(payload=payload)

    def _fetch_year_fields(self, ticker: str, year: int) -> Dict[str, Any]:
        year_end = f"{year}-12-31"
        year_start = f"{year}-01-01"

        open_close = self._safe_get(f"{self.source_endpoint}/v1/open-close/{ticker}/{year_end}")
        reference = self._safe_get(
            f"{self.source_endpoint}/v3/reference/tickers/{ticker}",
            params={"date": year_end},
        )
        financials = self._safe_get(
            f"{self.source_endpoint}/vX/reference/financials",
            params={
                "ticker": ticker,
                "timeframe": "annual",
                "filing_date.gte": year_start,
                "filing_date.lte": year_end,
                "limit": 20,
                "order": "desc",
                "sort": "filing_date",
            },
        )
        aggs = self._safe_get(
            f"{self.source_endpoint}/v2/aggs/ticker/{ticker}/range/1/day/{year_start}/{year_end}"
        )
        dividends = self._safe_get(
            f"{self.source_endpoint}/v3/reference/dividends",
            params={
                "ticker": ticker,
                "ex_dividend_date.gte": year_start,
                "ex_dividend_date.lte": year_end,
                "limit": 1000,
            },
        )

        fields: Dict[str, Any] = {}

        close_price = self._to_float((open_close or {}).get("close"))
        if close_price is None:
            close_price = self._last_close_from_aggs(aggs)
        if close_price is not None:
            fields["market:price"] = {
                "value": close_price,
                "unit": "USD/share",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        volume = self._to_float((open_close or {}).get("volume"))
        if volume is None:
            volume = self._sum_volume_from_aggs(aggs)
        if volume is not None:
            fields["market:volume"] = {
                "value": volume,
                "unit": "shares",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        ref_results = (reference or {}).get("results", {}) if isinstance(reference, dict) else {}
        market_cap_ref = self._to_float(ref_results.get("market_cap"))
        shares_ref = self._to_float(ref_results.get("share_class_shares_outstanding"))
        eps_basic, bvps, shares_fin = self._extract_financials_point_estimates(financials)
        shares = shares_fin if shares_fin is not None else shares_ref

        market_cap = None
        if close_price is not None and shares is not None:
            market_cap = close_price * shares
        elif market_cap_ref is not None:
            market_cap = market_cap_ref

        if market_cap is not None:
            fields["market:market_cap"] = {
                "value": market_cap,
                "unit": "USD",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }
        if shares is not None:
            fields["market:shares_outstanding"] = {
                "value": shares,
                "unit": "shares",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        debt_fin, cash_fin = self._extract_balance_estimates(financials)
        enterprise_value = None
        if None not in (market_cap, debt_fin, cash_fin):
            enterprise_value = market_cap + debt_fin - cash_fin
        if enterprise_value is None:
            enterprise_value = self._to_float(ref_results.get("enterprise_value"))
        if enterprise_value is not None:
            fields["market:enterprise_value"] = {
                "value": enterprise_value,
                "unit": "USD",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }
        if debt_fin is not None:
            fields["market:total_debt"] = {
                "value": debt_fin,
                "unit": "USD",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        beta = self._to_float(ref_results.get("beta"))
        if beta is not None:
            fields["market:beta"] = {
                "value": beta,
                "unit": "ratio",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        total_return = self._extract_total_return(aggs)
        if total_return is not None:
            fields["market:total_return"] = {
                "value": total_return,
                "unit": "ratio",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        dividend_yield = self._extract_dividend_yield(dividends, close_price)
        if dividend_yield is not None:
            fields["market:dividend_yield"] = {
                "value": dividend_yield,
                "unit": "ratio",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }
        annual_dividends = self._extract_annual_dividends(dividends)
        if annual_dividends is not None:
            fields["market:annual_dividends_per_share"] = {
                "value": annual_dividends,
                "unit": "USD/share",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        if eps_basic is not None:
            fields["market:eps_basic"] = {
                "value": eps_basic,
                "unit": "USD/share",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }
        if bvps is not None:
            fields["market:book_value_per_share"] = {
                "value": bvps,
                "unit": "USD/share",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }
        if close_price is not None and eps_basic not in (None, 0):
            fields["market:pe_ratio"] = {
                "value": close_price / eps_basic,
                "unit": "ratio",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }
        if close_price is not None and bvps not in (None, 0):
            fields["market:pb_ratio"] = {
                "value": close_price / bvps,
                "unit": "ratio",
                "period_end": year_end,
                "mode": "HISTORICAL",
            }

        return fields

    def _extract_total_return(self, aggs: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(aggs, dict):
            return None
        results = aggs.get("results") or []
        if not isinstance(results, list) or len(results) < 2:
            return None
        first_close = self._to_float(results[0].get("c"))
        last_close = self._to_float(results[-1].get("c"))
        if first_close is None or last_close is None or first_close == 0:
            return None
        return (last_close / first_close) - 1.0

    def _extract_dividend_yield(
        self,
        dividends: Optional[Dict[str, Any]],
        close_price: Optional[float],
    ) -> Optional[float]:
        if close_price is None or close_price <= 0 or not isinstance(dividends, dict):
            return None
        results = dividends.get("results") or []
        if not isinstance(results, list):
            return None
        annual_dividends = 0.0
        for row in results:
            cash_amount = self._to_float((row or {}).get("cash_amount"))
            if cash_amount is not None:
                annual_dividends += cash_amount
        if annual_dividends <= 0:
            return None
        return annual_dividends / close_price

    def _extract_annual_dividends(
        self,
        dividends: Optional[Dict[str, Any]],
    ) -> Optional[float]:
        if not isinstance(dividends, dict):
            return None
        results = dividends.get("results") or []
        if not isinstance(results, list):
            return None
        total = 0.0
        found = False
        for row in results:
            cash_amount = self._to_float((row or {}).get("cash_amount"))
            if cash_amount is not None:
                total += cash_amount
                found = True
        if not found:
            return None
        return total

    def _extract_financials_point_estimates(
        self,
        financials: Optional[Dict[str, Any]],
    ) -> tuple[Optional[float], Optional[float], Optional[float]]:
        if not isinstance(financials, dict):
            return None, None, None
        results = financials.get("results") or []
        if not isinstance(results, list) or not results:
            return None, None, None
        item = results[0] or {}
        node = item.get("financials") or {}
        income = node.get("income_statement") or {}
        balance = node.get("balance_sheet") or {}

        eps_candidates = [
            self._nested_value(income, "basic_earnings_per_share", "value"),
            self._nested_value(income, "diluted_earnings_per_share", "value"),
            self._nested_value(income, "earnings_per_share_basic", "value"),
        ]
        eps = next((x for x in eps_candidates if x is not None), None)

        bvps_candidates = [
            self._nested_value(balance, "book_value_per_share", "value"),
            self._nested_value(balance, "common_stock_book_value_per_share", "value"),
        ]
        bvps = next((x for x in bvps_candidates if x is not None), None)
        shares_candidates = [
            self._nested_value(balance, "share_class_shares_outstanding", "value"),
            self._nested_value(income, "weighted_average_shares", "value"),
            self._nested_value(income, "weighted_average_shares_outstanding_basic", "value"),
        ]
        shares = next((x for x in shares_candidates if x is not None), None)
        return eps, bvps, shares

    def _last_close_from_aggs(self, aggs: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(aggs, dict):
            return None
        results = aggs.get("results") or []
        if not isinstance(results, list) or not results:
            return None
        return self._to_float((results[-1] or {}).get("c"))

    def _sum_volume_from_aggs(self, aggs: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(aggs, dict):
            return None
        results = aggs.get("results") or []
        if not isinstance(results, list) or not results:
            return None
        total = 0.0
        found = False
        for row in results:
            v = self._to_float((row or {}).get("v"))
            if v is not None:
                total += v
                found = True
        return total if found else None

    def _nested_value(self, node: Dict[str, Any], key: str, value_key: str) -> Optional[float]:
        try:
            sub = (node or {}).get(key) or {}
            return self._to_float(sub.get(value_key))
        except Exception:
            return None

    def _extract_balance_estimates(
        self,
        financials: Optional[Dict[str, Any]],
    ) -> tuple[Optional[float], Optional[float]]:
        if not isinstance(financials, dict):
            return None, None
        results = financials.get("results") or []
        if not isinstance(results, list) or not results:
            return None, None
        item = results[0] or {}
        node = item.get("financials") or {}
        bs = node.get("balance_sheet") or {}

        debt_candidates = [
            self._nested_value(bs, "total_debt", "value"),
            self._nested_value(bs, "long_term_debt", "value"),
            self._nested_value(bs, "short_term_debt", "value"),
        ]
        debt = next((x for x in debt_candidates if x is not None), None)
        cash_candidates = [
            self._nested_value(bs, "cash_and_cash_equivalents", "value"),
            self._nested_value(bs, "cash_and_cash_equivalents_at_carrying_value", "value"),
            self._nested_value(bs, "cash_cash_equivalents_and_short_term_investments", "value"),
        ]
        cash = next((x for x in cash_candidates if x is not None), None)
        return debt, cash

    def _safe_get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        q = dict(params or {})
        q["apiKey"] = self.api_key
        try:
            response = requests.get(url, params=q, timeout=20)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return {}
        except Exception:
            return {}

    def _disabled_payload(
        self,
        ticker: str,
        reason: str,
        start_year: int,
        end_year: int,
    ) -> Dict[str, Any]:
        periods = {str(y): {"fields": {}} for y in range(start_year, end_year + 1)}
        return {
            "layer": "MARKET",
            "status": reason,
            "ticker": ticker,
            "source": "Polygon.io",
            "source_endpoint": self.source_endpoint,
            "periods": periods,
            "data_source_trace": {"ticker": ticker, "endpoint": self.source_endpoint},
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"depends_on": [], "provides": ["MARKET"]},
        }

    def _write_output(self, payload: Dict[str, Any]) -> None:
        out = self.output_dir / "structured_market_data.json"
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None
