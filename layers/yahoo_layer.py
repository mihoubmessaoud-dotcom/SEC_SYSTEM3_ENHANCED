"""Yahoo Finance data layer (Layer 4).

Purpose:
- Pull market-side inputs that complement SEC accounting data.
- Provide real-time and forward-looking fields used by ratio and strategy engines.
- Keep source metadata for every value to preserve traceability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Dict, Optional


try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None


@dataclass(frozen=True)
class LayerOutput:
    """Standardized payload wrapper for layer outputs."""

    payload: Dict[str, Any]


class YahooLayer:
    """Fetch market/estimate/analyst data exclusively from Yahoo Finance."""

    def __init__(self, output_dir: str = "outputs") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch(
        self,
        ticker: str,
        start_year: int,
        end_year: int,
        fiscal_period_end_by_year: Optional[Dict[int, str]] = None,
    ) -> LayerOutput:
        """Fetch Yahoo data and return normalized multi-year payload."""
        if yf is None:
            payload = self._disabled_payload(
                ticker=ticker,
                reason="MISSING_DEPENDENCY",
                start_year=start_year,
                end_year=end_year,
            )
            self._write_output(payload)
            return LayerOutput(payload=payload)

        info = {}
        history = None
        benchmark_history = None
        dividends = None
        recommendations = None
        earnings_estimate = None
        balance_sheet = None
        financials = None
        split_series = None

        last_error: Optional[Exception] = None
        fetch_ok = False
        for attempt in range(1, 4):
            try:
                tk = yf.Ticker(ticker)
                info = tk.info or {}
                if not info:
                    raise ValueError("EMPTY_YAHOO_INFO")
                history = tk.history(period="max", auto_adjust=False)
                try:
                    benchmark_history = yf.Ticker("^GSPC").history(
                        period="max", auto_adjust=False
                    )
                except Exception:
                    benchmark_history = None
                dividends = tk.dividends
                split_series = tk.splits
                recommendations = tk.recommendations
                earnings_estimate = getattr(tk, "earnings_estimate", None)
                balance_sheet = getattr(tk, "balance_sheet", None)
                financials = getattr(tk, "financials", None)
                fetch_ok = True
                break
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(float(attempt))
                    continue

        if not fetch_ok:
            payload = self._disabled_payload(
                ticker=ticker,
                reason="SOURCE_FETCH_FAILED",
                start_year=start_year,
                end_year=end_year,
            )
            payload["data_source_trace"]["last_error"] = str(last_error) if last_error else None
            self._write_output(payload)
            return LayerOutput(payload=payload)

        periods: Dict[str, Any] = {}
        for year in range(start_year, end_year + 1):
            ystr = str(year)
            fields: Dict[str, Any] = {}
            is_latest_year = year == end_year
            fiscal_period_end = None
            if isinstance(fiscal_period_end_by_year, dict):
                fiscal_period_end = fiscal_period_end_by_year.get(year)
                if isinstance(fiscal_period_end, str):
                    fiscal_period_end = fiscal_period_end.strip()
                else:
                    fiscal_period_end = None
            year_close = None
            if fiscal_period_end:
                year_close = self._close_on_or_before_date(history, fiscal_period_end)
            if year_close is None:
                year_close = self._last_close_for_year(history, year)
            year_volume = self._sum_volume_for_year(history, year)
            year_return = self._total_return_for_year(history, year)
            year_dividend_yield = self._dividend_yield_for_year(dividends, year, year_close)
            split_latest_ratio = self._latest_split_ratio_after_year(split_series, year)
            eps_estimate = self._eps_estimate_for_year(earnings_estimate, year)
            analyst_rating = self._analyst_rating(recommendations)

            if year_close is not None:
                fields["yahoo:price"] = self._field(
                    year_close,
                    "USD/share",
                    year,
                    period_end=(fiscal_period_end or f"{year}-12-31"),
                )
            if year_volume is not None:
                fields["yahoo:volume"] = self._field(year_volume, "shares", year)
            if year_return is not None:
                fields["yahoo:total_return"] = self._field(year_return, "ratio", year)
            if year_dividend_yield is not None:
                fields["yahoo:dividend_yield"] = self._field(year_dividend_yield, "ratio", year)
            if split_latest_ratio is not None and split_latest_ratio > 1.0:
                fields["yahoo:split_latest_ratio"] = self._field(split_latest_ratio, "ratio", year)

            beta = self._beta_for_year(history, benchmark_history, year, window_months=36)
            if beta is None:
                beta = self._to_float(info.get("beta"))
            if beta is not None:
                fields["yahoo:beta"] = self._field(beta, "ratio", year)

            shares_outstanding_info = self._to_float(
                info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
            )
            shares_outstanding_stmt = self._statement_value(
                balance_sheet,
                year,
                [
                    "Ordinary Shares Number",
                    "Share Issued",
                    "Common Stock Shares Outstanding",
                    "Share Issued",
                ],
            )
            # Historical prices from Yahoo are split-adjusted. Some statement-share
            # series are already split-adjusted while others are not. Apply split
            # uplift only when it is closer to the live shares anchor.
            if (
                shares_outstanding_stmt is not None
                and split_latest_ratio is not None
                and split_latest_ratio > 1.0
            ):
                stmt_raw = float(shares_outstanding_stmt)
                stmt_split = stmt_raw * float(split_latest_ratio)
                anchor = shares_outstanding_info
                if anchor is None:
                    # Conservative fallback: only apply if the raw value is clearly
                    # pre-split scale (very small for mega caps).
                    if stmt_raw < 10_000_000_000:
                        shares_outstanding_stmt = stmt_split
                else:
                    try:
                        raw_gap = abs(stmt_raw - anchor) / max(abs(anchor), 1.0)
                        split_gap = abs(stmt_split - anchor) / max(abs(anchor), 1.0)
                        if split_gap + 0.10 < raw_gap:
                            shares_outstanding_stmt = stmt_split
                    except Exception:
                        pass
            shares_outstanding = (
                shares_outstanding_stmt
                if shares_outstanding_stmt is not None
                else shares_outstanding_info
            )
            market_cap = None
            if year_close is not None and shares_outstanding is not None:
                market_cap = year_close * shares_outstanding
            if market_cap is None:
                # Snapshot fallback is only safe for latest requested year.
                market_cap = self._to_float(info.get("marketCap")) if is_latest_year else None
            if market_cap is not None:
                fields["yahoo:market_cap"] = self._field(market_cap, "USD", year)

            if shares_outstanding is not None:
                fields["yahoo:shares_outstanding"] = self._field(
                    shares_outstanding, "shares", year
                )

            total_debt_stmt = self._statement_value(
                balance_sheet,
                year,
                [
                    "Total Debt",
                    "Long Term Debt",
                    "Current Debt And Capital Lease Obligation",
                    "Current Debt",
                ],
            )
            cash_stmt = self._statement_value(
                balance_sheet,
                year,
                [
                    "Cash And Cash Equivalents",
                    "Cash Cash Equivalents And Short Term Investments",
                    "Cash Financial",
                ],
            )
            enterprise_value = None
            if None not in (market_cap, total_debt_stmt, cash_stmt):
                enterprise_value = market_cap + total_debt_stmt - cash_stmt
            if enterprise_value is None:
                enterprise_value = self._to_float(info.get("enterpriseValue")) if is_latest_year else None
            if enterprise_value is not None:
                fields["yahoo:enterprise_value"] = self._field(enterprise_value, "USD", year)

            total_debt = (
                total_debt_stmt
                if total_debt_stmt is not None
                else (self._to_float(info.get("totalDebt")) if is_latest_year else None)
            )
            if total_debt is not None:
                fields["yahoo:total_debt"] = self._field(total_debt, "USD", year)

            eps_ttm = self._to_float(info.get("trailingEps")) if is_latest_year else None
            if eps_ttm is not None:
                fields["yahoo:eps_ttm"] = self._field(eps_ttm, "USD/share", year)

            trailing_pe = self._to_float(info.get("trailingPE")) if is_latest_year else None
            if trailing_pe is not None:
                fields["yahoo:trailing_pe"] = self._field(trailing_pe, "ratio", year)
            if year_close is not None and eps_ttm not in (None, 0):
                fields["yahoo:pe_ratio"] = self._field(year_close / eps_ttm, "ratio", year)

            forward_pe = self._to_float(info.get("forwardPE")) if is_latest_year else None
            if forward_pe is not None:
                fields["yahoo:forward_pe"] = self._field(forward_pe, "ratio", year)

            price_to_book = self._to_float(info.get("priceToBook")) if is_latest_year else None
            if price_to_book is not None:
                fields["yahoo:price_to_book"] = self._field(price_to_book, "ratio", year)
            equity_stmt = self._statement_value(
                balance_sheet,
                year,
                [
                    "Stockholders Equity",
                    "Total Equity Gross Minority Interest",
                    "Total Equity",
                ],
            )
            book_value_per_share = None
            if equity_stmt is not None and shares_outstanding not in (None, 0):
                book_value_per_share = equity_stmt / shares_outstanding
            if book_value_per_share is None:
                book_value_per_share = self._to_float(info.get("bookValue")) if is_latest_year else None
            if book_value_per_share is not None:
                fields["yahoo:book_value_per_share"] = self._field(book_value_per_share, "USD/share", year)
            if year_close is not None and book_value_per_share not in (None, 0):
                fields["yahoo:pb_ratio"] = self._field(year_close / book_value_per_share, "ratio", year)

            dividend_rate = self._to_float(info.get("dividendRate")) if is_latest_year else None
            if dividend_rate is not None:
                fields["yahoo:dividend_rate"] = self._field(dividend_rate, "USD/share", year)

            payout_ratio = self._to_float(info.get("payoutRatio")) if is_latest_year else None
            if payout_ratio is not None:
                fields["yahoo:payout_ratio"] = self._field(payout_ratio, "ratio", year)

            operating_cf = self._to_float(info.get("operatingCashflow")) if is_latest_year else None
            if operating_cf is not None:
                fields["yahoo:operating_cash_flow"] = self._field(operating_cf, "USD", year)

            free_cf = self._to_float(info.get("freeCashflow")) if is_latest_year else None
            if free_cf is not None:
                fields["yahoo:free_cash_flow"] = self._field(free_cf, "USD", year)

            recommendation_mean = self._to_float(info.get("recommendationMean")) if is_latest_year else None
            if recommendation_mean is not None:
                fields["yahoo:recommendation_mean"] = self._field(recommendation_mean, "score", year)

            if eps_estimate is not None:
                fields["yahoo:eps_estimate"] = self._field(eps_estimate, "USD/share", year)

            if analyst_rating is not None:
                fields["yahoo:analyst_rating"] = self._field(analyst_rating, "score", year)

            # Mirror critical keys into MARKET namespace for consistent downstream consumption.
            for src_key, dst_key in {
                "yahoo:price": "market:price",
                "yahoo:market_cap": "market:market_cap",
                "yahoo:enterprise_value": "market:enterprise_value",
                "yahoo:total_debt": "market:total_debt",
                "yahoo:beta": "market:beta",
                "yahoo:dividend_yield": "market:dividend_yield",
                "yahoo:volume": "market:volume",
                "yahoo:total_return": "market:total_return",
                "yahoo:pe_ratio": "market:pe_ratio",
                "yahoo:pb_ratio": "market:pb_ratio",
                "yahoo:split_latest_ratio": "market:split_latest_ratio",
            }.items():
                if src_key in fields and dst_key not in fields:
                    fields[dst_key] = dict(fields[src_key])

            periods[ystr] = {"fields": fields}

        payload = {
            "layer": "YAHOO",
            "status": "OK",
            "ticker": ticker,
            "source": "Yahoo Finance",
            "source_endpoint": "yfinance",
            "periods": periods,
            "data_source_trace": {
                "ticker": ticker,
                "provider": "yfinance",
                "info_keys": sorted(list(info.keys()))[:50],
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"depends_on": [], "provides": ["YAHOO"]},
        }
        self._write_output(payload)
        return LayerOutput(payload=payload)

    def _disabled_payload(
        self,
        ticker: str,
        reason: str,
        start_year: int,
        end_year: int,
    ) -> Dict[str, Any]:
        periods = {str(y): {"fields": {}} for y in range(start_year, end_year + 1)}
        return {
            "layer": "YAHOO",
            "status": reason,
            "ticker": ticker,
            "source": "Yahoo Finance",
            "source_endpoint": "yfinance",
            "periods": periods,
            "data_source_trace": {"ticker": ticker, "provider": "yfinance"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"depends_on": [], "provides": ["YAHOO"]},
        }

    def _field(
        self,
        value: float,
        unit: str,
        year: int,
        period_end: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "value": float(value),
            "unit": unit,
            "period_end": period_end or f"{year}-12-31",
            "source": "Yahoo Finance",
            "as_of": datetime.now(timezone.utc).isoformat(),
        }

    def _close_on_or_before_date(self, history: Any, period_end: str) -> Optional[float]:
        try:
            if history is None or len(history) == 0 or not period_end:
                return None
            idx = history.index
            target = None
            try:
                target = idx.__class__([period_end])[0]
            except Exception:
                try:
                    import pandas as pd  # type: ignore
                    target = pd.Timestamp(period_end)
                except Exception:
                    target = None
            if target is None:
                return None
            subset = history.loc[history.index <= target]
            if subset is None or len(subset) == 0:
                return None
            close_series = subset["Close"].dropna()
            if close_series is None or len(close_series) == 0:
                return None
            return self._to_float(close_series.iloc[-1])
        except Exception:
            return None

    def _last_close_for_year(self, history: Any, year: int) -> Optional[float]:
        try:
            year_data = history.loc[str(year)]
            if year_data is None or len(year_data) == 0:
                return None
            return self._to_float(year_data["Close"].dropna().iloc[-1])
        except Exception:
            return None

    def _sum_volume_for_year(self, history: Any, year: int) -> Optional[float]:
        try:
            year_data = history.loc[str(year)]
            if year_data is None or len(year_data) == 0:
                return None
            return self._to_float(year_data["Volume"].fillna(0).sum())
        except Exception:
            return None

    def _total_return_for_year(self, history: Any, year: int) -> Optional[float]:
        try:
            year_data = history.loc[str(year)]
            if year_data is None or len(year_data) < 2:
                return None
            close_series = year_data["Close"].dropna()
            if len(close_series) < 2:
                return None
            first_close = self._to_float(close_series.iloc[0])
            last_close = self._to_float(close_series.iloc[-1])
            if first_close is None or last_close is None or first_close == 0:
                return None
            return (last_close / first_close) - 1.0
        except Exception:
            return None

    def _dividend_yield_for_year(self, dividends: Any, year: int, close_price: Optional[float]) -> Optional[float]:
        if close_price is None or close_price <= 0:
            return None
        try:
            div_series = dividends.loc[str(year)]
            if div_series is None:
                return None
            total_div = self._to_float(div_series.sum())
            if total_div is None or total_div <= 0:
                return None
            return total_div / close_price
        except Exception:
            return None

    def _latest_split_ratio_after_year(self, split_series: Any, year: int) -> Optional[float]:
        """Return the latest split ratio strictly after a given year."""
        try:
            if split_series is None or len(split_series) == 0:
                return None
            latest_ratio = None
            latest_key = None
            for idx, raw in split_series.items():
                ratio = self._to_float(raw)
                if ratio is None or ratio <= 1.0:
                    continue
                split_year = None
                try:
                    split_year = int(getattr(idx, "year", None))
                except Exception:
                    split_year = None
                if split_year is None:
                    try:
                        split_year = int(str(idx)[:4])
                    except Exception:
                        split_year = None
                if split_year is None or split_year <= year:
                    continue
                if latest_key is None or idx > latest_key:
                    latest_key = idx
                    latest_ratio = ratio
            return latest_ratio
        except Exception:
            return None

    def _eps_estimate_for_year(self, earnings_estimate: Any, year: int) -> Optional[float]:
        try:
            if earnings_estimate is None or len(earnings_estimate) == 0:
                return None
            if "avg" not in earnings_estimate.columns:
                return None
            avg = earnings_estimate["avg"].dropna()
            if len(avg) == 0:
                return None
            return self._to_float(avg.iloc[0])
        except Exception:
            return None

    def _statement_value(
        self,
        statement_df: Any,
        year: int,
        labels: list[str],
    ) -> Optional[float]:
        try:
            if statement_df is None or getattr(statement_df, "empty", True):
                return None
            cols = list(statement_df.columns)
            target_col = None
            for c in cols:
                c_year = None
                try:
                    c_year = int(getattr(c, "year", c))
                except Exception:
                    try:
                        c_year = int(str(c)[:4])
                    except Exception:
                        c_year = None
                if c_year == year:
                    target_col = c
                    break
            if target_col is None:
                return None
            idx = {str(i).strip().lower(): i for i in statement_df.index}
            for label in labels:
                key = str(label).strip().lower()
                if key not in idx:
                    continue
                raw = statement_df.loc[idx[key], target_col]
                val = self._to_float(raw)
                if val is not None:
                    return val
            return None
        except Exception:
            return None

    def _analyst_rating(self, recommendations: Any) -> Optional[float]:
        try:
            if recommendations is None or len(recommendations) == 0:
                return None
            latest = recommendations.iloc[-1]
            strong_buy = self._to_float(latest.get("strongBuy")) or 0.0
            buy = self._to_float(latest.get("buy")) or 0.0
            hold = self._to_float(latest.get("hold")) or 0.0
            sell = self._to_float(latest.get("sell")) or 0.0
            strong_sell = self._to_float(latest.get("strongSell")) or 0.0
            total = strong_buy + buy + hold + sell + strong_sell
            if total == 0:
                return None
            weighted = (
                (1.0 * strong_buy)
                + (2.0 * buy)
                + (3.0 * hold)
                + (4.0 * sell)
                + (5.0 * strong_sell)
            ) / total
            return weighted
        except Exception:
            return None

    def _beta_for_year(
        self,
        history: Any,
        benchmark_history: Any,
        year: int,
        window_months: int = 36,
    ) -> Optional[float]:
        """Compute trailing beta vs S&P 500 using monthly returns."""
        try:
            if history is None or benchmark_history is None:
                return None
            if len(history) == 0 or len(benchmark_history) == 0:
                return None

            end_ts = f"{year}-12-31"
            stock_close = history["Close"].loc[:end_ts].dropna()
            bench_close = benchmark_history["Close"].loc[:end_ts].dropna()
            if len(stock_close) < 60 or len(bench_close) < 60:
                return None

            stock_m = stock_close.resample("ME").last().dropna().pct_change().dropna()
            bench_m = bench_close.resample("ME").last().dropna().pct_change().dropna()
            if len(stock_m) == 0 or len(bench_m) == 0:
                return None

            monthly = stock_m.to_frame("stock").join(bench_m.to_frame("bench"), how="inner").dropna()
            if len(monthly) < max(12, window_months // 2):
                return None
            monthly = monthly.tail(window_months)
            if len(monthly) < 12:
                return None

            var_bench = float(monthly["bench"].var())
            if var_bench <= 0:
                return None
            cov = float(monthly["stock"].cov(monthly["bench"]))
            beta = cov / var_bench
            if beta != beta:
                return None
            return float(beta)
        except Exception:
            return None

    def _write_output(self, payload: Dict[str, Any]) -> None:
        out = self.output_dir / "structured_yahoo_data.json"
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None
