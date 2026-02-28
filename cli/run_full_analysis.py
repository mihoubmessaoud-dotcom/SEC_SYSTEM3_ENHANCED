"""CLI runner for full multi-layer financial analysis pipeline.

Execution order:
1) SEC layer (official accounting facts)
2) MARKET layer (Polygon trading/valuation data)
3) MACRO layer (FRED macro indicators)
4) YAHOO layer (yfinance real-time/estimate/analyst data)
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List

from config.layer_registry import build_layer_registry
from core.ratio_engine import RatioEngine
from core.strategy_engine import StrategicAnalysisEngine


CIK_TO_TICKER = {
    "0000050863": "INTC",
    "0000320193": "AAPL",
    "0000021344": "KO",
}


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run full modular financial analysis")
    parser.add_argument("--ciks", nargs="+", required=True, help="List of CIKs")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--user-agent", required=True)
    parser.add_argument("--polygon-api-key", default=None)
    parser.add_argument("--fred-api-key", default=None)
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--disable-sec", action="store_true")
    parser.add_argument("--disable-market", action="store_true")
    parser.add_argument("--disable-macro", action="store_true")
    parser.add_argument("--disable-yahoo", action="store_true")
    return parser.parse_args()


def run() -> None:
    """Execute full analysis for each CIK and write JSON outputs."""
    args = parse_args()
    enable_market = (not args.disable_market) and bool(args.polygon_api_key)
    registry = build_layer_registry(
        user_agent=args.user_agent,
        polygon_api_key=args.polygon_api_key,
        fred_api_key=args.fred_api_key,
        output_dir=args.output_dir,
        enable_sec=not args.disable_sec,
        enable_market=enable_market,
        enable_macro=not args.disable_macro,
        enable_yahoo=not args.disable_yahoo,
    )

    ratio_engine = RatioEngine(layer_registry=registry)
    strategy_engine = StrategicAnalysisEngine()

    out_root = Path(args.output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    for cik in args.ciks:
        cik_padded = str(cik).zfill(10)
        ticker = CIK_TO_TICKER.get(cik_padded, cik_padded)
        cik_dir = out_root / cik_padded
        cik_dir.mkdir(parents=True, exist_ok=True)

        layer_payloads: Dict[str, Dict[str, Any]] = {}

        if "SEC" in registry:
            layer_payloads["SEC"] = registry["SEC"].fetch(
                cik=cik_padded,
                start_year=args.start_year,
                end_year=args.end_year,
            ).payload

        if "MARKET" in registry:
            layer_payloads["MARKET"] = registry["MARKET"].fetch(
                ticker=ticker,
                start_year=args.start_year,
                end_year=args.end_year,
            ).payload

        if "MACRO" in registry:
            layer_payloads["MACRO"] = registry["MACRO"].fetch(
                start_year=args.start_year,
                end_year=args.end_year,
            ).payload

        if "YAHOO" in registry:
            layer_payloads["YAHOO"] = registry["YAHOO"].fetch(
                ticker=ticker,
                start_year=args.start_year,
                end_year=args.end_year,
            ).payload

        # Fallback bridge: if MARKET is disabled/unavailable, map Yahoo fields to market namespace
        # so ratio/strategy engines can still consume compatible inputs.
        if "MARKET" not in layer_payloads and "YAHOO" in layer_payloads:
            yahoo_payload = layer_payloads["YAHOO"]
            mapped_periods: Dict[str, Any] = {}
            for year_key, year_obj in (yahoo_payload.get("periods", {}) or {}).items():
                y_fields = (year_obj or {}).get("fields", {}) or {}
                mapped_fields: Dict[str, Any] = {}
                alias = {
                    "yahoo:price": "market:price",
                    "yahoo:market_cap": "market:market_cap",
                    "yahoo:enterprise_value": "market:enterprise_value",
                    "yahoo:beta": "market:beta",
                    "yahoo:dividend_yield": "market:dividend_yield",
                    "yahoo:volume": "market:volume",
                    "yahoo:total_return": "market:total_return",
                }
                for src, dst in alias.items():
                    if src in y_fields:
                        mapped_fields[dst] = y_fields[src]
                mapped_periods[year_key] = {"fields": mapped_fields}

            layer_payloads["MARKET"] = {
                "layer": "MARKET",
                "status": "OK",
                "source": "Yahoo-Mapped",
                "source_endpoint": "yfinance",
                "periods": mapped_periods,
                "data_source_trace": {
                    "mapped_from": "YAHOO",
                    "ticker": ticker,
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dependency_map": {"depends_on": ["YAHOO"], "provides": ["MARKET"]},
            }

        full_ratios: Dict[str, Any] = {}
        full_raw_inputs: Dict[str, Any] = {}
        full_decision_tree: Dict[str, Any] = {}
        full_strategy: Dict[str, Any] = {}

        for year in range(args.start_year, args.end_year + 1):
            result = ratio_engine.compute_all(layer_payloads=layer_payloads, year=year)
            full_ratios[str(year)] = result["ratio_results"]
            full_raw_inputs[str(year)] = result["raw_inputs"]
            full_decision_tree[str(year)] = result["na_decision_tree_report"]
            full_strategy[str(year)] = strategy_engine.analyze(
                ratio_results=result["ratio_results"],
                layer_payloads=layer_payloads,
                year=year,
            )

        ratio_output = {
            "cik": cik_padded,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "results": full_ratios,
        }
        raw_output = {
            "cik": cik_padded,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "raw_inputs": full_raw_inputs,
        }
        decision_output = {
            "cik": cik_padded,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "na_decision_tree_report": full_decision_tree,
        }
        strategy_output = {
            "cik": cik_padded,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "strategy_results": full_strategy,
        }

        (cik_dir / "ratio_results.json").write_text(
            json.dumps(ratio_output, indent=2),
            encoding="utf-8",
        )
        (cik_dir / "raw_inputs.json").write_text(
            json.dumps(raw_output, indent=2),
            encoding="utf-8",
        )
        (cik_dir / "na_decision_tree_report.json").write_text(
            json.dumps(decision_output, indent=2),
            encoding="utf-8",
        )
        (cik_dir / "strategy_results.json").write_text(
            json.dumps(strategy_output, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    run()
