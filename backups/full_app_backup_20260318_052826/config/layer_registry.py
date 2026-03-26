"""Layer registry and dependency-injected layer construction.

Layer responsibilities:
- SEC (Layer 1): Official financial statements and filings from EDGAR XBRL.
- MARKET (Layer 2): Trading/valuation data from Polygon.io.
- MACRO (Layer 3): Macroeconomic indicators from FRED.
- YAHOO (Layer 4): Real-time/estimate/analyst market data from Yahoo Finance.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from layers.macro_layer import MacroLayer
from layers.market_layer import MarketLayer
from layers.sec_layer import SECLayer
from layers.yahoo_layer import YahooLayer


def build_layer_registry(
    user_agent: str,
    polygon_api_key: Optional[str],
    fred_api_key: Optional[str],
    output_dir: str,
    enable_sec: bool = True,
    enable_market: bool = True,
    enable_macro: bool = True,
    enable_yahoo: bool = True,
    extra_layers: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build independent layer instances keyed by layer name."""
    registry: Dict[str, Any] = {}
    if enable_sec:
        registry["SEC"] = SECLayer(user_agent=user_agent, output_dir=output_dir)
    if enable_market:
        registry["MARKET"] = MarketLayer(api_key=polygon_api_key, output_dir=output_dir)
    if enable_macro:
        registry["MACRO"] = MacroLayer(api_key=fred_api_key, output_dir=output_dir)
    if enable_yahoo:
        registry["YAHOO"] = YahooLayer(output_dir=output_dir)
    for layer_name, layer_instance in (extra_layers or {}).items():
        if layer_name and layer_instance is not None:
            registry[str(layer_name).upper()] = layer_instance
    return registry
