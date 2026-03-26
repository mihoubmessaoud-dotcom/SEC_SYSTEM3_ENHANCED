from __future__ import annotations


EXTENDED_PEER_UNIVERSE = {
    "hardware_platform": ["AAPL", "DELL", "HPQ", "LENOVO", "SAMSUNG", "MSFT", "GOOGL"],
    "software_saas": ["MSFT", "ORCL", "SAP", "ADBE", "CRM", "GOOGL", "AAPL"],
    "semiconductor_fabless": ["NVDA", "AMD", "QCOM", "AVGO", "MRVL", "INTC", "TSM"],
    "consumer_staples": ["KO", "PEP", "PG", "CL", "KMB", "MDLZ", "GIS", "HRL", "MKC"],
}


def get_peers(ticker, sub_sector, available_tickers):
    extended = EXTENDED_PEER_UNIVERSE.get(sub_sector, [])
    available = {str(t).upper() for t in (available_tickers or [])}
    local_peers = [t for t in extended if t in available and t != ticker]
    reference_peers = [t for t in extended if t not in available and t != ticker]
    return {
        "local": local_peers,
        "reference": reference_peers,
        "total_count": len(local_peers) + len(reference_peers),
    }

