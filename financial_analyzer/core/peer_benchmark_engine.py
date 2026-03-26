PEER_UNIVERSE = {
    "semiconductor_fabless": ["NVDA", "AMD", "QCOM", "AVGO", "MRVL", "ARM", "TSM"],
    "semiconductor_idm": ["INTC", "TXN", "MCHP", "STM"],
    "software_saas": ["MSFT", "ORCL", "CRM", "SNOW", "ADBE", "NOW", "GOOGL"],
    "hardware_platform": ["AAPL", "DELL", "HPQ", "MSFT"],
    "internet_platform": ["GOOGL", "META", "AMZN", "MSFT"],
    "commercial_bank": ["JPM", "BAC", "WFC", "C", "USB", "PNC"],
    "investment_bank": ["MS", "GS", "BX", "KKR"],
    "insurance_pc": ["PGR", "TRV", "ALL", "CB", "AIG"],
    "insurance_life": ["PRU", "MET", "LNC", "AFL", "GL"],
    "insurance_broker": ["AON", "MMC", "WTW", "RYAN"],
    "integrated_oil": ["XOM", "CVX", "COP", "BP"],
    "consumer_staples": ["KO", "PEP", "PG", "CL", "KMB"],
    "ev_automaker": ["TSLA", "GM", "F", "RIVN"],
}

PEER_METRICS_BY_SECTOR = {
    "semiconductor_fabless": ["gross_margin", "operating_margin", "roic", "pe_ratio", "fcf_yield"],
    "commercial_bank": ["nim", "roe", "roa", "pe_ratio", "pb_ratio"],
    "investment_bank": ["roe", "roa", "pe_ratio", "pb_ratio"],
    "insurance_life": ["roe", "net_margin", "fcf_yield", "pb_ratio"],
    "insurance_pc": ["combined_ratio", "roe", "fcf_yield", "pb_ratio"],
    "insurance_broker": ["net_margin", "fcf_yield", "pe_ratio", "revenue_cagr"],
    "integrated_oil": ["roic", "fcf_yield", "operating_margin", "pe_ratio"],
    "consumer_staples": ["gross_margin", "roic", "operating_margin", "pe_ratio", "dividend_yield"],
    "default": ["gross_margin", "operating_margin", "roic", "pe_ratio", "fcf_yield"],
}


class PeerBenchmarkEngine:
    def get_peers(self, ticker: str, sub_sector: str, loaded_tickers: list) -> dict:
        universe = PEER_UNIVERSE.get(sub_sector, [])
        local = [t for t in universe if t in (loaded_tickers or []) and t != ticker]
        reference = [t for t in universe if t not in (loaded_tickers or []) and t != ticker]
        return {"local": local, "reference": reference, "all": [t for t in universe if t != ticker]}

    def get_metrics(self, sub_sector: str) -> list:
        return PEER_METRICS_BY_SECTOR.get(sub_sector, PEER_METRICS_BY_SECTOR["default"])

    def benchmark(self, ticker: str, sub_sector: str, current_ratios: dict, peers_data: dict, year: int) -> list:
        metrics = self.get_metrics(sub_sector)
        results = []
        current_year_ratios = current_ratios.get(year, {}) if isinstance(current_ratios, dict) else {}
        for metric in metrics:
            current_val_obj = current_year_ratios.get(metric)
            current_val = getattr(current_val_obj, "value", current_val_obj)
            if current_val is None:
                continue

            peer_vals = []
            for pt, pd_data in (peers_data or {}).items():
                # Accept both dict payloads and PipelineResult-like objects.
                if isinstance(pd_data, dict):
                    peer_ratios = pd_data.get("ratios", {}) or {}
                else:
                    peer_ratios = getattr(pd_data, "ratios", {}) or {}
                pv = (peer_ratios.get(year, {}) or {}).get(metric)
                pv = getattr(pv, "value", pv)
                if pv is not None:
                    peer_vals.append((pt, pv))

            if not peer_vals:
                position = "INSUFFICIENT_PEERS"
                peer_median = None
            else:
                vals = [v for _, v in peer_vals]
                peer_median = sorted(vals)[len(vals) // 2]
                higher_better = metric not in ["pe_ratio", "pb_ratio", "debt_to_equity", "combined_ratio"]
                rank = (sum(1 for v in vals if v < current_val) + 1) if higher_better else (sum(1 for v in vals if v > current_val) + 1)
                total = len(vals) + 1
                pct = rank / total
                position = "LEADER" if pct <= 0.25 else ("UPPER_TIER" if pct <= 0.50 else "LOWER_TIER")

            results.append(
                {
                    "metric": metric,
                    "current_value": current_val,
                    "peer_median": peer_median,
                    "peer_count": len(peer_vals),
                    "position": position,
                    "peers_detail": peer_vals,
                }
            )
        return results

    def benchmark_with_reference(self, ticker: str, sub_sector: str, current_ratios: dict, loaded_peers: dict, year: int, audit: object) -> list:
        metrics = self.get_metrics(sub_sector)
        results = []
        SECTOR_REFERENCE_AVERAGES = {
            "semiconductor_fabless": {
                2025: {"gross_margin": 0.52, "operating_margin": 0.22, "roic": 0.18, "pe_ratio": 35.0, "fcf_yield": 0.025},
                2024: {"gross_margin": 0.48, "operating_margin": 0.19, "roic": 0.15, "pe_ratio": 28.0, "fcf_yield": 0.022},
            },
            "semiconductor_idm": {2025: {"gross_margin": 0.42, "operating_margin": 0.10, "roic": 0.06, "pe_ratio": 18.0, "fcf_yield": 0.04}},
            "commercial_bank": {2024: {"roe": 0.12, "roa": 0.011, "nim": 0.027}},
            "insurance_life": {2024: {"roe": 0.10, "net_margin": 0.07, "fcf_yield": 0.15}},
            "insurance_broker": {2024: {"net_margin": 0.15, "fcf_yield": 0.04, "roe": 0.35}},
            "consumer_staples": {2024: {"gross_margin": 0.45, "operating_margin": 0.18, "roic": 0.12, "pe_ratio": 22.0}},
        }
        ref_years = SECTOR_REFERENCE_AVERAGES.get(sub_sector, {})
        ref_year = year if year in ref_years else (max(ref_years.keys()) if ref_years else None)
        ref_avgs = ref_years.get(ref_year, {}) if ref_year is not None else {}

        current_year_ratios = current_ratios.get(year, {}) if isinstance(current_ratios, dict) else {}
        for metric in metrics:
            current_val_obj = current_year_ratios.get(metric)
            current_val = getattr(current_val_obj, "value", current_val_obj)
            if current_val is None:
                continue

            peer_vals = []
            for pt, pd_data in (loaded_peers or {}).items():
                if pt == ticker:
                    continue
                if isinstance(pd_data, dict):
                    peer_ratios = pd_data.get("ratios", {}) or {}
                else:
                    peer_ratios = getattr(pd_data, "ratios", {}) or {}
                pv = (peer_ratios.get(year, {}) or {}).get(metric)
                pv = getattr(pv, "value", pv)
                if pv is not None:
                    peer_vals.append((pt, pv, "local"))

            use_reference = False
            ref_avg = ref_avgs.get(metric)
            if len(peer_vals) < 2 and ref_avg is not None:
                peer_vals.append((f"متوسط {sub_sector}", ref_avg, "reference"))
                use_reference = True

            if not peer_vals:
                results.append({"metric": metric, "position": "INSUFFICIENT_PEERS", "note": "لا أقران متاحون"})
                continue

            vals = [v for _, v, _ in peer_vals]
            peer_median = sorted(vals)[len(vals) // 2]
            higher_better = metric not in ["pe_ratio", "pb_ratio", "debt_to_equity", "combined_ratio"]
            rank = (sum(1 for v in vals if v < current_val) + 1) if higher_better else (sum(1 for v in vals if v > current_val) + 1)
            pct = rank / (len(vals) + 1)
            position = "LEADER" if pct <= 0.25 else ("UPPER_TIER" if pct <= 0.50 else "LOWER_TIER")

            results.append(
                {
                    "metric": metric,
                    "current_value": current_val,
                    "peer_median": peer_median,
                    "peer_count": len(peer_vals),
                    "position": position,
                    "source": "reference_avg" if use_reference else "local_peers",
                    "note": "مقارنة بمتوسط القطاع" if use_reference else "",
                }
            )
        return results
