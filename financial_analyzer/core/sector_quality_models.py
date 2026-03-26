EXPLICIT_SECTOR_OVERRIDES = {
    "INTC": "semiconductor_idm",
    "TXN": "semiconductor_idm",
    "MCHP": "semiconductor_idm",
    "STM": "semiconductor_idm",
    "GFS": "semiconductor_idm",
    "UMC": "semiconductor_idm",
    "TSM": "semiconductor_foundry",
}

SECTOR_MAP = {
    "technology": {
        "semiconductor_fabless": {"tickers": ["NVDA", "AMD", "QCOM", "AVGO", "MRVL", "ARM"], "sic_codes": ["3674"]},
        "semiconductor_idm": {
            "tickers": ["INTC", "TXN", "MCHP", "STM"],
            "sic_codes": ["3674", "3672"],
            "key_metrics": ["gross_margin", "capex_to_revenue", "roic", "operating_margin", "fcf_yield", "inventory_days"],
        },
        "semiconductor_foundry": {"tickers": ["TSM"], "sic_codes": ["3674"]},
        "software_saas": {"tickers": ["MSFT", "ORCL", "CRM", "SNOW", "ADBE", "NOW"], "sic_codes": ["7372"]},
        "hardware_platform": {"tickers": ["AAPL", "DELL", "HPQ"], "sic_codes": ["3571", "3572"]},
        "internet_platform": {"tickers": ["GOOGL", "META", "AMZN"], "sic_codes": ["7375"]},
    },
    "financial": {
        "commercial_bank": {"tickers": ["JPM", "BAC", "WFC", "C", "USB", "PNC"], "sic_codes": ["6020", "6022"]},
        "investment_bank": {"tickers": ["MS", "GS", "BX", "KKR"], "sic_codes": ["6211"]},
        "insurance_pc": {"tickers": ["PGR", "TRV", "ALL", "CB", "AIG"], "sic_codes": ["6321", "6311"]},
        "insurance_life": {"tickers": ["PRU", "MET", "LNC", "AFL", "GL"], "sic_codes": ["6311"]},
        "insurance_broker": {"tickers": ["AON", "MMC", "WTW", "RYAN"], "sic_codes": ["6411"]},
    },
    "industrial": {
        "integrated_oil": {"tickers": ["XOM", "CVX", "COP", "BP"], "sic_codes": ["1311", "2911"]},
        "consumer_staples": {"tickers": ["KO", "PEP", "PG", "CL", "KMB"], "sic_codes": ["2080", "2844"]},
        "ev_automaker": {"tickers": ["TSLA", "GM", "F", "RIVN"], "sic_codes": ["3711"]},
    },
}


QUALITY_MODELS = {
    "semiconductor_fabless": {
        "weights": {"roic": 0.30, "gross_margin": 0.20, "revenue_cagr": 0.20, "fcf_yield": 0.15, "economic_spread": 0.15},
        "thresholds": {"roic": {"excellent": 0.30, "good": 0.15, "ok": 0.08, "poor": 0.0}},
        "p_e_context": "premium_growth",
        "penalties": {},
    },
    "semiconductor_idm": {
        "weights": {
            "gross_margin": 0.25,
            "roic": 0.25,
            "operating_margin": 0.20,
            "fcf_yield": 0.15,
            "capex_efficiency": 0.15,
        },
        "thresholds": {
            "gross_margin": {"excellent": 0.55, "good": 0.45, "ok": 0.35, "poor": 0.25},
            "roic": {"excellent": 0.15, "good": 0.08, "ok": 0.03, "poor": -0.05},
        },
        "capex_penalty_override": True,
        "capex_normal_range": [0.20, 0.45],
        "correct_peers": ["INTC", "TXN", "MCHP", "STM"],
        "p_e_context": "value_cyclical",
        "penalties": {},
    },
    "software_saas": {
        "weights": {"gross_margin": 0.25, "roic": 0.25, "operating_margin": 0.20, "revenue_cagr": 0.20, "fcf_yield": 0.10},
        "thresholds": {"gross_margin": {"excellent": 0.75, "good": 0.65, "ok": 0.55, "poor": 0.45}},
        "p_e_context": "premium_growth",
        "penalties": {},
    },
    "hardware_platform": {
        "weights": {"roic": 0.30, "gross_margin": 0.20, "operating_margin": 0.20, "fcf_yield": 0.20, "economic_spread": 0.10},
        "ignore_low_current_ratio": True,
        "ignore_low_fcf_yield_if_buyback": True,
        "roic_override_threshold": 0.40,
        "penalties": {},
    },
    "commercial_bank": {
        "weights": {"roe_spread": 0.30, "nim": 0.20, "roa": 0.20, "efficiency_ratio": 0.15, "capital_ratio": 0.15},
        "fcf_blocked": True,
        "working_capital_blocked": True,
        "penalties": {"fraud_flag_tolerance": 10},
    },
    "investment_bank": {
        "weights": {"roe": 0.35, "roa": 0.20, "net_margin_proxy": 0.20, "capital_ratio": 0.15, "revenue_cagr": 0.10},
        "fcf_blocked": True,
        "working_capital_blocked": True,
        "failure_prob_cap": 0.15,
        "penalties": {},
    },
    "insurance_life": {
        "weights": {"roe": 0.30, "net_margin": 0.25, "fcf_yield": 0.25, "solvency_proxy": 0.20},
        "combined_ratio_blocked": True,
        "roic_fallback": "roe",
        "penalties": {},
    },
    "insurance_broker": {
        "weights": {"organic_growth": 0.30, "fcf_margin": 0.25, "net_margin": 0.25, "retention_proxy": 0.20},
        "combined_ratio_blocked": True,
        "failure_prob_cap_buyback": 0.12,
        "penalties": {},
    },
    "consumer_staples": {
        "weights": {"roic": 0.25, "gross_margin": 0.20, "operating_margin": 0.20, "dividend_sustainability": 0.20, "fcf_yield": 0.15},
        "high_debt_normal": True,
        "mandatory_missing_tolerance": 12,
        "roic_override_threshold": 0.09,
        "penalties": {},
    },
    "integrated_oil": {
        "weights": {"roic": 0.25, "fcf_yield": 0.25, "ev_ebitda": 0.20, "operating_margin": 0.15, "reserve_proxy": 0.15},
        "cyclical_gross_margin": True,
        "penalties": {},
    },
    "ev_automaker": {
        "weights": {"gross_margin": 0.25, "revenue_cagr": 0.25, "operating_margin": 0.25, "fcf_yield": 0.15, "roic": 0.10},
        "startup_loss_tolerance": True,
        "penalties": {},
    },
}


def detect_sub_sector(ticker: str, sector_from_file: str, sic_code: str = None) -> str:
    t = str(ticker or "").upper()
    if t in EXPLICIT_SECTOR_OVERRIDES:
        return EXPLICIT_SECTOR_OVERRIDES[t]

    sic_map = {
        "6411": "insurance_broker",
        "6311": "insurance_life",
        "6321": "insurance_pc",
        "6020": "commercial_bank",
        "6022": "commercial_bank",
        "6211": "investment_bank",
        "1311": "integrated_oil",
        "2911": "integrated_oil",
        "3674": "semiconductor_fabless",
        "3672": "semiconductor_idm",
        "7372": "software_saas",
        "3571": "hardware_platform",
        "3711": "ev_automaker",
        "2080": "consumer_staples",
    }
    if sic_code and str(sic_code) in sic_map:
        return sic_map[str(sic_code)]

    for sector_data in SECTOR_MAP.values():
        for sub, data in sector_data.items():
            if t in data.get("tickers", []):
                return sub
    return sector_from_file or "unknown"
