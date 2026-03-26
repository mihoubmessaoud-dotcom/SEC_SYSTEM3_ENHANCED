class CorporateActionsEngine:
    ACTIONS_DB = {
        "NVDA": [
            {"date": "2021-07-20", "type": "split", "factor": 4},
            {"date": "2024-06-10", "type": "split", "factor": 10},
        ],
        "AMD": [{"date": "1999-08-02", "type": "split", "factor": 2}],
        "AAPL": [
            {"date": "2014-06-09", "type": "split", "factor": 7},
            {"date": "2020-08-31", "type": "split", "factor": 4},
        ],
        "TSLA": [
            {"date": "2020-08-31", "type": "split", "factor": 5},
            {"date": "2022-08-25", "type": "split", "factor": 3},
        ],
        "GOOGL": [{"date": "2022-07-18", "type": "split", "factor": 20}],
        "AMZN": [{"date": "2022-06-06", "type": "split", "factor": 20}],
        "META": [],
        "MSFT": [],
        "JPM": [],
        "BAC": [],
        "KO": [],
        "XOM": [],
        "PRU": [],
        "AON": [],
        "MS": [],
        "AIG": [],
    }

    def get_split_factor(self, ticker: str, year: int) -> float:
        actions = self.ACTIONS_DB.get(str(ticker or "").upper(), [])
        factor = 1.0
        for action in actions:
            action_year = int(action["date"][:4])
            if action_year > year and action["type"] == "split":
                factor *= action["factor"]
        return factor

    def normalize_market_cap(self, ticker: str, year: int, raw_market_cap: float, audit: object) -> dict:
        if raw_market_cap is None:
            return {"value": None, "corrected": False}

        if raw_market_cap < 100:
            factor = self.get_split_factor(ticker, year)
            corrected = raw_market_cap * factor * 1000
            if audit is not None:
                audit.correction(year, "market_cap", raw_market_cap, corrected, "unit_slip_corrected")
            return {
                "value": corrected,
                "raw_value": raw_market_cap,
                "corrected": True,
                "factor_applied": factor,
            }

        return {
            "value": raw_market_cap,
            "raw_value": raw_market_cap,
            "corrected": False,
        }
