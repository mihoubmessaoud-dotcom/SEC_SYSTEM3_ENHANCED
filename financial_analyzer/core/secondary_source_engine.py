import math


def _to_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip()
    if not text or text in {"—", "-", "N/A", "nan", "None"}:
        return None
    text = text.replace(",", "")
    if text.endswith("%"):
        try:
            return float(text[:-1]) / 100.0
        except ValueError:
            return None
    try:
        return float(text)
    except ValueError:
        return None


class SecondarySourceEngine:
    """
    Resolves fallback values from secondary sheets when SEC primary fields
    are missing. The engine never overwrites valid SEC direct values.
    """

    RATIO_FALLBACK_MAP = {
        "gross_margin": ["gross_margin"],
        "operating_margin": ["operating_margin"],
        "net_margin": ["net_margin", "net_margin_core"],
        "roe": ["roe"],
        "roic": ["roic"],
        "inventory_days": ["inventory_days"],
        "ar_days": ["ar_days", "days_sales_outstanding"],
        "ap_days": ["ap_days"],
        "free_cash_flow": ["free_cash_flow"],
        "book_value_per_share": ["book_value_per_share", "bvps"],
    }

    STRATEGIC_FALLBACK_MAP = {
        "Altman_Z_Score": ["Altman_Z_Score", "altman_z_score"],
        "Economic_Spread": ["Economic_Spread", "economic_spread"],
        "ROIC": ["ROIC", "roic"],
        "WACC": ["WACC", "wacc"],
    }

    MARKET_FALLBACK_MAP = {
        "market_cap": ["market_cap", "market:market_cap"],
        "shares_outstanding": ["shares_outstanding", "market:shares_outstanding", "yahoo:shares_outstanding"],
    }

    def __init__(self, sheets):
        self.sheets = sheets
        self.ratios_idx = self._build_metric_year_index("Ratios")
        self.strategic_idx = self._build_metric_year_index("Strategic")
        self.market_idx = self._build_metric_year_index("Layer2_Market")
        self.yahoo_idx = self._build_metric_year_index("Layer4_Yahoo")

    def _build_metric_year_index(self, sheet_name):
        sheet = self.sheets.get(sheet_name)
        if sheet is None or sheet.empty:
            return {}
        out = {}
        metric_col = sheet.columns[0]
        year_cols = []
        for col in sheet.columns[1:]:
            try:
                year_cols.append((col, int(str(col).strip())))
            except ValueError:
                continue
        for _, row in sheet.iterrows():
            metric = str(row[metric_col]).strip().lower()
            if not metric or metric == "nan":
                continue
            for col, year in year_cols:
                val = _to_float(row[col])
                if val is None:
                    continue
                out.setdefault(metric, {})[year] = val
        return out

    def _lookup(self, index, keys, year):
        for key in keys:
            values = index.get(key.lower())
            if not values:
                continue
            if year in values:
                return values[year], key
        return None, None

    def fill_year_inputs(self, year, yr_data, audit):
        """
        Fill missing raw inputs needed by ratio calculations from secondary sources.
        """
        filled = dict(yr_data or {})
        for canonical, candidates in self.RATIO_FALLBACK_MAP.items():
            if filled.get(canonical) is not None:
                continue
            val, src_metric = self._lookup(self.ratios_idx, candidates, year)
            if val is None:
                continue
            filled[canonical] = val
            if audit is not None:
                audit.correction(
                    year,
                    canonical,
                    None,
                    val,
                    f"secondary_ratios:{src_metric}",
                )
        return filled

    def get_strategic_value(self, year, metric_name):
        candidates = self.STRATEGIC_FALLBACK_MAP.get(metric_name, [metric_name])
        val, _ = self._lookup(self.strategic_idx, candidates, year)
        return val

    def get_market_value(self, year, metric_name):
        candidates = self.MARKET_FALLBACK_MAP.get(metric_name, [metric_name])
        val, _ = self._lookup(self.market_idx, candidates, year)
        if val is not None:
            return val
        val, _ = self._lookup(self.yahoo_idx, candidates, year)
        return val
