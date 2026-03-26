import math
import pandas as pd

from financial_analyzer.core.canonical_label_map import CanonicalLabelResolver
from financial_analyzer.core.corporate_actions_engine import CorporateActionsEngine
from financial_analyzer.core.data_anchor_validator import DataAnchorValidator
from financial_analyzer.core.revenue_policy_engine import RevenuePolicyEngine
from financial_analyzer.core.ratio_engine import RatioEngine
from financial_analyzer.core.ratio_engine import enforce_impossible_bounds
from financial_analyzer.core.sector_quality_models import QUALITY_MODELS, detect_sub_sector
from financial_analyzer.core.secondary_source_engine import SecondarySourceEngine
from financial_analyzer.core.strategic_engine import validate_altman_z_series
from financial_analyzer.core.academic_policy import get_data_quality_grade, get_financial_health
from financial_analyzer.core.verdict_engine import VerdictEngine
from financial_analyzer.core.peer_benchmark_engine import PeerBenchmarkEngine
from financial_analyzer.models.audit_log import AuditLog
from financial_analyzer.models.pipeline_result import PipelineResult
from financial_analyzer.models.ratio_result import RatioResult


class PipelineOrchestrator:
    DATA_QUALITY_PERIODS = {"sec_reliable_from": 2014, "market_reliable_from": 2010}
    OPTIMAL_YEAR_RANGE = {
        "default_start": 2015,
        "default_end": 2025,
        "reason": (
            "SEC EDGAR قبل 2015 ناقص هيكلياً: Liabilities مفقودة، "
            "إيرادات غير موحدة. 2015-2025 يعطي 11 سنة موثوقة بتغطية أعلى."
        ),
        "allow_override": True,
        "warn_if_before": 2015,
    }

    @classmethod
    def filter_years_by_quality(cls, all_years: list, user_start: int = None, user_end: int = None) -> dict:
        start = user_start or cls.OPTIMAL_YEAR_RANGE["default_start"]
        end = user_end or cls.OPTIMAL_YEAR_RANGE["default_end"]
        recommended = [y for y in all_years if start <= y <= end]
        extended = [y for y in all_years if y < start]
        future = [y for y in all_years if y > end]
        return {
            "recommended_years": recommended,
            "extended_years": extended,
            "future_years": future,
            "scoring_years": recommended,
            "display_years": all_years,
            "quality_note": (
                f"يُستخدم {start}-{end} للتقييم. البيانات قبل {start} معروضة بموثوقية مخفضة."
                if extended
                else None
            ),
        }

    @classmethod
    def classify_year_quality(cls, year: int) -> dict:
        if year < cls.DATA_QUALITY_PERIODS["sec_reliable_from"]:
            return {
                "quality": "LIMITED",
                "reliability": 50,
                "note": f"بيانات {year} محدودة — SEC EDGAR غير مكتمل قبل 2014",
                "use_in_scoring": False,
                "display_warning": True,
            }
        return {
            "quality": "STANDARD",
            "reliability": 80,
            "note": "",
            "use_in_scoring": True,
            "display_warning": False,
        }

    def __init__(self):
        self.label_resolver = CanonicalLabelResolver()
        self.corp_engine = CorporateActionsEngine()
        self.anchor_validator = DataAnchorValidator()
        self.revenue_engine = RevenuePolicyEngine()
        self.ratio_engine = RatioEngine()
        self.verdict_engine = VerdictEngine()
        self.peer_engine = PeerBenchmarkEngine()

    def run(self, filepath: str, loaded_tickers: dict = None, year_start: int = None, year_end: int = None) -> PipelineResult:
        sheets = pd.read_excel(filepath, sheet_name=None)
        secondary_engine = SecondarySourceEngine(sheets)
        ticker = self._get_fa(sheets, "Ticker") or self._infer_ticker_from_name(filepath)
        sector = self._get_fa(sheets, "Sector_Profile") or "unknown"
        sic = self._get_fa(sheets, "SIC_Code")
        sub_sector = detect_sub_sector(ticker, sector, sic)

        audit = AuditLog(ticker)
        audit.log("init", {"ticker": ticker, "sub_sector": sub_sector})

        raw_data = self._extract_raw(sheets)
        resolved_all = {}
        coverage_ref = 0
        for year, year_raw in raw_data.items():
            resolved = self.label_resolver.resolve_all(year_raw, sector=sub_sector)
            resolved_all[year] = resolved["data"]
            coverage_ref = resolved["coverage_pct"]
        audit.log("label_resolution", {"coverage": coverage_ref})

        mkt_data = self._extract_market(sheets)
        anchor_sheet = self._extract_balance_check(sheets)
        normalized_mkt = {}
        all_market_years = set(mkt_data.keys()) | set(raw_data.keys()) | set(anchor_sheet.keys())
        for year in sorted(all_market_years):
            mkt = mkt_data.get(year, {})
            mc_raw = mkt.get("market_cap")
            if mc_raw is None:
                mc_raw = secondary_engine.get_market_value(year, "market_cap")
            normalized_mkt[year] = self.corp_engine.normalize_market_cap(ticker, year, mc_raw, audit)
        audit.log("corporate_actions", {"corrections": len(audit.corrections)})

        balance_by_year = {}
        for year in resolved_all:
            bal = {
                "total_assets": resolved_all[year].get("total_assets"),
                "total_liabilities": resolved_all[year].get("total_liabilities"),
                "total_equity": resolved_all[year].get("total_equity"),
            }
            if year in anchor_sheet:
                for k, v in anchor_sheet[year].items():
                    if v is not None:
                        bal[k] = v
            balance_by_year[year] = bal

        anchor_results = self.anchor_validator.validate_all_years(ticker, balance_by_year, audit)
        valid_years = anchor_results["valid_years"]
        blocked_years = anchor_results["blocked_years"]
        if not valid_years:
            return PipelineResult(
                ticker=ticker,
                sub_sector=sub_sector,
                status="BLOCKED_NO_VALID_YEARS",
                audit=audit,
            )

        all_years = sorted(set(resolved_all.keys()) | set(balance_by_year.keys()))
        year_quality = {year: self.classify_year_quality(year) for year in all_years}
        year_filter = self.filter_years_by_quality(all_years, year_start, year_end)
        scoring_years = [year for year in year_filter["scoring_years"] if year in valid_years and year_quality[year]["use_in_scoring"]]
        display_years = list(year_filter["display_years"])

        revenues = {}
        for year in sorted(valid_years):
            prev_year = year - 1
            prev_rev = revenues.get(prev_year, {}).get("value")
            revenues[year] = self.revenue_engine.get_revenue_with_context(
                ticker,
                year,
                resolved_all[year],
                audit,
                prev_year_revenue=prev_rev,
            )

        ratios = {}
        ratios_sheet = sheets.get("Ratios", pd.DataFrame())
        for year in valid_years:
            yr_data = secondary_engine.fill_year_inputs(year, resolved_all[year], audit)
            anchor = balance_by_year.get(year, {})
            for k in ("total_assets", "total_liabilities", "total_equity"):
                if yr_data.get(k) is None and anchor.get(k) is not None:
                    yr_data[k] = anchor[k]

            rev = revenues[year].get("value")
            rev_inf = revenues[year].get("inferred", False)
            if rev_inf and revenues[year].get("inference_context"):
                audit.flag(year, "REVENUE_INFERRED_CONTEXT", "MEDIUM", revenues[year].get("note") or "FY context")
            eq = yr_data.get("total_equity")
            ni = yr_data.get("net_income")
            op_inc = yr_data.get("operating_income")
            roe_backup = self._get_ratio_backup(ratios_sheet, "roe", year)
            gross_backup = self._get_ratio_backup(ratios_sheet, "gross_margin", year)
            op_backup = self._get_ratio_backup(ratios_sheet, "operating_margin", year)
            net_backup = self._get_ratio_backup(ratios_sheet, "net_margin", year)

            year_ratios = {
                "gross_margin": self._ratio_or_backup(
                    "gross_margin",
                    self.ratio_engine.calc_gross_margin(yr_data, rev, ticker=ticker, year=year, audit=audit),
                    gross_backup,
                ),
                "operating_margin": self._ratio_or_backup(
                    "operating_margin",
                    self.ratio_engine.calc_operating_margin(op_inc, rev, ticker=ticker, year=year, audit=audit),
                    op_backup,
                ),
                "net_margin": self._ratio_or_backup(
                    "net_margin",
                    self.ratio_engine.calc_net_margin(ni, rev, ticker=ticker, year=year, audit=audit),
                    net_backup,
                ),
                "roe": self.ratio_engine.calc_roe(ni, eq, roe_backup, ticker, year, audit=audit),
                "roic": self._ratio_or_backup(
                    "roic",
                    RatioResult(
                        value=self._extract_metric_value("roic", year, {}, {year: {"ROIC": secondary_engine.get_strategic_value(year, "ROIC")}}),
                        status="COMPUTED" if self._extract_metric_value("roic", year, {}, {year: {"ROIC": secondary_engine.get_strategic_value(year, "ROIC")}}) is not None else "NOT_COMPUTABLE",
                        reliability=80,
                        metric="roic",
                        reason="MISSING_INPUT",
                    ),
                    self._get_ratio_backup(ratios_sheet, "roic", year),
                ),
                "fcf_yield": self.ratio_engine.calc_fcf_yield(
                    yr_data.get("free_cash_flow"),
                    normalized_mkt.get(year, {}).get("value"),
                    sub_sector,
                    ticker=ticker,
                    year=year,
                    audit=audit,
                ),
                "ccc_days": self.ratio_engine.calc_ccc(
                    yr_data.get("inventory_days"),
                    yr_data.get("ar_days"),
                    yr_data.get("ap_days"),
                    rev_inf,
                    ticker=ticker,
                    year=year,
                    audit=audit,
                ),
                "revenue": revenues[year],
            }

            dso_val = yr_data.get("ar_days")
            if dso_val is not None:
                p = enforce_impossible_bounds("days_sales_outstanding", float(dso_val), ticker, year, audit)
                year_ratios["days_sales_outstanding"] = RatioResult(
                    value=p.get("value"),
                    status="COMPUTED" if p.get("value") is not None else "NOT_COMPUTABLE",
                    reliability=75 if p.get("value") is not None else 0,
                    metric="days_sales_outstanding",
                    reason=p.get("reason") or "",
                    display=f"{p.get('value'):.1f} يوم" if p.get("value") is not None else "— (قيمة مستحيلة) ⚠️",
                    note="",
                )
            dio_val = yr_data.get("inventory_days")
            if dio_val is not None:
                p = enforce_impossible_bounds("days_inventory_outstanding", float(dio_val), ticker, year, audit)
                year_ratios["days_inventory_outstanding"] = RatioResult(
                    value=p.get("value"),
                    status="COMPUTED" if p.get("value") is not None else "NOT_COMPUTABLE",
                    reliability=75 if p.get("value") is not None else 0,
                    metric="days_inventory_outstanding",
                    reason=p.get("reason") or "",
                    display=f"{p.get('value'):.1f} يوم" if p.get("value") is not None else "— (قيمة مستحيلة) ⚠️",
                    note="",
                )

            for metric in list(year_ratios.keys()):
                if self.ratio_engine.is_blocked(metric, sub_sector):
                    year_ratios[metric] = RatioResult(
                        value=None,
                        status="NOT_COMPUTABLE",
                        reliability=0,
                        reason="BLOCKED_BY_SECTOR",
                        note=f"Not applicable for {sub_sector}",
                        metric=metric,
                    )
            ratios[year] = year_ratios

        strategic = self._extract_strategic(sheets)
        for year in valid_years:
            yr_str = strategic.setdefault(year, {})
            for metric in ("Altman_Z_Score", "Economic_Spread", "ROIC", "WACC"):
                if yr_str.get(metric) is None:
                    fallback_val = secondary_engine.get_strategic_value(year, metric)
                    if fallback_val is not None:
                        yr_str[metric] = fallback_val
                        audit.correction(year, metric, None, fallback_val, "secondary_strategic")

        altman_series = {y: (strategic.get(y, {}) or {}).get("Altman_Z_Score") for y in valid_years}
        altman_validated = validate_altman_z_series(ticker, altman_series, audit)
        for y, z in altman_validated.items():
            strategic.setdefault(y, {})["Altman_Z_Score"] = z

        # Strategic ROE computation with robust priority (ratios sheet, balance, then BVPS * shares).
        from financial_analyzer.core.strategic_engine import compute_roe_for_strategic
        for year in valid_years:
            ratios_roe = self._get_ratio_backup(ratios_sheet, "roe", year)
            bvps = secondary_engine.ratios_idx.get("book_value_per_share", {}).get(year)
            shares = secondary_engine.get_market_value(year, "shares_outstanding")
            roe_payload = compute_roe_for_strategic(
                ticker=ticker,
                year=year,
                net_income=resolved_all[year].get("net_income"),
                total_equity_balance=balance_by_year.get(year, {}).get("total_equity"),
                ratios_sheet_roe=ratios_roe,
                bvps=bvps,
                shares=shares,
            )
            strategic.setdefault(year, {})["ROE"] = roe_payload

        verdicts = {}
        for year in valid_years:
            verdicts[year] = self.verdict_engine.compute_verdict(
                ticker, year, ratios[year], strategic.get(year, {}), sub_sector, audit
            )

        quality_score, quality_verdict = self._compute_quality(
            ratios, strategic, sub_sector, ticker, scoring_years or valid_years
        )
        loaded = loaded_tickers or {}
        peers_info = self.peer_engine.get_peers(ticker, sub_sector, list(loaded.keys()))
        latest_year = max(valid_years)
        peer_benchmark = self.peer_engine.benchmark_with_reference(
            ticker,
            sub_sector,
            ratios,
            loaded,
            latest_year,
            audit,
        )

        original_score = self._get_fa_score(sheets)
        correction_alert = None
        if original_score and abs(quality_score - original_score) > 15:
            correction_alert = {
                "original": original_score,
                "corrected": quality_score,
                "diff": quality_score - original_score,
                "sub_sector": sub_sector,
            }

        prof_score = self._calc_professional_score(anchor_results, ratios, quality_score)
        coverage_pct = (len(valid_years) / max(1, len(display_years))) * 100.0
        data_quality_grade = get_data_quality_grade(coverage_pct, blocked_years, len(display_years))
        latest = max(valid_years) if valid_years else None
        latest_roic = self._extract_metric_value("roic", latest, ratios, strategic) if latest else None
        latest_altman = (strategic.get(latest, {}) or {}).get("Altman_Z_Score") if latest else None
        latest_op_margin = self._extract_metric_value("operating_margin", latest, ratios, strategic) if latest else None
        financial_health = get_financial_health(latest_roic, latest_altman, latest_op_margin, sub_sector)
        return PipelineResult(
            ticker=ticker,
            sector=sector,
            sub_sector=sub_sector,
            status="OK",
            valid_years=valid_years,
            blocked_years=blocked_years,
            ratios=ratios,
            strategic=strategic,
            verdicts=verdicts,
            quality_score=quality_score,
            quality_verdict=quality_verdict,
            professional_score=prof_score,
            scoring_years=scoring_years,
            display_years=display_years,
            year_quality=year_quality,
            data_quality_grade=data_quality_grade,
            financial_health_indicator=financial_health,
            peers={"info": peers_info, "benchmark": peer_benchmark},
            audit=audit,
            correction_alert=correction_alert,
        )

    def _compute_quality(self, ratios, strategic, sub_sector, ticker, valid_years):
        model = QUALITY_MODELS.get(sub_sector, QUALITY_MODELS.get("hardware_platform", {}))
        weights = model.get("weights", {})
        weighted_score = 0.0
        available_weight = 0.0
        total_weight = sum(weights.values()) if weights else 0.0
        latest = max(ratios.keys()) if ratios else None
        if not latest:
            return 0, "undefined"

        for metric, weight in weights.items():
            val = self._extract_metric_value(metric, latest, ratios, strategic)
            if val is None:
                continue
            metric_score = self._score_metric(metric, val, model)
            weighted_score += weight * metric_score
            available_weight += weight

        if total_weight <= 0:
            return 50, "neutral"

        if available_weight <= 0:
            score_int = 35
        else:
            normalized = weighted_score / available_weight
            coverage = available_weight / total_weight
            year_cov = min(1.0, (len(valid_years) / 8.0)) if valid_years else 0.0
            confidence_factor = 0.55 + 0.30 * coverage + 0.15 * year_cov
            score_int = int(max(0, min(100, normalized * confidence_factor)))

        if score_int >= 75:
            verdict = "Excellent"
        elif score_int >= 55:
            verdict = "Good"
        elif score_int >= 35:
            verdict = "Average"
        else:
            verdict = "Weak"
        return score_int, verdict

    @staticmethod
    def _score_metric(metric, val, model):
        thresholds = model.get("thresholds", {}).get(metric, {})
        if thresholds:
            if val >= thresholds.get("excellent", math.inf):
                return 100
            if val >= thresholds.get("good", math.inf):
                return 80
            if val >= thresholds.get("ok", math.inf):
                return 60
            return 30

        if metric in {"roic", "roe", "roe_spread", "economic_spread"}:
            if val >= 0.20:
                return 95
            if val >= 0.10:
                return 80
            if val >= 0.03:
                return 65
            if val >= 0:
                return 50
            return 25
        if metric in {"gross_margin", "operating_margin", "net_margin"}:
            if val >= 0.40:
                return 90
            if val >= 0.25:
                return 75
            if val >= 0.10:
                return 60
            if val >= 0:
                return 45
            return 20
        if metric == "fcf_yield":
            if val >= 0.08:
                return 90
            if val >= 0.04:
                return 75
            if val >= 0.01:
                return 60
            if val >= 0:
                return 50
            return 20
        if metric == "revenue_cagr":
            if val >= 0.15:
                return 90
            if val >= 0.08:
                return 75
            if val >= 0.03:
                return 60
            if val >= 0:
                return 50
            return 25
        if metric in {"pe_ratio", "pb_ratio"}:
            if val <= 15:
                return 85
            if val <= 30:
                return 70
            if val <= 50:
                return 55
            if val <= 90:
                return 45
            return 30
        return 60

    def _extract_metric_value(self, metric, year, ratios, strategic):
        yr = ratios.get(year, {}) if isinstance(ratios, dict) else {}
        st = strategic.get(year, {}) if isinstance(strategic, dict) else {}

        def _unwrap(v):
            if isinstance(v, RatioResult):
                return v.value
            if isinstance(v, dict):
                return v.get("value")
            return v

        aliases = {
            "economic_spread": ["Economic_Spread", "economic_spread"],
            "roic": ["roic", "ROIC"],
            "roe": ["roe", "ROE"],
            "net_margin_proxy": ["net_margin", "net_margin_core"],
            "fcf_margin": ["ocf_margin", "fcf_yield"],
            "dividend_sustainability": ["dividend_yield"],
            "solvency_proxy": ["Altman_Z_Score", "altman_z_score"],
            "reserve_proxy": ["combined_ratio_proxy", "combined_ratio"],
            "capital_ratio": ["capital_ratio", "Capital_Ratio", "debt_to_assets"],
            "efficiency_ratio": ["efficiency_ratio", "operating_margin"],
            "nim": ["nim", "Net_Interest_Margin"],
            "roe_spread": ["roe"],
            "revenue_cagr": ["Revenue_CAGR", "revenue_cagr"],
            "inventory_days": ["inventory_days", "days_inventory_outstanding"],
            "capex_to_revenue": ["capex_to_revenue"],
        }

        if metric in yr:
            return _unwrap(yr[metric])

        for key in aliases.get(metric, []):
            if key in yr:
                return _unwrap(yr[key])
            if key in st:
                try:
                    return float(st[key])
                except (TypeError, ValueError):
                    continue

        if metric == "roe_spread":
            roe = self._extract_metric_value("roe", year, ratios, strategic)
            return (roe - 0.10) if roe is not None else None
        if metric == "capital_ratio":
            dta = self._extract_metric_value("debt_to_assets", year, ratios, strategic)
            if dta is not None:
                return max(0.0, 1.0 - float(dta))
        if metric == "revenue_cagr":
            years = sorted(ratios.keys()) if isinstance(ratios, dict) else []
            if len(years) >= 3:
                first_y, last_y = years[0], years[-1]
                first = self._extract_metric_value("net_margin", first_y, ratios, strategic)
                last = self._extract_metric_value("net_margin", last_y, ratios, strategic)
                if first is not None and last is not None and first != 0:
                    n = max(1, last_y - first_y)
                    base = abs(float(last) / float(first))
                    return (base ** (1 / n) - 1) if base > 0 else None
        if metric == "capex_to_revenue":
            rev_obj = yr.get("revenue")
            rev = rev_obj.get("value") if isinstance(rev_obj, dict) else None
            capex = st.get("CapEx_to_Revenue")
            try:
                if capex is not None:
                    return float(capex)
            except Exception:
                pass
            if rev and isinstance(rev, (int, float)):
                # If raw capex is unavailable in this lightweight pipeline, leave None.
                return None

        return None

    def _get_fa(self, sheets, key):
        fa = sheets.get("Final_Acceptance", pd.DataFrame())
        if fa.empty:
            return None
        row = fa[fa.iloc[:, 0].astype(str) == key]
        return row.iloc[0, 1] if len(row) > 0 else None

    def _get_fa_score(self, sheets):
        score = self._get_fa(sheets, "Final_Professional_Score")
        try:
            return float(score)
        except (TypeError, ValueError):
            return None

    def _get_ratio_backup(self, ratios_df, metric, year):
        if ratios_df.empty:
            return None
        row = ratios_df[ratios_df.iloc[:, 0].astype(str).str.lower().str.strip() == metric.lower()]
        if row.empty:
            return None
        cols = ratios_df.columns.tolist()
        if str(year) in [str(c) for c in cols]:
            idx = [str(c) for c in cols].index(str(year))
            return row.iloc[0, idx]
        return None

    def _extract_raw(self, sheets):
        base = self._extract_year_matrix(sheets.get("Raw_by_Year", pd.DataFrame()))
        layer1 = self._extract_year_matrix(sheets.get("Layer1_Raw_SEC", pd.DataFrame()))
        if not base and not layer1:
            return {}
        years = set(base.keys()) | set(layer1.keys())
        merged = {}
        for year in years:
            row = {}
            row.update(layer1.get(year, {}))
            row.update(base.get(year, {}))
            merged[year] = row
        return merged

    def _extract_year_matrix(self, sheet_df):
        if sheet_df is None or sheet_df.empty:
            return {}
        result = {}
        for col in sheet_df.columns[1:]:
            try:
                year = int(str(col).strip())
            except (ValueError, TypeError):
                continue
            year_data = {}
            for _, row in sheet_df.iterrows():
                label = str(row.iloc[0]).strip()
                value = row[col]
                if pd.notna(value):
                    year_data[label] = self._coerce_numeric(value)
            result[year] = year_data
        return result

    def _extract_market(self, sheets):
        mkt = sheets.get("Layer2_Market", pd.DataFrame())
        if mkt.empty:
            return {}
        result = {}
        for col in mkt.columns[1:]:
            try:
                year = int(col)
            except (ValueError, TypeError):
                continue
            year_data = {}
            for _, row in mkt.iterrows():
                label = str(row.iloc[0]).strip()
                value = row[col]
                if pd.notna(value):
                    year_data[label.replace("market:", "").replace("yahoo:", "")] = self._coerce_numeric(value)
            result[year] = year_data
        return result

    def _extract_balance_check(self, sheets):
        b = sheets.get("Balance_Check", pd.DataFrame())
        if b is None or b.empty:
            return {}
        cols = {str(c).strip().lower(): c for c in b.columns}
        year_col = cols.get("year")
        assets_col = cols.get("assets")
        liab_col = cols.get("liabilities")
        equity_col = cols.get("equity")
        if not year_col:
            return {}
        out = {}
        for _, row in b.iterrows():
            year = self._coerce_numeric(row[year_col])
            if year is None:
                continue
            year_i = int(year)
            out[year_i] = {
                "total_assets": self._coerce_numeric(row[assets_col]) if assets_col else None,
                "total_liabilities": self._coerce_numeric(row[liab_col]) if liab_col else None,
                "total_equity": self._coerce_numeric(row[equity_col]) if equity_col else None,
            }
        return out

    def _extract_strategic(self, sheets):
        s = sheets.get("Strategic", pd.DataFrame())
        if s.empty:
            return {}
        result = {}
        for col in s.columns[1:]:
            try:
                year = int(col)
            except (ValueError, TypeError):
                continue
            year_data = {}
            for _, row in s.iterrows():
                metric = str(row.iloc[0]).strip()
                value = row[col]
                year_data[metric] = self._coerce_numeric(value, keep_text=True)
            result[year] = year_data
        return result

    def _calc_professional_score(self, anchor_results, ratios, quality_score):
        total_years = len(anchor_results["valid_years"]) + len(anchor_results["blocked_years"])
        valid = len(anchor_results["valid_years"])
        coverage = (valid / total_years * 100) if total_years > 0 else 0
        return round(coverage * 0.4 + quality_score * 0.6, 2)

    @staticmethod
    def _infer_ticker_from_name(filepath: str) -> str:
        name = str(filepath).replace("\\", "/").split("/")[-1]
        return name.split("_analysis_")[0].upper() if "_analysis_" in name else name[:6].upper()

    @staticmethod
    def _ratio_or_backup(metric, computed, backup):
        if backup is None:
            return computed
        try:
            b = float(backup)
        except (TypeError, ValueError):
            return computed
        if computed is None:
            return RatioResult(value=b, status="COMPUTED", reliability=80, source="ratios_sheet", metric=metric)
        if isinstance(computed, RatioResult):
            if computed.value is None or computed.status.startswith("OUTLIER"):
                return RatioResult(value=b, status="COMPUTED", reliability=80, source="ratios_sheet", metric=metric)
            if metric in {"gross_margin", "operating_margin", "net_margin"} and computed.value is not None:
                if computed.value <= 0 and b > 0:
                    return RatioResult(value=b, status="COMPUTED", reliability=75, source="ratios_sheet", metric=metric, note="fallback_from_ratio_sheet")
        return computed

    @staticmethod
    def _coerce_numeric(value, keep_text=False):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return None
            return float(value)
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "n/a", "—", "-"}:
            return None
        clean = text.replace(",", "").replace("%", "")
        try:
            num = float(clean)
            if "%" in text:
                return num / 100.0
            return num
        except ValueError:
            return text if keep_text else None
