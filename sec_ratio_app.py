import argparse
import datetime as dt
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests


ANNUAL_FORMS = {"10-K", "10-K/A"}


@dataclass(frozen=True)
class FactObservation:
    taxonomy: str
    concept: str
    tag: str
    unit: str
    value: float
    decimals: Optional[int]
    start: Optional[str]
    end: Optional[str]
    fy: Optional[int]
    fp: Optional[str]
    form: Optional[str]
    filed: Optional[str]
    accn: Optional[str]
    frame: Optional[str]


def _parse_date(s: Optional[str]) -> Optional[dt.date]:
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        return None


def _duration_days(start: Optional[str], end: Optional[str]) -> Optional[int]:
    ds = _parse_date(start)
    de = _parse_date(end)
    if ds is None or de is None:
        return None
    return (de - ds).days


class SECClient:
    def __init__(self, user_agent: str, timeout: int = 30) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "Host": "data.sec.gov",
            }
        )

    @staticmethod
    def normalize_cik(cik: str) -> str:
        s = "".join(ch for ch in str(cik) if ch.isdigit())
        if not s:
            raise ValueError(f"Invalid CIK: {cik}")
        return s.zfill(10)

    def fetch_companyfacts(self, cik: str) -> Dict:
        cik10 = self.normalize_cik(cik)
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()


class FactStore:
    def __init__(self, companyfacts: Dict) -> None:
        self.companyfacts = companyfacts
        self.observations: List[FactObservation] = self._extract_observations(companyfacts)

    @staticmethod
    def _to_int(v) -> Optional[int]:
        try:
            if v is None:
                return None
            return int(v)
        except Exception:
            return None

    @staticmethod
    def _to_float(v) -> Optional[float]:
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    def _extract_observations(self, payload: Dict) -> List[FactObservation]:
        out: List[FactObservation] = []
        facts = (payload or {}).get("facts", {})
        for taxonomy, by_concept in facts.items():
            if not isinstance(by_concept, dict):
                continue
            for concept, concept_obj in by_concept.items():
                units = (concept_obj or {}).get("units", {})
                if not isinstance(units, dict):
                    continue
                for unit, rows in units.items():
                    if not isinstance(rows, list):
                        continue
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        val = self._to_float(row.get("val"))
                        if val is None:
                            continue
                        dec = self._to_int(row.get("decimals"))
                        fy = self._to_int(row.get("fy"))
                        out.append(
                            FactObservation(
                                taxonomy=str(taxonomy),
                                concept=str(concept),
                                tag=f"{taxonomy}:{concept}",
                                unit=str(unit),
                                value=val,
                                decimals=dec,
                                start=row.get("start"),
                                end=row.get("end"),
                                fy=fy,
                                fp=row.get("fp"),
                                form=row.get("form"),
                                filed=row.get("filed"),
                                accn=row.get("accn"),
                                frame=row.get("frame"),
                            )
                        )
        return out

    def annual_10k_years(self, start_year: int, end_year: int) -> Dict[int, List[FactObservation]]:
        by_year: Dict[int, List[FactObservation]] = {y: [] for y in range(start_year, end_year + 1)}
        for obs in self.observations:
            if obs.fy is None or obs.fy not in by_year:
                continue
            if obs.form not in ANNUAL_FORMS:
                continue
            by_year[obs.fy].append(obs)
        return by_year

    def validate_year_coverage(self, start_year: int, end_year: int) -> List[int]:
        by_year = self.annual_10k_years(start_year, end_year)
        return [y for y, rows in by_year.items() if not rows]

    def pick_fact(
        self,
        year: int,
        concepts: List[str],
        period_type: str,
        preferred_end: Optional[str] = None,
    ) -> Optional[FactObservation]:
        candidates: List[FactObservation] = []
        concept_set = set(concepts)
        for obs in self.observations:
            if obs.fy != year:
                continue
            if obs.form not in ANNUAL_FORMS:
                continue
            if obs.concept not in concept_set:
                continue
            if period_type == "duration":
                days = _duration_days(obs.start, obs.end)
                if days is None or days < 340 or days > 390:
                    continue
            elif period_type == "instant":
                if obs.end is None:
                    continue
            if preferred_end and obs.end != preferred_end:
                continue
            candidates.append(obs)

        if not candidates:
            return None

        concept_rank = {c: i for i, c in enumerate(concepts)}
        candidates.sort(
            key=lambda o: (
                concept_rank.get(o.concept, 10**6),
                str(o.filed or ""),
                str(o.accn or ""),
            ),
            reverse=False,
        )
        return candidates[0]


class RatioEngine:
    METRICS = {
        "current_assets": ["AssetsCurrent"],
        "inventory": ["InventoryNet", "InventoryFinishedGoods", "InventoryGross"],
        "current_liabilities": ["LiabilitiesCurrent"],
        "total_liabilities": ["Liabilities"],
        "equity": [
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
            "StockholdersEquity",
        ],
        "assets": ["Assets"],
        "revenue": ["Revenues", "SalesRevenueNet", "RevenueFromContractWithCustomerExcludingAssessedTax"],
        "gross_profit": ["GrossProfit"],
        "net_income": ["NetIncomeLoss", "ProfitLoss"],
        "cogs": ["CostOfGoodsAndServicesSold", "CostOfRevenue"],
        "interest_expense": ["InterestExpense", "InterestAndDebtExpense"],
        "ebit": ["OperatingIncomeLoss", "IncomeBeforeTax"],
    }

    def __init__(self, computation_timestamp: Optional[str] = None) -> None:
        self.computation_timestamp = computation_timestamp or dt.datetime.utcnow().isoformat() + "Z"

    @staticmethod
    def _obs_payload(metric: str, obs: FactObservation) -> Dict:
        return {
            "metric": metric,
            "tag": obs.tag,
            "taxonomy": obs.taxonomy,
            "concept": obs.concept,
            "raw_value": obs.value,
            "unit": obs.unit,
            "decimals": obs.decimals,
            "period": {"start": obs.start, "end": obs.end, "fy": obs.fy, "fp": obs.fp},
            "form": obs.form,
            "filed": obs.filed,
            "accn": obs.accn,
        }

    @staticmethod
    def _period_key(obs: FactObservation, period_type: str) -> Tuple:
        if period_type == "duration":
            return (obs.start, obs.end)
        return (obs.end,)

    def _validate_same_period(self, inputs: List[Tuple[str, FactObservation]], period_type: str) -> Tuple[bool, str]:
        if not inputs:
            return False, "No inputs provided"
        keys = {self._period_key(obs, period_type) for _, obs in inputs}
        if len(keys) != 1:
            return False, f"Period mismatch among inputs: {sorted(list(keys))}"
        if period_type == "duration":
            _, any_obs = inputs[0]
            days = _duration_days(any_obs.start, any_obs.end)
            if days is None or days < 340 or days > 390:
                return False, f"Duration is not annual: {days} days"
        return True, ""

    def _make_not_computable(self, name: str, formula: str, reason: str, inputs: List[Tuple[str, FactObservation]]) -> Dict:
        payload = {
            "ratio_name": name,
            "status": "Not Computable",
            "value": None,
            "not_computable_reason": reason,
            "formula": formula,
            "period": None,
            "inputs": [self._obs_payload(m, o) for m, o in inputs],
            "computation_timestamp": self.computation_timestamp,
        }
        return payload

    def _compute_division(
        self,
        *,
        name: str,
        formula: str,
        numerator: Tuple[str, Optional[FactObservation]],
        denominator: Tuple[str, Optional[FactObservation]],
        period_type: str,
    ) -> Dict:
        n_metric, n_obs = numerator
        d_metric, d_obs = denominator
        used: List[Tuple[str, FactObservation]] = []
        if n_obs is None:
            return self._make_not_computable(name, formula, f"Missing input: {n_metric}", used)
        used.append((n_metric, n_obs))
        if d_obs is None:
            return self._make_not_computable(name, formula, f"Missing input: {d_metric}", used)
        used.append((d_metric, d_obs))

        ok, reason = self._validate_same_period(used, period_type)
        if not ok:
            return self._make_not_computable(name, formula, reason, used)
        if d_obs.value == 0:
            return self._make_not_computable(name, formula, "Division by zero", used)

        value = n_obs.value / d_obs.value
        period = {"start": n_obs.start if period_type == "duration" else None, "end": n_obs.end, "fy": n_obs.fy, "fp": n_obs.fp}
        return {
            "ratio_name": name,
            "status": "Computed",
            "value": value,
            "not_computable_reason": None,
            "formula": formula,
            "period": period,
            "inputs": [self._obs_payload(m, o) for m, o in used],
            "computation_timestamp": self.computation_timestamp,
        }

    def _compute_quick_ratio(
        self,
        current_assets: Optional[FactObservation],
        inventory: Optional[FactObservation],
        current_liabilities: Optional[FactObservation],
    ) -> Dict:
        name = "Quick Ratio"
        formula = "(Current Assets - Inventory) / Current Liabilities"
        used: List[Tuple[str, FactObservation]] = []
        if current_assets is None:
            return self._make_not_computable(name, formula, "Missing input: current_assets", used)
        used.append(("current_assets", current_assets))
        if inventory is None:
            return self._make_not_computable(name, formula, "Missing input: inventory", used)
        used.append(("inventory", inventory))
        if current_liabilities is None:
            return self._make_not_computable(name, formula, "Missing input: current_liabilities", used)
        used.append(("current_liabilities", current_liabilities))

        ok, reason = self._validate_same_period(used, "instant")
        if not ok:
            return self._make_not_computable(name, formula, reason, used)
        if current_liabilities.value == 0:
            return self._make_not_computable(name, formula, "Division by zero", used)

        value = (current_assets.value - inventory.value) / current_liabilities.value
        period = {"start": None, "end": current_assets.end, "fy": current_assets.fy, "fp": current_assets.fp}
        return {
            "ratio_name": name,
            "status": "Computed",
            "value": value,
            "not_computable_reason": None,
            "formula": formula,
            "period": period,
            "inputs": [self._obs_payload(m, o) for m, o in used],
            "computation_timestamp": self.computation_timestamp,
        }

    def _compute_margin(
        self,
        name: str,
        numerator_label: str,
        numerator_obs: Optional[FactObservation],
        revenue_obs: Optional[FactObservation],
    ) -> Dict:
        return self._compute_division(
            name=name,
            formula=f"{numerator_label} / Revenue",
            numerator=(numerator_label, numerator_obs),
            denominator=("revenue", revenue_obs),
            period_type="duration",
        )

    def _compute_mixed_ratio(
        self,
        name: str,
        formula: str,
        duration_input: Tuple[str, Optional[FactObservation]],
        instant_input: Tuple[str, Optional[FactObservation]],
    ) -> Dict:
        d_label, d_obs = duration_input
        i_label, i_obs = instant_input
        used: List[Tuple[str, FactObservation]] = []
        if d_obs is None:
            return self._make_not_computable(name, formula, f"Missing input: {d_label}", used)
        used.append((d_label, d_obs))
        if i_obs is None:
            return self._make_not_computable(name, formula, f"Missing input: {i_label}", used)
        used.append((i_label, i_obs))

        ok_d, reason_d = self._validate_same_period([(d_label, d_obs)], "duration")
        if not ok_d:
            return self._make_not_computable(name, formula, reason_d, used)
        ok_i, reason_i = self._validate_same_period([(i_label, i_obs)], "instant")
        if not ok_i:
            return self._make_not_computable(name, formula, reason_i, used)
        if d_obs.end != i_obs.end:
            return self._make_not_computable(name, formula, "Period mismatch between duration and instant inputs", used)
        if i_obs.value == 0:
            return self._make_not_computable(name, formula, "Division by zero", used)

        value = d_obs.value / i_obs.value
        period = {"start": d_obs.start, "end": d_obs.end, "fy": d_obs.fy, "fp": d_obs.fp}
        return {
            "ratio_name": name,
            "status": "Computed",
            "value": value,
            "not_computable_reason": None,
            "formula": formula,
            "period": period,
            "inputs": [self._obs_payload(m, o) for m, o in used],
            "computation_timestamp": self.computation_timestamp,
        }

    def compute_for_year(self, store: FactStore, year: int) -> Dict:
        duration_anchor = store.pick_fact(year, self.METRICS["revenue"], "duration")
        if duration_anchor is None:
            duration_anchor = store.pick_fact(year, self.METRICS["net_income"], "duration")
        duration_end = duration_anchor.end if duration_anchor else None

        instant_anchor = store.pick_fact(year, self.METRICS["assets"], "instant")
        if instant_anchor is None:
            instant_anchor = store.pick_fact(year, self.METRICS["equity"], "instant")
        instant_end = instant_anchor.end if instant_anchor else None

        picks: Dict[str, Optional[FactObservation]] = {
            "current_assets": store.pick_fact(year, self.METRICS["current_assets"], "instant", preferred_end=instant_end),
            "inventory": store.pick_fact(year, self.METRICS["inventory"], "instant", preferred_end=instant_end),
            "current_liabilities": store.pick_fact(year, self.METRICS["current_liabilities"], "instant", preferred_end=instant_end),
            "total_liabilities": store.pick_fact(year, self.METRICS["total_liabilities"], "instant", preferred_end=instant_end),
            "equity": store.pick_fact(year, self.METRICS["equity"], "instant", preferred_end=instant_end),
            "assets": store.pick_fact(year, self.METRICS["assets"], "instant", preferred_end=instant_end),
            "revenue": store.pick_fact(year, self.METRICS["revenue"], "duration", preferred_end=duration_end),
            "gross_profit": store.pick_fact(year, self.METRICS["gross_profit"], "duration", preferred_end=duration_end),
            "net_income": store.pick_fact(year, self.METRICS["net_income"], "duration", preferred_end=duration_end),
            "cogs": store.pick_fact(year, self.METRICS["cogs"], "duration", preferred_end=duration_end),
            "interest_expense": store.pick_fact(year, self.METRICS["interest_expense"], "duration", preferred_end=duration_end),
            "ebit": store.pick_fact(year, self.METRICS["ebit"], "duration", preferred_end=duration_end),
        }

        results = []

        results.append(
            self._compute_division(
                name="Current Ratio",
                formula="Current Assets / Current Liabilities",
                numerator=("current_assets", picks["current_assets"]),
                denominator=("current_liabilities", picks["current_liabilities"]),
                period_type="instant",
            )
        )

        results.append(self._compute_quick_ratio(picks["current_assets"], picks["inventory"], picks["current_liabilities"]))

        results.append(
            self._compute_division(
                name="Debt to Equity",
                formula="Total Liabilities / Equity",
                numerator=("total_liabilities", picks["total_liabilities"]),
                denominator=("equity", picks["equity"]),
                period_type="instant",
            )
        )

        results.append(
            self._compute_division(
                name="Debt Ratio",
                formula="Total Liabilities / Total Assets",
                numerator=("total_liabilities", picks["total_liabilities"]),
                denominator=("assets", picks["assets"]),
                period_type="instant",
            )
        )

        results.append(
            self._compute_mixed_ratio(
                name="ROA",
                formula="Net Income / Total Assets",
                duration_input=("net_income", picks["net_income"]),
                instant_input=("assets", picks["assets"]),
            )
        )

        results.append(
            self._compute_mixed_ratio(
                name="ROE",
                formula="Net Income / Equity",
                duration_input=("net_income", picks["net_income"]),
                instant_input=("equity", picks["equity"]),
            )
        )

        results.append(self._compute_margin("Gross Margin", "gross_profit", picks["gross_profit"], picks["revenue"]))
        results.append(self._compute_margin("Net Margin", "net_income", picks["net_income"], picks["revenue"]))

        results.append(
            self._compute_mixed_ratio(
                name="Asset Turnover",
                formula="Revenue / Total Assets",
                duration_input=("revenue", picks["revenue"]),
                instant_input=("assets", picks["assets"]),
            )
        )

        results.append(
            self._compute_mixed_ratio(
                name="Inventory Turnover",
                formula="COGS / Inventory",
                duration_input=("cogs", picks["cogs"]),
                instant_input=("inventory", picks["inventory"]),
            )
        )

        results.append(
            self._compute_division(
                name="Interest Coverage",
                formula="EBIT / Interest Expense",
                numerator=("ebit", picks["ebit"]),
                denominator=("interest_expense", picks["interest_expense"]),
                period_type="duration",
            )
        )

        return {
            "year": year,
            "ratios": results,
            "raw_inputs": {
                k: (self._obs_payload(k, v) if v is not None else None)
                for k, v in picks.items()
            },
        }


def run_for_cik(cik: str, start_year: int, end_year: int, client: SECClient) -> Dict:
    facts = client.fetch_companyfacts(cik)
    store = FactStore(facts)
    missing_years = store.validate_year_coverage(start_year, end_year)
    if missing_years:
        raise ValueError(
            f"CIK {SECClient.normalize_cik(cik)} missing annual 10-K facts for years: {missing_years}"
        )

    engine = RatioEngine()
    per_year = []
    raw_inputs = []
    for year in range(start_year, end_year + 1):
        res = engine.compute_for_year(store, year)
        per_year.append({"year": year, "ratios": res["ratios"]})
        raw_inputs.append({"year": year, "inputs": res["raw_inputs"]})

    return {
        "cik": SECClient.normalize_cik(cik),
        "entity_name": facts.get("entityName"),
        "start_year": start_year,
        "end_year": end_year,
        "ratio_results": per_year,
        "raw_inputs": raw_inputs,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SEC XBRL ratio engine with full audit trail")
    p.add_argument("--cik", nargs="+", required=True, help="One or more CIK values")
    p.add_argument("--start-year", type=int, required=True, help="Start fiscal year")
    p.add_argument("--end-year", type=int, required=True, help="End fiscal year")
    p.add_argument(
        "--user-agent",
        required=True,
        help="SEC compliant User-Agent, e.g. 'Your Name your@email.com'",
    )
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--ratio-output", default="ratio_results.json")
    p.add_argument("--raw-output", default="raw_inputs.json")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.start_year > args.end_year:
        raise ValueError("start-year must be <= end-year")

    client = SECClient(user_agent=args.user_agent, timeout=args.timeout)

    all_ratio_results = []
    all_raw_inputs = []
    errors = []

    for cik in args.cik:
        try:
            out = run_for_cik(cik, args.start_year, args.end_year, client)
            all_ratio_results.append(
                {
                    "cik": out["cik"],
                    "entity_name": out["entity_name"],
                    "start_year": out["start_year"],
                    "end_year": out["end_year"],
                    "results": out["ratio_results"],
                }
            )
            all_raw_inputs.append(
                {
                    "cik": out["cik"],
                    "entity_name": out["entity_name"],
                    "start_year": out["start_year"],
                    "end_year": out["end_year"],
                    "inputs": out["raw_inputs"],
                }
            )
        except Exception as e:
            errors.append({"cik": str(cik), "error": str(e)})

    with open(args.ratio_output, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": dt.datetime.utcnow().isoformat() + "Z",
                "requested_period": {"start_year": args.start_year, "end_year": args.end_year},
                "results": all_ratio_results,
                "errors": errors,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    with open(args.raw_output, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": dt.datetime.utcnow().isoformat() + "Z",
                "requested_period": {"start_year": args.start_year, "end_year": args.end_year},
                "inputs": all_raw_inputs,
                "errors": errors,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


if __name__ == "__main__":
    main()

