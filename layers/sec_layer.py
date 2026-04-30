"""SEC-only data layer for accounting facts from EDGAR XBRL APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


@dataclass(frozen=True)
class LayerOutput:
    """Standardized payload wrapper for layer outputs."""

    payload: Dict[str, Any]


class SECLayer:
    """Fetches accounting facts exclusively from SEC XBRL APIs."""

    BASE_COMPANYFACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    def __init__(self, user_agent: str, output_dir: str = "outputs") -> None:
        self.user_agent = user_agent
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Avoid broken proxy env vars interfering with SEC access.
        self._session = requests.Session()
        self._session.trust_env = False

    def fetch(self, cik: str, start_year: int, end_year: int) -> LayerOutput:
        """Return structured SEC accounting data for a CIK and year range."""
        cik_padded = str(cik).zfill(10)
        endpoint = self.BASE_COMPANYFACTS.format(cik=cik_padded)

        response = self._session.get(
            endpoint,
            headers={"User-Agent": self.user_agent, "Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        if "facts" not in payload:
            raise ValueError("Missing 'facts' in SEC companyfacts response")

        concepts = self._concepts_for_statements()
        periods = self._extract_periods(payload, concepts, start_year, end_year)

        structured = {
            "layer": "SEC",
            "status": "OK",
            "cik": cik_padded,
            "source": "SEC EDGAR XBRL",
            "source_endpoint": endpoint,
            "statement_scope": [
                "Balance Sheet",
                "Income Statement",
                "Cash Flow Statement",
                "Shares Outstanding",
            ],
            "periods": periods,
            "data_source_trace": {"endpoint": endpoint, "cik": cik_padded},
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependency_map": {"depends_on": [], "provides": ["SEC"]},
        }

        out_path = self.output_dir / "structured_accounting_data.json"
        out_path.write_text(json.dumps(structured, indent=2), encoding="utf-8")
        return LayerOutput(payload=structured)

    def _concepts_for_statements(self) -> List[str]:
        return [
            "us-gaap:Assets",
            "us-gaap:AssetsCurrent",
            "us-gaap:AssetsNoncurrent",
            "us-gaap:Liabilities",
            "us-gaap:LiabilitiesCurrent",
            "us-gaap:LiabilitiesNoncurrent",
            "us-gaap:StockholdersEquity",
            "us-gaap:LiabilitiesAndStockholdersEquity",
            # Debt (corporates + banks): needed for leverage, EV, WACC weights, and debt ratios.
            # Note: some concepts include capital leases; we still ingest them so the application
            # can apply a "debt-only preferred, lease-inclusive last resort" policy upstream.
            "us-gaap:Debt",
            "us-gaap:TotalDebt",
            "us-gaap:DebtCurrent",
            "us-gaap:DebtNoncurrent",
            "us-gaap:LongTermDebt",
            "us-gaap:LongTermDebtCurrent",
            "us-gaap:LongTermDebtNoncurrent",
            "us-gaap:CurrentPortionOfLongTermDebt",
            "us-gaap:ShortTermBorrowings",
            "us-gaap:CommercialPaper",
            "us-gaap:NotesPayable",
            "us-gaap:LongTermDebtAndCapitalLeaseObligations",
            "us-gaap:DebtAndCapitalLeaseObligations",
            "us-gaap:OperatingLeaseLiability",
            "us-gaap:OperatingLeaseLiabilityCurrent",
            "us-gaap:OperatingLeaseLiabilityNoncurrent",
            "us-gaap:FinanceLeaseLiability",
            "us-gaap:FinanceLeaseLiabilityCurrent",
            "us-gaap:FinanceLeaseLiabilityNoncurrent",
            "us-gaap:Revenues",
            "us-gaap:SalesRevenueNet",
            "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
            "us-gaap:OperatingRevenue",
            "us-gaap:CostOfGoodsAndServicesSold",
            "us-gaap:CostOfRevenue",
            "us-gaap:CostOfSales",
            "us-gaap:GrossProfit",
            "us-gaap:OperatingExpenses",
            "us-gaap:OperatingIncomeLoss",
            "us-gaap:NetIncomeLoss",
            "us-gaap:ProfitLoss",
            "us-gaap:NetCashProvidedByUsedInOperatingActivities",
            "us-gaap:NetCashProvidedByUsedInInvestingActivities",
            "us-gaap:NetCashProvidedByUsedInFinancingActivities",
            "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
            "us-gaap:InventoryNet",
            "us-gaap:AccountsReceivableNetCurrent",
            "us-gaap:AccountsPayableCurrent",
            "us-gaap:CashAndCashEquivalentsAtCarryingValue",
            "us-gaap:DepreciationAndAmortization",
            "us-gaap:DepreciationDepletionAndAmortization",
            "us-gaap:Dividends",
            "us-gaap:DividendsPaid",
            "us-gaap:PaymentsOfDividends",
            "us-gaap:PaymentsOfDividendsCommonStock",
            "us-gaap:DividendsCommonStockCash",
            "us-gaap:InterestExpense",
            "us-gaap:InterestExpenseNonoperating",
            "us-gaap:InterestAndDebtExpense",
            "us-gaap:InterestExpenseDebt",
            "us-gaap:InterestPaidNet",
            "us-gaap:InterestCostsIncurred",
            "us-gaap:EarningsPerShareBasic",
            "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
            "us-gaap:RetainedEarningsAccumulatedDeficit",
            # Banking-focused concepts
            "us-gaap:NetInterestIncome",
            "us-gaap:InterestIncomeOperating",
            "us-gaap:InterestAndDividendIncomeOperating",
            "us-gaap:LoansReceivableNet",
            "us-gaap:LoansAndLeasesReceivableNetReportedAmount",
            "us-gaap:LoansHeldForSale",
            "us-gaap:LoansAndLeasesReceivable",
            "us-gaap:Deposits",
            "us-gaap:DepositLiabilities",
            "us-gaap:InterestBearingDepositsInBanks",
            "us-gaap:NoninterestBearingDeposits",
            "us-gaap:ProvisionForCreditLosses",
            "us-gaap:ProvisionForLoanLosses",
            "us-gaap:AllowanceForCreditLosses",
            "us-gaap:CommonEquityTier1Capital",
            "us-gaap:CommonEquityTier1CapitalRatio",
            "us-gaap:Tier1Capital",
            # Insurance-focused concepts
            "us-gaap:PremiumsEarned",
            "us-gaap:PremiumsEarnedNet",
            "us-gaap:DirectPremiumsEarned",
            "us-gaap:AssumedPremiumsEarned",
            "us-gaap:PolicyholderBenefitsAndClaimsIncurredNet",
            "us-gaap:PolicyholderBenefits",
            "us-gaap:PolicyClaimsAndBenefits",
            "us-gaap:IncurredClaimsPropertyCasualtyAndLiability",
            "us-gaap:BenefitsLossesAndExpenses",
            "us-gaap:DeferredPolicyAcquisitionCosts",
            "us-gaap:ReinsuranceRecoverables",
            "us-gaap:LossAndLossAdjustmentExpense",
        ]

    def _extract_periods(
        self,
        companyfacts: Dict[str, Any],
        concepts: List[str],
        start_year: int,
        end_year: int,
    ) -> Dict[str, Any]:
        periods: Dict[str, Any] = {str(y): {"facts": {}} for y in range(start_year, end_year + 1)}
        facts_root = companyfacts.get("facts", {})

        for concept in concepts:
            taxonomy, concept_name = concept.split(":", 1)
            concept_node = facts_root.get(taxonomy, {}).get(concept_name)
            if not concept_node:
                continue

            units = concept_node.get("units", {})
            for unit_name, entries in units.items():
                for entry in entries:
                    if entry.get("form") not in {"10-K", "10-K/A"}:
                        continue
                    end_date = entry.get("end")
                    if not end_date:
                        continue
                    # Prefer SEC-provided fiscal year (fy) when present.
                    # Many issuers have fiscal years that end in the following calendar year
                    # (e.g., fiscal 2022 end date in Jan 2023). Using end_date[:4] would shift
                    # the fact by +1 year and breaks time-series matching.
                    fy = entry.get("fy")
                    year = int(fy) if isinstance(fy, int) else int(end_date[:4])
                    if year < start_year or year > end_year:
                        continue
                    # Do not exclude annual 10-K facts just because the SEC "frame" contains Q4.
                    # Many annual instant facts are framed like "CY2020Q4I" even for 10-K filings.
                    # Instead, drop true quarterly filings via fp ("Q1/Q2/Q3").
                    fp = str(entry.get("fp") or "").upper()
                    if fp.startswith("Q"):
                        continue
                    frame = entry.get("frame", "")

                    candidate = {
                        "value": entry.get("val"),
                        "unit": unit_name,
                        "period_start": entry.get("start"),
                        "period_end": entry.get("end"),
                        "fy": fy,
                        "fp": entry.get("fp"),
                        "form": entry.get("form"),
                        "accn": entry.get("accn"),
                        "filed": entry.get("filed"),
                        "frame": frame,
                        "tag": concept,
                    }

                    existing = periods[str(year)]["facts"].get(concept)
                    if existing is None:
                        periods[str(year)]["facts"][concept] = candidate
                        continue

                    existing_filed = existing.get("filed") or ""
                    candidate_filed = candidate.get("filed") or ""
                    if candidate_filed > existing_filed:
                        periods[str(year)]["facts"][concept] = candidate

        return periods
