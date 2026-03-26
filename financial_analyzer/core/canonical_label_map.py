CANONICAL_MAP = {
    "total_assets": {
        "labels": [
            "Assets",
            "TotalAssets",
            "Total assets",
            "TOTAL ASSETS",
            "AssetsCurrent+AssetsNoncurrent",
        ],
        "mandatory": True,
        "sector_override": {},
    },
    "total_liabilities": {
        "labels": [
            "Liabilities",
            "TotalLiabilities",
            "Total liabilities",
            "LiabilitiesAndStockholdersEquity",
        ],
        "fallback": "total_assets - total_equity",
        "mandatory": True,
    },
    "total_equity": {
        "labels": [
            "StockholdersEquity",
            "Equity",
            "TotalEquity",
            "StockholdersEquityAttributableToParent",
            "CommonStockholdersEquity",
        ],
        "fallback": "total_assets - total_liabilities",
        "mandatory": True,
    },
    "revenue": {
        "labels": [
            "Revenues",
            "Revenue",
            "NetRevenues",
            "TotalRevenues",
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "Net revenue",
            "Net revenues",
            "Net sales",
            "Total revenues",
            "Total net revenues",
            "TotalRevenue",
            "Total revenue",
            "ServiceRevenue",
            "SubscriptionRevenue",
        ],
        "sector_override": {
            "commercial_bank": [
                "TotalNonInterestRevenue",
                "NetInterestIncome",
                "InterestAndNoninterestIncome",
            ],
            "investment_bank": [
                "NetRevenues",
                "TotalNetRevenues",
                "RevenuesNetOfInterestExpense",
            ],
            "insurance_pc": ["PremiumsEarnedNet", "TotalRevenues"],
            "insurance_life": ["PremiumsEarnedNet", "TotalRevenues"],
            "insurance_broker": ["TotalRevenues", "Revenues"],
        },
        "sanity_check": "annual_vs_quarterly",
        "mandatory": True,
    },
    "net_income": {
        "labels": [
            "NetIncome",
            "NetIncomeLoss",
            "NetIncomeAttributableToParent",
            "ProfitLoss",
            "NetEarnings",
        ],
        "mandatory": True,
    },
    "gross_profit": {
        "labels": [
            "GrossProfit",
            "GrossProfitLoss",
            "Gross Profit",
            "Gross profit",
            "GROSS PROFIT",
            "GrossIncome",
        ],
        "fallback": "revenue - cost_of_revenue",
        "mandatory": True,
    },
    "gross_margin_ratio": {
        "labels": ["GrossMargin", "Gross Margin"],
        "mandatory": False,
    },
    "cost_of_revenue": {
        "labels": [
            "CostOfRevenue",
            "CostOfGoodsSold",
            "COGS",
            "CostOfGoodsAndServicesSold",
            "CostOfSales",
            "CostOfRevenueProduct",
            "CostOfRevenueService",
            "Cost of Revenue",
            "Cost of revenue",
            "Cost of goods sold",
            "Cost of Sales",
            "Cost of revenue: Product",
            "Cost of revenue: Service",
        ],
        "aggregation_rule": "sum_if_multiple",
        "mandatory": False,
    },
    "operating_income": {
        "labels": [
            "OperatingIncomeLoss",
            "OperatingIncome",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxes",
        ],
        "mandatory": False,
    },
    "accounts_payable": {
        "labels": [
            "AccountsPayableCurrent",
            "AccountsPayable",
            "Accounts Payable",
            "Accounts Payable, Other",
            "Accounts payables, accrued liabilities and other",
        ],
        "dedup_rule": "take_largest",
        "mandatory": False,
    },
    "accounts_receivable": {
        "labels": [
            "AccountsReceivableNetCurrent",
            "AccountsReceivable",
            "Accounts receivable, net",
        ],
        "mandatory": False,
    },
    "inventory": {"labels": ["InventoryNet", "Inventories", "Inventory"], "mandatory": False},
    "long_term_debt": {
        "labels": [
            "LongTermDebt",
            "LongTermDebtNoncurrent",
            "DebtNoncurrent",
            "Long-term debt",
            "Long term debt",
            "Debt, non-current",
        ],
        "mandatory": False,
    },
    "short_term_borrowings": {
        "labels": ["ShortTermBorrowings"],
        "mandatory": False,
    },
    "long_term_debt_current": {
        "labels": ["LongTermDebtCurrent"],
        "mandatory": False,
    },
    "notes_payable": {
        "labels": ["NotesPayable"],
        "mandatory": False,
    },
    "total_debt": {
        "labels": [
            "TotalDebt",
            "LongTermDebtAndCapitalLeaseObligations",
        ],
        "mandatory": False,
    },
    "ebitda": {
        "labels": ["EBITDA", "EarningsBeforeInterestTaxesDepreciationAmortization"],
        "fallback": "operating_income + depreciation_amortization",
        "mandatory": False,
    },
    "operating_expenses": {
        "labels": ["OperatingExpenses", "Operating Expense", "Operating expenses"],
        "mandatory": False,
    },
    "cash_and_equivalents": {
        "labels": [
            "CashAndCashEquivalents",
            "CashAndCashEquivalentsAtCarryingValue",
            "Cash and Cash Equivalents",
        ],
        "mandatory": False,
    },
    "capex": {
        "labels": [
            "CapitalExpenditures",
            "PaymentsToAcquirePropertyPlant",
            "CapitalExpenditureDiscontinuedOperations",
        ],
        "sign_convention": "negative_means_outflow",
        "mandatory": False,
    },
    "depreciation_amortization": {
        "labels": [
            "DepreciationDepletionAndAmortization",
            "DepreciationAmortization",
            "Depreciation and Amortization",
        ],
        "mandatory": False,
    },
    "interest_expense": {
        "labels": [
            "InterestExpense",
            "InterestExpenseDebt",
            "Interest Expense",
            "Interest paid",
        ],
        "mandatory": False,
    },
    "net_interest_income": {
        "labels": ["NetInterestIncome", "InterestIncomeExpenseNet"],
        "sectors_only": ["commercial_bank", "investment_bank"],
        "mandatory": False,
    },
    "loan_loss_provision": {
        "labels": [
            "ProvisionForLoanLossesExpensed",
            "AllowanceForLoanAndLeaseLossesPeriodIncreaseDecrease",
        ],
        "sectors_only": ["commercial_bank"],
        "mandatory": False,
    },
    "combined_ratio_proxy": {
        "labels": ["combined_proxy", "CombinedRatio"],
        "sectors_only": ["insurance_pc"],
        "mandatory": False,
    },
}


class CanonicalLabelResolver:
    @staticmethod
    def _normalize(text: str) -> str:
        return (
            str(text or "")
            .lower()
            .replace(" ", "")
            .replace("-", "")
            .replace("_", "")
            .replace(",", "")
        )

    @staticmethod
    def _tokenize(text: str) -> set:
        import re

        raw = str(text or "").lower()
        tokens = re.split(r"[^a-z0-9]+", raw)
        stop = {
            "total",
            "net",
            "and",
            "of",
            "for",
            "from",
            "the",
            "to",
            "with",
            "on",
            "at",
            "non",
        }
        return {t for t in tokens if t and t not in stop}

    def _token_match(self, tokens: set, canonical_key: str) -> float:
        rules = {
            "revenue": {"any": {"revenue", "revenues", "sales"}, "block": {"cost"}},
            "cost_of_revenue": {
                "any": {"cost", "cogs", "goods", "services", "revenue", "sales"},
                "all": {"cost"},
            },
            "gross_profit": {"all": {"gross"}, "any": {"profit", "income"}},
            "gross_margin_ratio": {"all": {"gross", "margin"}},
            "operating_income": {"all": {"operating"}, "any": {"income", "loss"}},
            "operating_expenses": {"all": {"operating"}, "any": {"expense", "expenses"}},
            "total_debt": {"all": {"debt"}, "block": {"long", "short", "current", "noncurrent"}},
            "long_term_debt": {"all": {"debt"}, "any": {"long", "noncurrent"}},
            "short_term_borrowings": {"all": {"short", "borrowings"}},
            "long_term_debt_current": {"all": {"debt", "current"}},
            "notes_payable": {"all": {"notes", "payable"}},
            "accounts_payable": {"all": {"accounts", "payable"}},
            "accounts_receivable": {"all": {"accounts", "receivable"}},
            "inventory": {"any": {"inventory", "inventories"}},
            "interest_expense": {"all": {"interest"}, "any": {"expense", "paid"}},
        }
        rule = rules.get(canonical_key)
        if not rule:
            return 0.0
        if "block" in rule and tokens.intersection(rule["block"]):
            return 0.0
        if "all" in rule and not rule["all"].issubset(tokens):
            return 0.0
        if "any" in rule and not tokens.intersection(rule["any"]):
            return 0.0
        # score: overlap with any/all tokens
        target = set()
        if "all" in rule:
            target |= rule["all"]
        if "any" in rule:
            target |= rule["any"]
        return len(tokens.intersection(target)) / max(1, len(target))

    def resolve(self, raw_label: str, sector: str = None) -> dict:
        raw = str(raw_label or "").strip()
        raw_lower = raw.lower().strip()
        normalized = self._normalize(raw)
        tokens = self._tokenize(raw)

        for canonical_key, config in CANONICAL_MAP.items():
            if sector and config.get("sector_override"):
                for sl in config["sector_override"].get(sector, []):
                    sl_raw = str(sl or "").strip()
                    sl_norm = self._normalize(sl_raw)
                    if sl_raw.lower() == raw_lower or normalized == sl_norm:
                        return {
                            "canonical": canonical_key,
                            "match": "sector_exact",
                            "confidence": 100,
                        }

            for label in config["labels"]:
                if label.lower() == raw_lower:
                    return {"canonical": canonical_key, "match": "exact", "confidence": 100}

            for label in config["labels"]:
                label_norm = self._normalize(label)
                if normalized == label_norm and normalized:
                    return {"canonical": canonical_key, "match": "normalized", "confidence": 95}

            for label in config["labels"]:
                label_norm = self._normalize(label)
                if canonical_key == "revenue":
                    if "costofrevenue" in normalized:
                        continue
                if len(normalized) >= 5 and (normalized in label_norm or label_norm in normalized):
                    return {"canonical": canonical_key, "match": "partial", "confidence": 75}

        # token-based fuzzy match as last resort
        best = {"canonical": None, "score": 0.0}
        for canonical_key in CANONICAL_MAP.keys():
            score = self._token_match(tokens, canonical_key)
            if score > best["score"]:
                best = {"canonical": canonical_key, "score": score}
        if best["canonical"] and best["score"] >= 0.5:
            return {"canonical": best["canonical"], "match": "token", "confidence": 70}

        return {"canonical": None, "match": "no_match", "confidence": 0}

    def resolve_all(self, raw_data: dict, sector: str = None) -> dict:
        resolved = {}
        unresolved = []

        for label, value in (raw_data or {}).items():
            result = self.resolve(label, sector)
            if result["canonical"]:
                key = result["canonical"]
                cfg = CANONICAL_MAP.get(key, {})
                raw_norm = self._normalize(label)
                if key == "cost_of_revenue":
                    if "costofrevenueproduct" in raw_norm:
                        resolved["cost_revenue_product"] = value
                    elif "costofrevenueservice" in raw_norm:
                        resolved["cost_revenue_service"] = value
                if key in resolved:
                    if key == "cost_of_revenue":
                        if "costofrevenueproduct" in raw_norm or "costofrevenueservice" in raw_norm:
                            try:
                                resolved[key] = float(resolved[key]) + float(value)
                            except (TypeError, ValueError):
                                pass
                        else:
                            try:
                                if float(value) > float(resolved[key]):
                                    resolved[key] = value
                            except (TypeError, ValueError):
                                pass
                    elif cfg.get("aggregation_rule") in {"sum_if_multiple", "sum_components"}:
                        try:
                            resolved[key] = float(resolved[key]) + float(value)
                        except (TypeError, ValueError):
                            pass
                    elif cfg.get("dedup_rule") == "take_largest":
                        try:
                            if float(value) > float(resolved[key]):
                                resolved[key] = value
                        except (TypeError, ValueError):
                            pass
                else:
                    resolved[key] = value
            else:
                unresolved.append(label)

        return {
            "data": resolved,
            "unresolved": unresolved,
            "coverage_pct": (len(resolved) / len(raw_data) * 100 if raw_data else 0),
        }
