from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class OntologyNode:
    id: str
    label: str
    parent_id: Optional[str]
    depth_level: int
    xbrl_concepts: List[str]
    calculation_type: str
    sign_rule: str
    validation_rule: str
    is_core: bool
    is_structural: bool
    valid_profiles: List[str]
    materiality_weight: float
    risk_weight: float
    children: List[str] = field(default_factory=list)


class HierarchicalFinancialOntology:
    """
    Hierarchical parent-child ontology with tolerant computation behavior.
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, OntologyNode] = {}
        self._build_default_tree()

    def _build_default_tree(self) -> None:
        profiles = ['industrial', 'bank', 'insurance', 'reit', 'investment_firm', 'utility', 'energy']

        def add(node: OntologyNode) -> None:
            self.nodes[node.id] = node
            if node.parent_id and node.parent_id in self.nodes:
                self.nodes[node.parent_id].children.append(node.id)

        add(OntologyNode('IS', 'Income Statement', None, 0, [], 'SUM', 'NATURAL', 'structure', True, True, profiles, 1.0, 0.8))
        add(OntologyNode('IS.REV', 'Revenue', 'IS', 1,
                         ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, profiles, 1.0, 0.7))
        add(OntologyNode('IS.COGS', 'Cost of Revenue', 'IS', 1,
                         ['CostOfRevenue', 'CostOfGoodsAndServicesSold'],
                         'DIRECT', 'NEGATIVE', 'non_negative_abs', True, False, ['industrial'], 0.9, 0.6))
        add(OntologyNode('IS.GP', 'Gross Profit', 'IS', 1,
                         ['GrossProfit'],
                         'DERIVED', 'POSITIVE', 'rev_minus_cogs', True, False, ['industrial', 'reit'], 0.9, 0.7))
        add(OntologyNode('IS.OP', 'Operating Income', 'IS', 1,
                         ['OperatingIncomeLoss'],
                         'DIRECT', 'POSITIVE', 'margin_bounds', True, False, profiles, 0.9, 0.8))
        add(OntologyNode('IS.NET', 'Net Income', 'IS', 1,
                         ['NetIncomeLoss', 'ProfitLoss'],
                         'DIRECT', 'POSITIVE', 'margin_bounds', True, False, profiles, 1.0, 0.9))

        add(OntologyNode('BS', 'Balance Sheet', None, 0, [], 'BALANCING', 'NATURAL', 'balance', True, True, profiles, 1.0, 1.0))
        add(OntologyNode('BS.ASSETS', 'Total Assets', 'BS', 1,
                         ['Assets'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, profiles, 1.0, 1.0))
        add(OntologyNode('BS.LIAB', 'Total Liabilities', 'BS', 1,
                         ['Liabilities'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, profiles, 1.0, 1.0))
        add(OntologyNode('BS.EQ', 'Total Equity', 'BS', 1,
                         ['StockholdersEquity'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, profiles, 1.0, 1.0))

        add(OntologyNode('CF', 'Cash Flow', None, 0, [], 'SUM', 'NATURAL', 'cash_bridge', True, True, profiles, 0.8, 0.7))
        add(OntologyNode('CF.OCF', 'Operating Cash Flow', 'CF', 1,
                         ['NetCashProvidedByUsedInOperatingActivities'],
                         'DIRECT', 'POSITIVE', 'cash_flow_sign', True, False, profiles, 0.9, 0.9))
        add(OntologyNode('CF.CAPEX', 'Capital Expenditures', 'CF', 1,
                         ['PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpenditures'],
                         'DIRECT', 'NEGATIVE', 'cash_flow_sign', True, False, profiles, 0.8, 0.8))
        add(OntologyNode('CF.FCF', 'Free Cash Flow', 'CF', 1,
                         [],
                         'DERIVED', 'POSITIVE', 'ocf_minus_capex', True, False, profiles, 1.0, 0.9))

        # Profile-specific structural leaves (tolerant if missing)
        add(OntologyNode('IS.NII', 'Net Interest Income', 'IS', 1,
                         ['NetInterestIncome', 'InterestIncomeExpenseNet'],
                         'DIRECT', 'POSITIVE', 'interest_consistency', True, False, ['bank'], 0.9, 0.9))
        add(OntologyNode('BS.LOANS', 'Loans', 'BS', 1,
                         ['LoansAndLeasesReceivableNetReportedAmount', 'LoansHeldForInvestmentNet', 'LoansReceivableNet'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, ['bank'], 0.9, 0.9))
        add(OntologyNode('BS.DEPOSITS', 'Deposits', 'BS', 1,
                         ['Deposits', 'InterestBearingDeposits', 'NoninterestBearingDeposits'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, ['bank'], 0.9, 0.9))
        add(OntologyNode('BS.CET1', 'CET1 Capital', 'BS', 1,
                         ['CommonEquityTier1CapitalAmount', 'CommonEquityTier1CapitalRatio'],
                         'DIRECT', 'POSITIVE', 'capital_adequacy', True, False, ['bank'], 1.0, 1.0))
        add(OntologyNode('IS.PREMIUM', 'Premium Revenue', 'IS', 1,
                         ['PremiumsEarnedNet', 'InsurancePremiumRevenue'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, ['insurance'], 0.9, 0.9))
        add(OntologyNode('IS.FFO', 'FFO Proxy', 'IS', 1,
                         ['FundsFromOperations'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, ['reit'], 0.9, 0.8))
        add(OntologyNode('IS.REG_REV', 'Regulated Revenue', 'IS', 1,
                         ['UtilityRevenue', 'RegulatedUtilityRevenue'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, ['utility'], 0.9, 0.8))
        add(OntologyNode('IS.HC_REV', 'Hydrocarbon Revenue', 'IS', 1,
                         ['OilAndGasRevenue', 'ExplorationAndProductionRevenue'],
                         'DIRECT', 'POSITIVE', 'non_negative', True, False, ['energy'], 0.9, 0.9))

    def get_nodes_for_profile(self, profile: str) -> List[OntologyNode]:
        if profile == 'unknown':
            # fail-closed fallback: keep only core structural nodes for conservative mapping
            return [n for n in self.nodes.values() if n.is_core]
        return [n for n in self.nodes.values() if profile in n.valid_profiles]

    def to_records(self) -> List[Dict]:
        return [n.__dict__.copy() for n in self.nodes.values()]
