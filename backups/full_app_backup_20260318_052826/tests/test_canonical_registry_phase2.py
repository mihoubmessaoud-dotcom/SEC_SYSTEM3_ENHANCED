import unittest

from modules.canonical_registry import get_priority_list, is_not_applicable
from modules.canonical_resolver import resolve_item


class CanonicalRegistryPhase2Tests(unittest.TestCase):
    def test_sector_priority_prefers_technology_revenue_tags(self):
        tags = get_priority_list("annual_revenue", "technology")
        self.assertTrue(len(tags) > 0)
        self.assertEqual(tags[0], "RevenueFromContractWithCustomerExcludingAssessedTax")

    def test_not_applicable_for_bank_inventory(self):
        self.assertTrue(is_not_applicable("inventory", "bank"))
        self.assertTrue(is_not_applicable("annual_cogs", "bank"))

    def test_resolver_returns_not_applicable_contract(self):
        out = resolve_item(
            2025,
            "inventory",
            candidates=[],
            require_fy=False,
            allow_negative=True,
            sector_profile="bank",
        )
        self.assertIsNone(out.get("value"))
        self.assertIn("not_applicable_for_sector", str(out.get("selection_reason") or ""))


if __name__ == "__main__":
    unittest.main()

