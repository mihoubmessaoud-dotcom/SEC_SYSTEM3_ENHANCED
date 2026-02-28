import unittest

from modules.ratio_engine import RatioEngine


class RatioEngineExplainableStatusTests(unittest.TestCase):
    def test_missing_sec_inputs_reason_code(self):
        eng = RatioEngine()
        c = eng._contract(None, 0, "missing_ratio_from_engine", "roa", {})
        out = eng._finalize_contract("roa", c)
        self.assertEqual(out["status"], "NOT_COMPUTABLE")
        self.assertEqual(out["reason"], "MISSING_SEC_CONCEPT")

    def test_market_dependent_missing_market_data(self):
        eng = RatioEngine()
        c = eng._build_base_contract("pe_ratio", None)
        out = eng._finalize_contract("pe_ratio", c)
        self.assertEqual(out["status"], "NOT_COMPUTABLE")
        self.assertEqual(out["reason"], "MISSING_MARKET_DATA")
        self.assertEqual(out["ratio_type"], "MARKET_DEPENDENT")

    def test_zero_denominator_structured_output(self):
        eng = RatioEngine()
        rev = {"value": 0.0, "tag": "us-gaap:Revenues", "period_end": "2024-12-31"}
        cogs = {"value": 100.0, "tag": "us-gaap:CostOfRevenue", "period_end": "2024-12-31"}
        c = eng._compute_gross_margin(rev, cogs)
        out = eng._finalize_contract("gross_margin", c)
        self.assertEqual(out["status"], "NOT_COMPUTABLE")
        self.assertEqual(out["reason"], "ZERO_DENOMINATOR")

    def test_not_computable_has_audit_fields(self):
        eng = RatioEngine()
        c = eng._contract(None, 0, "missing_ratio_from_engine", "inventory_turnover", {})
        c["input_tags"] = ["us-gaap:InventoryNet", "us-gaap:CostOfRevenue"]
        c["inputs"] = {"inventory": None, "cogs": None}
        out = eng._finalize_contract("inventory_turnover", c)
        self.assertEqual(out["status"], "NOT_COMPUTABLE")
        self.assertIsNotNone(out["formula_used"])
        self.assertIn("input_concepts", out)
        self.assertIn("raw_values_used", out)
        self.assertIn("computation_timestamp", out)


if __name__ == "__main__":
    unittest.main()
