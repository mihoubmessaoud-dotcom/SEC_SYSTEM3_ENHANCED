import unittest

from sec_ratio_app import FactObservation, RatioEngine


def obs(concept, value, start=None, end="2024-12-31", fy=2024, unit="USD", form="10-K"):
    return FactObservation(
        taxonomy="us-gaap",
        concept=concept,
        tag=f"us-gaap:{concept}",
        unit=unit,
        value=float(value),
        decimals=0,
        start=start,
        end=end,
        fy=fy,
        fp="FY",
        form=form,
        filed="2025-01-31",
        accn="0000000000-25-000001",
        frame=None,
    )


class RatioEngineUnitTests(unittest.TestCase):
    def test_current_ratio_formula(self):
        engine = RatioEngine(computation_timestamp="2026-02-15T00:00:00Z")
        result = engine._compute_division(
            name="Current Ratio",
            formula="Current Assets / Current Liabilities",
            numerator=("current_assets", obs("AssetsCurrent", 200)),
            denominator=("current_liabilities", obs("LiabilitiesCurrent", 100)),
            period_type="instant",
        )
        self.assertEqual(result["status"], "Computed")
        self.assertAlmostEqual(result["value"], 2.0)

    def test_missing_input_not_computable(self):
        engine = RatioEngine(computation_timestamp="2026-02-15T00:00:00Z")
        result = engine._compute_division(
            name="Debt to Equity",
            formula="Total Liabilities / Equity",
            numerator=("total_liabilities", obs("Liabilities", 300)),
            denominator=("equity", None),
            period_type="instant",
        )
        self.assertEqual(result["status"], "Not Computable")
        self.assertIn("Missing input", result["not_computable_reason"])

    def test_period_mismatch_rejection(self):
        engine = RatioEngine(computation_timestamp="2026-02-15T00:00:00Z")
        ni = obs("NetIncomeLoss", 50, start="2024-01-01", end="2024-12-31")
        assets = obs("Assets", 500, start=None, end="2023-12-31")
        result = engine._compute_mixed_ratio(
            name="ROA",
            formula="Net Income / Total Assets",
            duration_input=("net_income", ni),
            instant_input=("assets", assets),
        )
        self.assertEqual(result["status"], "Not Computable")
        self.assertIn("Period mismatch", result["not_computable_reason"])

    def test_audit_log_generation(self):
        engine = RatioEngine(computation_timestamp="2026-02-15T00:00:00Z")
        gp = obs("GrossProfit", 400, start="2024-01-01", end="2024-12-31")
        rev = obs("Revenues", 1000, start="2024-01-01", end="2024-12-31")
        result = engine._compute_margin("Gross Margin", "gross_profit", gp, rev)
        self.assertEqual(result["status"], "Computed")
        self.assertIn("inputs", result)
        self.assertEqual(len(result["inputs"]), 2)
        self.assertTrue(result["inputs"][0]["tag"].startswith("us-gaap:"))
        self.assertEqual(result["computation_timestamp"], "2026-02-15T00:00:00Z")


if __name__ == "__main__":
    unittest.main()

