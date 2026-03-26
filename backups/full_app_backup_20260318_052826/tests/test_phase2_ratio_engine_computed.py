import unittest

from modules.ratio_engine import RatioEngine


class Phase2RatioEngineComputedTests(unittest.TestCase):
    def test_engine_computes_operating_net_roa_current(self):
        eng = RatioEngine()
        data_by_year = {
            2024: {
                "Revenues": 1000.0,
                "CostOfRevenue": 600.0,
                "OperatingIncomeLoss": 180.0,
                "NetIncomeLoss": 120.0,
                "Assets": 2000.0,
                "AssetsCurrent": 500.0,
                "LiabilitiesCurrent": 250.0,
                "StockholdersEquity": 800.0,
            },
            2023: {
                "Assets": 1800.0,
                "StockholdersEquity": 700.0,
            },
        }
        out = eng.build(data_by_year, {2024: {}})
        r = out["ratios"][2024]

        self.assertAlmostEqual(r["operating_margin"]["value"], 0.18, places=8)
        self.assertAlmostEqual(r["net_margin"]["value"], 0.12, places=8)
        # ROA uses average assets: (2000+1800)/2 = 1900
        self.assertAlmostEqual(r["roa"]["value"], 120.0 / 1900.0, places=8)
        self.assertAlmostEqual(r["current_ratio"]["value"], 2.0, places=8)


if __name__ == "__main__":
    unittest.main()

