import json
from pathlib import Path
import unittest


class TestReleaseV4Baseline(unittest.TestCase):
    def _load_json(self, rel_path: str):
        path = Path(rel_path)
        self.assertTrue(path.exists(), f"Missing baseline artifact: {rel_path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _preview_to_map(preview):
        out = {}
        for row in preview or []:
            k = row.get("Metric")
            if k:
                out[k] = row.get("Value")
        return out

    def test_release_stamp_exists(self):
        stamp = self._load_json("config/release_stamp.json")
        self.assertEqual(stamp.get("version"), "v4-definitive")
        self.assertEqual(stamp.get("status"), "FINAL_TECHNICAL_CLOSURE")

    def test_quick_eval_acceptance_floor(self):
        data = self._load_json("outputs/quick_eval_20260227_latest.json")
        self.assertTrue(data, "quick_eval baseline is empty")

        required_sheets = {
            "Raw_by_Year",
            "Ratios",
            "Strategic",
            "Final_Acceptance",
            "Ratio_Audit",
            "Balance_Check",
            "Comparative_Analysis",
        }

        for workbook, payload in data.items():
            sheets = set(payload.get("sheets", []))
            self.assertTrue(
                required_sheets.issubset(sheets),
                f"{workbook}: missing required sheets",
            )

            preview = self._preview_to_map(payload.get("final_acceptance_preview", []))
            verdict = str(preview.get("Verdict", "")).strip().upper()
            score = float(preview.get("Final_Professional_Score", 0.0) or 0.0)
            critical_flags = int(preview.get("Critical_Flag_Count", 999) or 0)

            self.assertIn(
                verdict,
                {"APPROVED_FOR_EXPERT_REVIEW", "APPROVED"},
                f"{workbook}: non-approved verdict",
            )
            self.assertGreaterEqual(
                score, 95.0, f"{workbook}: score dropped below baseline floor"
            )
            self.assertEqual(
                critical_flags, 0, f"{workbook}: critical flags detected"
            )

    def test_multi_company_validation_no_missing_success(self):
        data = self._load_json("outputs/final_multi_company_validation.json")
        self.assertTrue(data, "final_multi_company_validation is empty")
        for ticker, payload in data.items():
            self.assertTrue(payload.get("success"), f"{ticker}: success flag is false")


if __name__ == "__main__":
    unittest.main()
