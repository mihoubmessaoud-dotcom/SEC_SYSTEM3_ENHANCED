import uuid
from pathlib import Path

import pandas as pd

from modules.institutional.engine import EngineConfig, InstitutionalFinancialIntelligenceEngine


def test_deep_audit_reconciliation_hierarchy_and_balance():
    layer1 = pd.DataFrame(
        [
            {'Tag': 'TotalAssets_Hierarchy', '2023': 1000.0},
            {'Tag': 'Assets', '2023': 900.0},
            {'Tag': 'TotalLiabilities_Hierarchy', '2023': 400.0},
            {'Tag': 'TotalEquity_Hierarchy', '2023': 580.0},
            {'Tag': 'MiscGapPlug', '2023': 20.0},
            {'Tag': 'SegmentAdjustmentNoise', '2023': 99999.0},
        ]
    )
    layer1_path = Path(f"_tmp_deep_audit_{uuid.uuid4().hex}.csv")
    try:
        layer1.to_csv(layer1_path, index=False)

        engine = InstitutionalFinancialIntelligenceEngine(EngineConfig(output_dir='outputs'))
        out = engine._deep_audit_reconciliation(layer1_path)

        assets_row = out[(out['Year'] == 2023) & (out['Indicator'] == 'Total Assets')].iloc[0]
        diff_row = out[(out['Year'] == 2023) & (out['Indicator'] == 'Balance Difference')].iloc[0]

        assert float(assets_row['Value']) == 1000.0
        assert float(diff_row['Value']) == 0.0
        assert int(assets_row['Reliability']) == 100
        assert 'Hierarchy-first parent' in str(assets_row['Reason'])
    finally:
        try:
            layer1_path.unlink(missing_ok=True)
        except Exception:
            pass
