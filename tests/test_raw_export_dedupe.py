from __future__ import annotations

import pandas as pd

from core.raw_export_dedupe import dedupe_labeled_timeseries_df


def test_dedupe_drops_identical_duplicates():
    df = pd.DataFrame(
        [
            {"البند": "الذمم الدائنة", "2016": 296.0, "2017": 485.0, "الوحدة": "ملايين دولار أمريكي", "__concept__": "AccountsPayableCurrent"},
            {"البند": "الذمم الدائنة", "2016": 296.0, "2017": 485.0, "الوحدة": "ملايين دولار أمريكي", "__concept__": "AccountsPayable"},
        ]
    )
    out = dedupe_labeled_timeseries_df(df, label_col="البند", year_cols=["2016", "2017"], unit_col="الوحدة")
    assert len(out) == 1
    assert out.iloc[0]["البند"] == "الذمم الدائنة"


def test_dedupe_disambiguates_conflicting_duplicates():
    df = pd.DataFrame(
        [
            {"البند": "الأصول المتداولة", "2016": 6053.0, "2017": 8536.0, "الوحدة": "ملايين دولار أمريكي", "__concept__": "AssetsCurrent"},
            {"البند": "الأصول المتداولة", "2016": 541.0, "2017": 599.0, "الوحدة": "ملايين دولار أمريكي", "__concept__": "CurrentAssets_Legacy"},
        ]
    )
    out = dedupe_labeled_timeseries_df(df, label_col="البند", year_cols=["2016", "2017"], unit_col="الوحدة")
    assert len(out) == 2
    labels = sorted(out["البند"].astype(str).tolist())
    assert labels[0] != labels[1]
    assert any("AssetsCurrent" in s or "CurrentAssets_Legacy" in s for s in labels)

