import pandas as pd


def test_dedupe_timeseries_by_canonical_tag_collapses_alias_rows():
    from core.raw_export_dedupe import dedupe_timeseries_by_canonical_tag

    df = pd.DataFrame(
        [
            {
                "البند": "إجمالي الأصول (Assets)",
                "__concept__": "إجمالي الأصول (Assets)",
                "2015": 7201.0,
                "2016": 7370.0,
                "الوحدة": "ملايين دولار أمريكي",
            },
            {
                "البند": "إجمالي الأصول (TotalAssets)",
                "__concept__": "إجمالي الأصول (TotalAssets)",
                "2015": 7201.0,
                "2016": 7370.0,
                "الوحدة": "ملايين دولار أمريكي",
            },
            # A different tag should remain.
            {
                "البند": "الأصول المتداولة (AssetsCurrent)",
                "__concept__": "الأصول المتداولة (AssetsCurrent)",
                "2015": 5713.0,
                "2016": 6053.0,
                "الوحدة": "ملايين دولار أمريكي",
            },
        ]
    )

    out = dedupe_timeseries_by_canonical_tag(
        df,
        label_col="البند",
        year_cols=["2015", "2016"],
        unit_col="الوحدة",
        concept_col="__concept__",
    )

    # Assets aliases collapse to one row; AssetsCurrent remains.
    assert len(out) == 2
    labels = set(out["البند"].tolist())
    assert any("الأصول المتداولة" in s for s in labels)
    assert sum("إجمالي الأصول" in s for s in labels) == 1

