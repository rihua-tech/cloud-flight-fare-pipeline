import pandas as pd

from transform.bronze_to_silver import _clean_and_cast, _standardize_columns


# Week 6 minimal helper test: aligns to sql/analysis/lead_time_buckets.sql bucket logic.
def lead_time_bucket(days: int) -> str:
    if days < 7:
        return "0-6"
    if days < 14:
        return "7-13"
    if days < 30:
        return "14-29"
    return "30+"


def test_lead_time_bucket_boundaries():
    assert lead_time_bucket(0) == "0-6"
    assert lead_time_bucket(6) == "0-6"
    assert lead_time_bucket(7) == "7-13"
    assert lead_time_bucket(13) == "7-13"
    assert lead_time_bucket(14) == "14-29"
    assert lead_time_bucket(29) == "14-29"
    assert lead_time_bucket(30) == "30+"


def test_transform_normalizes_and_dedupes_rows():
    raw = pd.DataFrame(
        {
            "Snapshot Date": ["2026-01-01", "2026-01-01", "2026-01-01"],
            "Origin": [" jfk ", "JFK", "SFO"],
            "Dest": ["lax ", "LAX", "SEA"],
            "Depart Date": ["2026-01-10", "2026-01-10", "2026-01-20"],
            "Price USD": ["100.50", "100.50", "-25"],
        }
    )

    df = _standardize_columns(raw)
    df = _clean_and_cast(df)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["origin"] == "JFK"
    assert row["dest"] == "LAX"
    assert float(row["price_usd"]) == 100.5
    assert "load_ts" in df.columns
