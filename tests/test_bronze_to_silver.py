import pandas as pd

from transform.bronze_to_silver import _clean_and_cast, _standardize_columns


def test_bronze_to_silver_adds_load_ts_and_cleans():
    raw = pd.DataFrame({
        "Snapshot Date": ["2026-01-01"],
        "Origin": [" jfk "],
        "Dest": ["lax"],
        "Depart Date": ["2026-02-01"],
        "Price USD": ["199.99"],
    })

    df = _standardize_columns(raw)
    df = _clean_and_cast(df)

    assert "load_ts" in df.columns
    assert df.loc[0, "origin"] == "JFK"
    assert df.loc[0, "dest"] == "LAX"
    assert float(df.loc[0, "price_usd"]) > 0
