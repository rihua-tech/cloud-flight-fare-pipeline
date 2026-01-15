import pandas as pd
from transform.validate_silver import validate_df

def test_validation_fails_missing_required_columns():
    df = pd.DataFrame({"origin": ["JFK"], "dest": ["LAX"]})
    result = validate_df(df)
    assert result["ok"] is False
    assert any("missing_required_columns" in x for x in result["issues"])

def test_validation_fails_price_non_positive():
    df = pd.DataFrame({
        "snapshot_date": ["2026-01-01"],
        "origin": ["JFK"],
        "dest": ["LAX"],
        "depart_date": ["2026-02-01"],
        "price_usd": [0],
        "load_ts": ["2026-01-01T00:00:00Z"],
    })
    result = validate_df(df)
    assert result["ok"] is False
    assert any("price_usd_non_positive_rows" in x for x in result["issues"])
