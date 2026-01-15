# transform/bronze_to_silver.py
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from transform.contract import REQUIRED_COLUMNS

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df

def _clean_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Standardize string fields
    if "origin" in df.columns:
        df["origin"] = df["origin"].astype(str).str.strip().str.upper()
    if "dest" in df.columns:
        df["dest"] = df["dest"].astype(str).str.strip().str.upper()

    # Cast dates
    for c in ["snapshot_date", "depart_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # Cast price
    if "price_usd" in df.columns:
        df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")

    # Drop rows missing required cols
    df = df.dropna(subset=[c for c in REQUIRED_COLUMNS if c in df.columns])

    # Price must be > 0
    if "price_usd" in df.columns:
        df = df[df["price_usd"] > 0]

    # Deduplicate (define a stable key)
    dedupe_cols = [c for c in ["snapshot_date", "origin", "dest", "depart_date", "price_usd"] if c in df.columns]
    if dedupe_cols:
        df = df.drop_duplicates(subset=dedupe_cols, keep="last")

    # Add load timestamp (freshness)
    df["load_ts"] = datetime.now(timezone.utc)

    return df

def read_bronze_csvs(input_dir: Path) -> pd.DataFrame:
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    dfs = []
    for f in files:
        dfs.append(pd.read_csv(f))
    return pd.concat(dfs, ignore_index=True)

def write_silver_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/bronze", help="Bronze folder with CSV files")
    p.add_argument("--output", default="data/silver/flight_fares.parquet", help="Silver parquet output path")
    args = p.parse_args()

    input_dir = Path(args.input)
    output_path = Path(args.output)

    df = read_bronze_csvs(input_dir)
    df = _standardize_columns(df)
    df = _clean_and_cast(df)

    # Final check: required columns exist
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in transformed output: {missing}")

    write_silver_parquet(df, output_path)
    print(f"[OK] Wrote silver parquet: {output_path} rows={len(df)} cols={len(df.columns)}")

if __name__ == "__main__":
    main()
