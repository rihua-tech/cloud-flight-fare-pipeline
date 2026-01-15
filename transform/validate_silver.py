# transform/validate_silver.py
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from transform.contract import REQUIRED_COLUMNS, NULL_THRESHOLDS, ACCEPTED_CABIN

def validate_df(df: pd.DataFrame) -> dict:
    issues = []

    # row count
    if len(df) == 0:
        issues.append("row_count is 0")

    # required columns exist
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        issues.append(f"missing_required_columns: {missing}")

    # null thresholds
    for col, max_null in NULL_THRESHOLDS.items():
        if col in df.columns:
            null_rate = float(df[col].isna().mean())
            if null_rate > max_null:
                issues.append(f"null_rate_too_high: {col}={null_rate:.3f} > {max_null:.3f}")

    # price > 0
    if "price_usd" in df.columns:
        bad = int((df["price_usd"] <= 0).sum(skipna=True))
        if bad > 0:
            issues.append(f"price_usd_non_positive_rows: {bad}")

    # accepted values example
    if "cabin" in df.columns:
        bad_vals = sorted(set(df["cabin"].dropna().astype(str)) - ACCEPTED_CABIN)
        if bad_vals:
            issues.append(f"invalid_cabin_values: {bad_vals[:10]}")

    return {
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "issues": issues,
        "ok": len(issues) == 0,
    }

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--path", default="data/silver/flight_fares.parquet")
    p.add_argument("--report", default="analytics/outputs/validation_report.json")
    args = p.parse_args()

    df = pd.read_parquet(args.path)
    result = validate_df(df)

    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(result, indent=2), encoding="utf-8")

    if not result["ok"]:
        raise SystemExit(f"[FAILED] Validation issues: {result['issues']}")
    print(f"[OK] Validation passed. Report -> {args.report}")

if __name__ == "__main__":
    main()
