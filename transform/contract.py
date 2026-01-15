# transform/contract.py
from __future__ import annotations

REQUIRED_COLUMNS = [
    "snapshot_date",
    "origin",
    "dest",
    "depart_date",
    "price_usd",
]

# optional columns you may have (won't fail if missing)
OPTIONAL_COLUMNS = [
    "cabin",
    "carrier",
    "currency",
]

# accepted values example (only apply if column exists)
ACCEPTED_CABIN = {"economy", "premium_economy", "business", "first"}

# null thresholds (max % null allowed)
NULL_THRESHOLDS = {
    "snapshot_date": 0.0,
    "origin": 0.0,
    "dest": 0.0,
    "depart_date": 0.0,
    "price_usd": 0.0,
}
