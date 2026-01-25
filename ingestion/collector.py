#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

import requests
from dotenv import load_dotenv


# ──────────────────────────────────────────────────────────────────────────────
# Paths + env
# repo root is parent of ingestion/
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")


# ──────────────────────────────────────────────────────────────────────────────
# Config
@dataclass(frozen=True)
class Config:
    api_key: str
    currency: str = "usd"
    market: str = "us"
    days_ahead: int = 150
    timeout_sec: int = 15
    max_retries: int = 3
    sleep_between_calls_sec: float = 0.2

    # ✅ Prefer AIRPORT IATA codes (more reliable than city codes like TYO)
    origins: Tuple[str, ...] = ("JFK", "LAX", "SFO", "ATL", "ORD")
    dests: Tuple[str, ...] = ("LHR", "CDG", "DXB", "HND", "SIN")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
def utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def split_codes(s: str) -> Tuple[str, ...]:
    codes = [c.strip().upper() for c in s.split(",") if c.strip()]
    return tuple(codes)


def safe_int(v) -> str:
    """Return int-like values as string; empty if missing/invalid."""
    if v is None or v == "":
        return ""
    try:
        return str(int(v))
    except Exception:
        return ""


def parse_depart_date(raw: str) -> Optional[date]:
    if not raw:
        return None
    raw = raw.strip()
    try:
        # can be "YYYY-MM-DDTHH:MM:SS-05:00" or "YYYY-MM-DD" or "...Z"
        s = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.date()
    except Exception:
        # fallback to date-only
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d").date()
        except Exception:
            return None


# ──────────────────────────────────────────────────────────────────────────────
# API
def fetch_latest_prices(cfg: Config, origin: str, dest: str, session: requests.Session) -> dict:
    """
    Travelpayouts: /aviasales/v3/get_latest_prices

    Behavior:
    - 400: invalid route/code -> return empty (do NOT fail job)
    - 429: rate limit -> backoff + retry
    - other errors -> retry then raise
    """
    url = "https://api.travelpayouts.com/aviasales/v3/get_latest_prices"
    params = {
        "origin": origin,
        "destination": dest,
        "currency": cfg.currency,
        "market": cfg.market,
        "period_type": "year",
        "token": cfg.api_key,
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, cfg.max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=cfg.timeout_sec)

            if resp.status_code == 400:
                return {"success": False, "data": []}

            if resp.status_code == 429:
                time.sleep(1.5 * attempt)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            last_err = e
            time.sleep(0.8 * attempt)

    raise RuntimeError(f"API failed for {origin}->{dest}: {last_err}")


# ──────────────────────────────────────────────────────────────────────────────
# Bronze writer
def write_bronze_snapshot(cfg: Config) -> Path:
    snapshot_date = date.today().isoformat()
    scrape_ts = utc_now_iso_z()
    cutoff = date.today() + timedelta(days=cfg.days_ahead)

    out_dir = REPO_ROOT / "data" / "bronze" / f"dt={snapshot_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Stable filename downstream expects
    out_file = out_dir / "fares.csv"

    # Temp file prevents partial writes + helps Windows behavior
    tmp_file = out_dir / f"fares.tmp.{os.getpid()}.csv"

    header = [
        "snapshot_date",
        "origin",
        "dest",
        "depart_date",
        "price_usd",
        "scrape_ts",
        "gate",
        "trip_class",
        "number_of_changes",
    ]

    written = 0
    skipped_invalid = 0
    warns = 0

    # ✅ utf-8-sig helps Excel display non-English text correctly
    with requests.Session() as session, open(tmp_file, "w", newline="", encoding="utf-8-sig") as fp:
        w = csv.writer(fp)
        w.writerow(header)

        for origin in cfg.origins:
            for dest in cfg.dests:
                if origin == dest:
                    continue

                try:
                    payload = fetch_latest_prices(cfg, origin, dest, session)
                    data = payload.get("data") or []

                    if not data and payload.get("success") is False:
                        skipped_invalid += 1
                        continue

                    for item in data:
                        dep = parse_depart_date(item.get("depart_date", ""))
                        if not dep or dep > cutoff:
                            continue

                        price = item.get("value")
                        if price is None:
                            continue

                        w.writerow([
                            snapshot_date,
                            origin,
                            dest,
                            dep.isoformat(),
                            float(price),
                            scrape_ts,
                            (item.get("gate") or ""),
                            safe_int(item.get("trip_class")),
                            safe_int(item.get("number_of_changes")),
                        ])
                        written += 1

                except Exception as e:
                    warns += 1
                    print(f"[WARN] {origin}->{dest}: {e}")

                time.sleep(cfg.sleep_between_calls_sec)

    # Atomic replace into fares.csv
    try:
        os.replace(tmp_file, out_file)
    except PermissionError:
        # fares.csv is locked (open in Excel/VSCode). Write a fallback instead.
        safe_ts = scrape_ts.replace(":", "-")
        fallback = out_dir / f"fares_{safe_ts}_{os.getpid()}.csv"
        os.replace(tmp_file, fallback)
        print(f"[WARN] fares.csv is locked (close Excel/VSCode preview). Wrote: {fallback}")
        out_file = fallback

    print(f"[OK] wrote {out_file}")
    print(f"     rows_written={written}, skipped_invalid_pairs={skipped_invalid}, warns={warns}")
    return out_file


# ──────────────────────────────────────────────────────────────────────────────
# CLI
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--origins", default=os.getenv("HOT_ORIGINS", ""), help="Comma list (e.g. JFK,LAX,SFO)")
    ap.add_argument("--dests", default=os.getenv("HOT_DESTS", ""), help="Comma list (e.g. LHR,CDG,DXB)")
    args = ap.parse_args()

    api_key = os.getenv("TRAVELPAYOUTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing TRAVELPAYOUTS_API_KEY in repo-root .env")

    origins = split_codes(args.origins) if args.origins else Config.origins
    dests = split_codes(args.dests) if args.dests else Config.dests

    cfg = Config(
        api_key=api_key,
        currency=os.getenv("CURRENCY", "usd"),
        market=os.getenv("MARKET", "us"),
        days_ahead=int(os.getenv("DAYS_AHEAD", "150")),
        timeout_sec=int(os.getenv("API_TIMEOUT_SEC", "15")),
        max_retries=int(os.getenv("API_MAX_RETRIES", "3")),
        sleep_between_calls_sec=float(os.getenv("API_SLEEP_SEC", "0.2")),
        origins=origins,
        dests=dests,
    )

    write_bronze_snapshot(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
