"""Pull NYC taxi trips from the NYC Open Data (Socrata SODA) REST API.

Use this for small or incremental pulls. For bulk history use download.py.

Usage:
    uv run python ingestion/soda.py --since 2024-01-01 --until 2024-01-02
    uv run python ingestion/soda.py --since 2024-01-01 --until 2024-01-08 --max-rows 50000

Set SODA_APP_TOKEN in the environment for higher rate limits (optional).
"""

import argparse
import os
import re
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://data.cityofnewyork.us/resource"
YELLOW_2024 = "rwwi-khbc"  # 2024 Yellow Taxi Trip Data
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "soda"

DATETIME_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
STRING_COLS = {"store_and_fwd_flag"}


def check_date(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"date must look like YYYY-MM-DD, got {value!r}")
    return value


def fetch_pages(dataset: str, since: str, until: str, page_size: int, max_rows: int | None):
    """Yield lists of rows. Ordered by :id so offset paging is stable."""
    headers = {"X-App-Token": os.environ["SODA_APP_TOKEN"]} if "SODA_APP_TOKEN" in os.environ else {}
    where = f"tpep_pickup_datetime >= '{check_date(since)}T00:00:00' AND tpep_pickup_datetime < '{check_date(until)}T00:00:00'"
    offset = 0
    while max_rows is None or offset < max_rows:
        limit = page_size if max_rows is None else min(page_size, max_rows - offset)
        resp = requests.get(
            f"{BASE_URL}/{dataset}.json",
            params={"$where": where, "$order": ":id", "$limit": limit, "$offset": offset},
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return
        yield rows
        offset += len(rows)
        if len(rows) < limit:
            return


def to_frame(rows: list[dict]) -> pd.DataFrame:
    """SODA returns every value as a string; cast to real types."""
    df = pd.DataFrame(rows)
    for col in df.columns:
        if col in DATETIME_COLS:
            df[col] = pd.to_datetime(df[col])
        elif col not in STRING_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", default=YELLOW_2024, help="Socrata dataset id")
    parser.add_argument("--since", required=True, help="pickup >= YYYY-MM-DD")
    parser.add_argument("--until", required=True, help="pickup < YYYY-MM-DD")
    parser.add_argument("--page-size", type=int, default=50_000)
    parser.add_argument("--max-rows", type=int, help="stop after this many rows")
    args = parser.parse_args()

    rows = [r for page in fetch_pages(args.dataset, args.since, args.until, args.page_size, args.max_rows) for r in page]
    if not rows:
        print("no rows returned")
        return
    dest = RAW_DIR / f"{args.dataset}_{args.since}_{args.until}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    to_frame(rows).to_parquet(tmp, index=False)
    tmp.rename(dest)  # only a complete file ever gets the final name
    print(f"saved {dest.name} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
