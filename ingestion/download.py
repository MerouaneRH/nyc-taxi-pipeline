"""Download NYC TLC monthly trip files and the taxi zone lookup.

Usage:
    uv run python ingestion/download.py --service yellow --months 2024-01 2024-02
    uv run python ingestion/download.py --zones
"""

import argparse
import re
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
SERVICES = {"yellow", "green", "fhv", "fhvhv"}
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def trip_url(service: str, month: str) -> str:
    if service not in SERVICES:
        raise ValueError(f"unknown service {service!r}, expected one of {sorted(SERVICES)}")
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    return f"{BASE_URL}/trip-data/{service}_tripdata_{month}.parquet"


def download(url: str, dest: Path) -> Path:
    """Stream url to dest. Skips files already downloaded so reruns are cheap."""
    if dest.exists() and dest.stat().st_size > 0:
        print(f"skip  {dest.name} (already present)")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=60, allow_redirects=False) as resp:
        if 300 <= resp.status_code < 400:
            raise ValueError(f"refusing redirect ({resp.status_code}) from {url}")
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            f.writelines(resp.iter_content(chunk_size=1 << 20))
    tmp.rename(dest)  # only a complete file ever gets the final name
    print(f"saved {dest.name} ({dest.stat().st_size / 1e6:.0f} MB)")
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--service", choices=sorted(SERVICES))
    parser.add_argument("--months", nargs="+", default=[])
    parser.add_argument("--zones", action="store_true", help="download taxi_zone_lookup.csv")
    args = parser.parse_args()

    if args.zones:
        download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", RAW_DIR / "taxi_zone_lookup.csv")
    for month in args.months:
        if not args.service:
            parser.error("--months requires --service")
        url = trip_url(args.service, month)
        download(url, RAW_DIR / args.service / Path(url).name)


if __name__ == "__main__":
    main()
