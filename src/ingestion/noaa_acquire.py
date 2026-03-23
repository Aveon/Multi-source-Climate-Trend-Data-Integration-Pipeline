import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

THIS_DIR = Path(__file__).resolve().parent
SRC_ROOT = THIS_DIR.parent
PROJECT_ROOT = SRC_ROOT.parent
for path in (str(PROJECT_ROOT), str(SRC_ROOT), str(THIS_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

try:
    from src.ingestion.station_manifest import load_station_manifest
except ModuleNotFoundError:
    try:
        from ingestion.station_manifest import load_station_manifest
    except ModuleNotFoundError:
        from station_manifest import load_station_manifest


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = PROJECT_ROOT / "data"
NOAA_API_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
DEFAULT_DATATYPES = ["TAVG", "TMAX", "TMIN", "PRCP", "AWND", "SNOW", "SNWD"]


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_year_windows(start_date: date, end_date: date) -> Iterable[Tuple[date, date]]:
    current = start_date
    while current <= end_date:
        window_end = min(date(current.year, 12, 31), end_date)
        yield current, window_end
        current = window_end + timedelta(days=1)


def build_request_params(
    dataset_id: str,
    station_id: str,
    start_date: date,
    end_date: date,
    datatypes: List[str],
    *,
    limit: int,
    offset: int,
) -> Dict[str, object]:
    return {
        "datasetid": dataset_id,
        "stationid": station_id,
        "startdate": start_date.isoformat(),
        "enddate": end_date.isoformat(),
        "datatypeid": datatypes,
        "units": "metric",
        "limit": limit,
        "offset": offset,
        "includemetadata": "true",
    }


def fetch_page(
    session: requests.Session,
    *,
    token: str,
    user_agent: str,
    params: Dict[str, object],
    timeout: int = 60,
) -> Dict[str, object]:
    headers = {
        "token": token,
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    response = session.get(NOAA_API_URL, headers=headers, params=params, timeout=timeout)
    logger.info(
        "HTTP %s %s station=%s %s..%s offset=%s",
        response.status_code,
        NOAA_API_URL,
        params["stationid"],
        params["startdate"],
        params["enddate"],
        params["offset"],
    )
    response.raise_for_status()
    return response.json()


def json_line(record: Dict[str, object]) -> str:
    return json.dumps(record, sort_keys=True)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch NOAA CDO daily data and save raw JSONL")
    parser.add_argument("--stations-csv", required=True, help="CSV manifest with noaa_station_id values")
    parser.add_argument("--token", required=True, help="NOAA CDO API token")
    parser.add_argument("--user-agent", type=str, default="BigDataProject (mailto:joshua.young96@gmail.com)", help="User-Agent string with contact info")
    parser.add_argument("--start-date", required=True, help="Historical window start date in YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Historical window end date in YYYY-MM-DD")
    parser.add_argument("--dataset-id", default="GHCND", help="NOAA dataset id")
    parser.add_argument("--datatypes", default="TAVG,TMAX,TMIN,PRCP,AWND,SNOW,SNWD", help="Comma-separated NOAA datatype IDs")
    parser.add_argument("--out-dir", default=None, help="Directory to save raw NOAA JSONL")
    parser.add_argument("--date", default=None, help="Run date in YYYY-MM-DD (defaults to today UTC)")
    parser.add_argument("--limit-stations", type=int, default=None, help="Maximum number of stations to ingest from the manifest")
    args = parser.parse_args(argv)

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path(args.out_dir or (DATA_DIR / "raw" / "noaa" / f"run_date={run_date}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "noaa_daily.jsonl"

    stations = load_station_manifest(args.stations_csv, limit=args.limit_stations)
    datatypes = [part.strip() for part in args.datatypes.split(",") if part.strip()] or DEFAULT_DATATYPES
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    session = requests.Session()
    request_count = 0
    record_count = 0

    with out_path.open("w", encoding="utf-8") as fh:
        for station in stations:
            noaa_station_id = station.get("noaa_station_id", "")
            if not noaa_station_id:
                continue

            for window_start, window_end in iter_year_windows(start_date, end_date):
                offset = 1
                limit = 1000

                while True:
                    params = build_request_params(
                        args.dataset_id,
                        noaa_station_id,
                        window_start,
                        window_end,
                        datatypes,
                        limit=limit,
                        offset=offset,
                    )
                    payload = fetch_page(
                        session,
                        token=args.token,
                        user_agent=args.user_agent,
                        params=params,
                    )
                    request_count += 1
                    results = payload.get("results", [])

                    for item in results:
                        raw_value = item.get("value")
                        try:
                            value = float(raw_value) if raw_value is not None else None
                        except (TypeError, ValueError):
                            value = None

                        record = {
                            "source": "noaa",
                            "run_date": run_date,
                            "noaa_dataset_id": args.dataset_id,
                            "noaa_station_id": noaa_station_id,
                            "station_name": station.get("station_name"),
                            "state": station.get("state"),
                            "country": station.get("country"),
                            "latitude": station.get("latitude"),
                            "longitude": station.get("longitude"),
                            "nws_station_id": station.get("nws_station_id"),
                            "date": item.get("date", "")[:10],
                            "datatype": item.get("datatype"),
                            "value": value,
                            "attributes": item.get("attributes"),
                            "request_window_start": window_start.isoformat(),
                            "request_window_end": window_end.isoformat(),
                        }
                        fh.write(json_line(record) + "\n")
                        record_count += 1

                    metadata = payload.get("metadata", {}).get("resultset", {})
                    total = int(metadata.get("count", len(results)))
                    if not results or offset + limit > total:
                        break

                    offset += limit
                    time.sleep(0.25)

                time.sleep(0.25)

    logger.info(
        "Saved %d NOAA rows from %d requests to %s",
        record_count,
        request_count,
        out_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
