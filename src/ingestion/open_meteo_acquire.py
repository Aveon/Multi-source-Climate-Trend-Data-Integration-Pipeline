import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

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
OPEN_METEO_API_URL = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_DAILY_FIELDS = [
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_mean",
    "wind_speed_10m_max",
]


def build_params(
    station: Dict[str, str],
    *,
    start_date: str,
    end_date: str,
    model: str,
    timezone_name: str,
    daily_fields: List[str],
) -> Dict[str, object]:
    latitude = station.get("open_meteo_query_latitude") or station.get("latitude")
    longitude = station.get("open_meteo_query_longitude") or station.get("longitude")
    if not latitude or not longitude:
        raise ValueError(f"Station is missing coordinates for Open-Meteo: {station.get('noaa_station_id')}")

    return {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": timezone_name,
        "models": model,
        "daily": daily_fields,
        "temperature_unit": "celsius",
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
    }


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch Open-Meteo historical daily data and save raw JSONL")
    parser.add_argument("--stations-csv", required=True, help="CSV manifest with Open-Meteo-ready coordinates")
    parser.add_argument("--start-date", required=True, help="Historical window start date in YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Historical window end date in YYYY-MM-DD")
    parser.add_argument("--model", default="era5", help="Open-Meteo historical model (use era5 for decade trends)")
    parser.add_argument("--timezone", default="auto", help="Timezone for daily aggregates")
    parser.add_argument("--daily-fields", default="temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_mean,wind_speed_10m_max", help="Comma-separated daily fields")
    parser.add_argument("--user-agent", type=str, default="BigDataProject (mailto:joshua.young96@gmail.com)", help="User-Agent string with contact info")
    parser.add_argument("--out-dir", default=None, help="Directory to save raw Open-Meteo JSONL")
    parser.add_argument("--date", default=None, help="Run date in YYYY-MM-DD (defaults to today UTC)")
    parser.add_argument("--limit-stations", type=int, default=None, help="Maximum number of stations to ingest from the manifest")
    args = parser.parse_args(argv)

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path(args.out_dir or (DATA_DIR / "raw" / "open_meteo" / f"run_date={run_date}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "open_meteo_daily.jsonl"

    stations = load_station_manifest(args.stations_csv, limit=args.limit_stations)
    daily_fields = [part.strip() for part in args.daily_fields.split(",") if part.strip()] or DEFAULT_DAILY_FIELDS

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent, "Accept": "application/json"})

    record_count = 0
    with out_path.open("w", encoding="utf-8") as fh:
        for station in stations:
            params = build_params(
                station,
                start_date=args.start_date,
                end_date=args.end_date,
                model=args.model,
                timezone_name=args.timezone,
                daily_fields=daily_fields,
            )
            response = session.get(OPEN_METEO_API_URL, params=params, timeout=60)
            logger.info(
                "HTTP %s %s station=%s lat=%s lon=%s",
                response.status_code,
                OPEN_METEO_API_URL,
                station.get("noaa_station_id"),
                params["latitude"],
                params["longitude"],
            )
            response.raise_for_status()
            payload = response.json()

            daily = payload.get("daily", {})
            dates = daily.get("time", [])
            for idx, daily_date in enumerate(dates):
                record = {
                    "source": "open_meteo",
                    "run_date": run_date,
                    "model": args.model,
                    "timezone": payload.get("timezone"),
                    "elevation": payload.get("elevation"),
                    "noaa_station_id": station.get("noaa_station_id"),
                    "nws_station_id": station.get("nws_station_id"),
                    "station_name": station.get("station_name"),
                    "state": station.get("state"),
                    "country": station.get("country"),
                    "latitude": station.get("latitude"),
                    "longitude": station.get("longitude"),
                    "open_meteo_query_latitude": station.get("open_meteo_query_latitude") or station.get("latitude"),
                    "open_meteo_query_longitude": station.get("open_meteo_query_longitude") or station.get("longitude"),
                    "resolved_latitude": payload.get("latitude"),
                    "resolved_longitude": payload.get("longitude"),
                    "date": daily_date,
                }
                for field_name in daily_fields:
                    values = daily.get(field_name, [])
                    record[field_name] = values[idx] if idx < len(values) else None

                fh.write(json.dumps(record, sort_keys=True) + "\n")
                record_count += 1

            time.sleep(0.1)

    logger.info("Saved %d Open-Meteo rows to %s", record_count, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
