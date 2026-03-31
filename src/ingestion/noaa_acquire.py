import argparse
import csv
import gzip
import io
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
NOAA_BULK_BY_STATION_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/{station_code}.csv.gz"
DEFAULT_DATATYPES = ["TAVG", "TMAX", "TMIN", "PRCP", "AWND", "SNOW", "SNWD"]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
VALUE_SCALE_FACTORS = {
    "TAVG": 10.0,
    "TMAX": 10.0,
    "TMIN": 10.0,
    "PRCP": 10.0,
    "AWND": 10.0,
}


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
    max_attempts: int = 5,
    backoff_seconds: float = 1.0,
) -> Dict[str, object]:
    """Fetch a single page of NOAA CDO data with retries for transient failures."""
    headers = {
        "token": token,
        "User-Agent": user_agent,
        "Accept": "application/json",
    }

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(NOAA_API_URL, headers=headers, params=params, timeout=timeout)
            logger.info(
                "HTTP %s %s station=%s %s..%s offset=%s attempt=%s/%s",
                response.status_code,
                NOAA_API_URL,
                params["stationid"],
                params["startdate"],
                params["enddate"],
                params["offset"],
                attempt,
                max_attempts,
            )

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == max_attempts:
                    response.raise_for_status()

                sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Retrying NOAA request after HTTP %s for station=%s window=%s..%s offset=%s in %.1fs",
                    response.status_code,
                    params["stationid"],
                    params["startdate"],
                    params["enddate"],
                    params["offset"],
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                continue

            response.raise_for_status()
            return response.json()

        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == max_attempts:
                raise

            sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Retrying NOAA request after %s for station=%s window=%s..%s offset=%s in %.1fs",
                type(exc).__name__,
                params["stationid"],
                params["startdate"],
                params["enddate"],
                params["offset"],
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"Failed to fetch NOAA data after {max_attempts} attempts "
        f"for station={params['stationid']} window={params['startdate']}..{params['enddate']} "
        f"offset={params['offset']}"
    )


def json_line(record: Dict[str, object]) -> str:
    return json.dumps(record, sort_keys=True)


def normalize_bulk_value(datatype: str, raw_value: str) -> Optional[float]:
    """Convert a GHCN bulk CSV value into the metric units used by the pipeline."""
    if raw_value in {"", None, "-9999"}:
        return None

    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None

    scale = VALUE_SCALE_FACTORS.get(datatype)
    if scale:
        return value / scale
    return value


def build_common_record(
    station: Dict[str, str],
    *,
    run_date: str,
    dataset_id: str,
    station_id: str,
    day: date,
    datatype: str,
    value: Optional[float],
    attributes: Optional[str],
    request_window_start: str,
    request_window_end: str,
) -> Dict[str, object]:
    """Build the raw NOAA JSONL row used by downstream processing."""
    return {
        "source": "noaa",
        "run_date": run_date,
        "noaa_dataset_id": dataset_id,
        "noaa_station_id": station_id,
        "station_name": station.get("station_name"),
        "state": station.get("state"),
        "country": station.get("country"),
        "latitude": station.get("latitude"),
        "longitude": station.get("longitude"),
        "nws_station_id": station.get("nws_station_id"),
        "date": day.isoformat(),
        "datatype": datatype,
        "value": value,
        "attributes": attributes,
        "request_window_start": request_window_start,
        "request_window_end": request_window_end,
    }


def build_bulk_station_url(base_url: str, station_id: str) -> str:
    """Translate a manifest NOAA station id into the matching GHCN bulk file URL."""
    station_code = station_id.split(":", 1)[-1]
    return base_url.format(station_code=station_code)


def fetch_bulk_station_bytes(
    session: requests.Session,
    *,
    station_id: str,
    base_url: str,
    timeout: int = 120,
    max_attempts: int = 5,
    backoff_seconds: float = 1.0,
) -> bytes:
    """Download one NOAA bulk station file with retries for transient failures."""
    station_url = build_bulk_station_url(base_url, station_id)

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(station_url, timeout=timeout)
            logger.info(
                "HTTP %s %s station=%s attempt=%s/%s",
                response.status_code,
                station_url,
                station_id,
                attempt,
                max_attempts,
            )

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == max_attempts:
                    response.raise_for_status()

                sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Retrying NOAA bulk download after HTTP %s for station=%s in %.1fs",
                    response.status_code,
                    station_id,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                continue

            response.raise_for_status()
            return response.content

        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == max_attempts:
                raise

            sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Retrying NOAA bulk download after %s for station=%s in %.1fs",
                type(exc).__name__,
                station_id,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to fetch NOAA bulk data after {max_attempts} attempts for station={station_id}")


def iter_bulk_station_records(
    session: requests.Session,
    station: Dict[str, str],
    *,
    run_date: str,
    dataset_id: str,
    start_date: date,
    end_date: date,
    datatypes: List[str],
    base_url: str,
) -> Iterable[Dict[str, object]]:
    """Yield NOAA raw records from the official station CSV bulk files."""
    noaa_station_id = station.get("noaa_station_id", "")
    if not noaa_station_id:
        return

    payload = fetch_bulk_station_bytes(
        session,
        station_id=noaa_station_id,
        base_url=base_url,
    )
    text_stream = io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(payload)), encoding="utf-8")
    reader = csv.reader(text_stream)
    datatype_filter = set(datatypes)

    for row in reader:
        if len(row) < 8:
            continue

        station_id, raw_day, datatype, raw_value, mflag, qflag, sflag, obs_time = row[:8]
        if datatype not in datatype_filter:
            continue

        try:
            day = datetime.strptime(raw_day, "%Y%m%d").date()
        except ValueError:
            continue
        if day < start_date or day > end_date:
            continue

        attributes = ",".join(
            [
                f"mflag={mflag or ''}",
                f"qflag={qflag or ''}",
                f"sflag={sflag or ''}",
                f"obs_time={obs_time or ''}",
            ]
        )
        yield build_common_record(
            station,
            run_date=run_date,
            dataset_id=dataset_id,
            station_id=station_id if ":" in station_id else f"{dataset_id}:{station_id}",
            day=day,
            datatype=datatype,
            value=normalize_bulk_value(datatype, raw_value),
            attributes=attributes,
            request_window_start=start_date.isoformat(),
            request_window_end=end_date.isoformat(),
        )


def write_bulk_station_records(
    fh,
    session: requests.Session,
    stations: List[Dict[str, str]],
    *,
    run_date: str,
    dataset_id: str,
    start_date: date,
    end_date: date,
    datatypes: List[str],
    base_url: str,
) -> Tuple[int, int]:
    """Write NOAA bulk station records to JSONL and return download/row counts."""
    download_count = 0
    record_count = 0

    for station in stations:
        noaa_station_id = station.get("noaa_station_id", "")
        if not noaa_station_id:
            continue

        logger.info(
            "Downloading NOAA bulk station file for station=%s window=%s..%s",
            noaa_station_id,
            start_date.isoformat(),
            end_date.isoformat(),
        )
        wrote_station_rows = False
        for record in iter_bulk_station_records(
            session,
            station,
            run_date=run_date,
            dataset_id=dataset_id,
            start_date=start_date,
            end_date=end_date,
            datatypes=datatypes,
            base_url=base_url,
        ):
            if not wrote_station_rows:
                download_count += 1
                wrote_station_rows = True
            fh.write(json_line(record) + "\n")
            record_count += 1

    return download_count, record_count


def write_api_records(
    fh,
    session: requests.Session,
    stations: List[Dict[str, str]],
    *,
    run_date: str,
    dataset_id: str,
    start_date: date,
    end_date: date,
    datatypes: List[str],
    token: str,
    user_agent: str,
) -> Tuple[int, int]:
    """Write NOAA API results to JSONL and return request/row counts."""
    request_count = 0
    record_count = 0

    for station in stations:
        noaa_station_id = station.get("noaa_station_id", "")
        if not noaa_station_id:
            continue

        for window_start, window_end in iter_year_windows(start_date, end_date):
            offset = 1
            limit = 1000

            while True:
                params = build_request_params(
                    dataset_id,
                    noaa_station_id,
                    window_start,
                    window_end,
                    datatypes,
                    limit=limit,
                    offset=offset,
                )
                payload = fetch_page(
                    session,
                    token=token,
                    user_agent=user_agent,
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

                    record = build_common_record(
                        station,
                        run_date=run_date,
                        dataset_id=dataset_id,
                        station_id=noaa_station_id,
                        day=parse_date(item.get("date", "")[:10]),
                        datatype=item.get("datatype"),
                        value=value,
                        attributes=item.get("attributes"),
                        request_window_start=window_start.isoformat(),
                        request_window_end=window_end.isoformat(),
                    )
                    fh.write(json_line(record) + "\n")
                    record_count += 1

                metadata = payload.get("metadata", {}).get("resultset", {})
                total = int(metadata.get("count", len(results)))
                if not results or offset + limit > total:
                    break

                offset += limit

    return request_count, record_count


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch NOAA CDO daily data and save raw JSONL")
    parser.add_argument("--stations-csv", required=True, help="CSV manifest with noaa_station_id values")
    parser.add_argument("--token", required=False, default="", help="NOAA CDO API token")
    parser.add_argument("--user-agent", type=str, default="BigDataProject (mailto:joshua.young96@gmail.com)", help="User-Agent string with contact info")
    parser.add_argument("--start-date", required=True, help="Historical window start date in YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Historical window end date in YYYY-MM-DD")
    parser.add_argument("--dataset-id", default="GHCND", help="NOAA dataset id")
    parser.add_argument("--datatypes", default="TAVG,TMAX,TMIN,PRCP,AWND,SNOW,SNWD", help="Comma-separated NOAA datatype IDs")
    parser.add_argument("--ingest-mode", choices=["bulk_station", "api"], default="bulk_station", help="NOAA ingestion strategy")
    parser.add_argument("--bulk-base-url", default=NOAA_BULK_BY_STATION_URL, help="Template URL for NOAA bulk station CSV files")
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

    with out_path.open("w", encoding="utf-8") as fh:
        if args.ingest_mode == "bulk_station":
            request_count, record_count = write_bulk_station_records(
                fh,
                session,
                stations,
                run_date=run_date,
                dataset_id=args.dataset_id,
                start_date=start_date,
                end_date=end_date,
                datatypes=datatypes,
                base_url=args.bulk_base_url,
            )
            logger.info(
                "Saved %d NOAA rows from %d bulk station downloads to %s",
                record_count,
                request_count,
                out_path,
            )
            return 0

        if not args.token:
            raise ValueError("NOAA token is required when ingest-mode=api")

        request_count, record_count = write_api_records(
            fh,
            session,
            stations,
            run_date=run_date,
            dataset_id=args.dataset_id,
            start_date=start_date,
            end_date=end_date,
            datatypes=datatypes,
            token=args.token,
            user_agent=args.user_agent,
        )

    logger.info("Saved %d NOAA rows from %d requests to %s", record_count, request_count, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
