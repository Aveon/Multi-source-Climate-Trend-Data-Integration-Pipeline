import argparse
import csv
import gzip
import json
import logging
import math
import requests
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

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
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
METEOSTAT_STATIONS_URL = "https://bulk.meteostat.net/v2/stations/lite.json.gz"
METEOSTAT_DAILY_BULK_URL = "https://data.meteostat.net/daily/{year}/{station}.csv.gz"


def parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a date object."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def normalize_value(value):
    """Convert NaN-like dataframe values into JSON-friendly Python values."""
    if value is None:
        return None

    try:
        if math.isnan(value):
            return None
    except TypeError:
        pass

    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            return value

    return value


def parse_optional_float(value: Optional[str]) -> Optional[float]:
    """Parse a numeric string when present and return None for blanks."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute the approximate great-circle distance between two coordinates."""
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    return 2 * radius_km * math.asin(math.sqrt(a))


def station_has_daily_coverage(station_meta: Dict[str, object], start_date: date, end_date: date) -> bool:
    """Check whether a Meteostat station advertises daily coverage for the target window."""
    inventory = station_meta.get("inventory") or {}
    daily_inventory = inventory.get("daily") or {}
    start_value = daily_inventory.get("start")
    end_value = daily_inventory.get("end")
    if not start_value or not end_value:
        return False
    return parse_date(start_value) <= start_date and parse_date(end_value) >= end_date


def station_has_daily_overlap(station_meta: Dict[str, object], start_date: date, end_date: date) -> bool:
    """Check whether a Meteostat station overlaps the requested window at all."""
    inventory = station_meta.get("inventory") or {}
    daily_inventory = inventory.get("daily") or {}
    start_value = daily_inventory.get("start")
    end_value = daily_inventory.get("end")
    if not start_value or not end_value:
        return False

    available_start = parse_date(start_value)
    available_end = parse_date(end_value)
    return available_start <= end_date and available_end >= start_date


def station_covers_daily_start(station_meta: Dict[str, object], start_date: date) -> bool:
    """Check whether a Meteostat station reaches back to the requested start date."""
    inventory = station_meta.get("inventory") or {}
    daily_inventory = inventory.get("daily") or {}
    start_value = daily_inventory.get("start")
    end_value = daily_inventory.get("end")
    if not start_value or not end_value:
        return False

    available_start = parse_date(start_value)
    available_end = parse_date(end_value)
    return available_start <= start_date and available_end >= start_date


def download_stations_catalog(
    session: requests.Session,
    *,
    cache_path: Path,
    stations_url: str,
    timeout: int = 120,
) -> List[Dict[str, object]]:
    """Download and cache the Meteostat station catalog used for ID resolution."""
    if cache_path.exists() and cache_path.stat().st_size > 0:
        with gzip.open(cache_path, "rt", encoding="utf-8") as fh:
            return json.load(fh)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    response = session.get(stations_url, timeout=timeout)
    response.raise_for_status()
    cache_path.write_bytes(response.content)
    with gzip.open(cache_path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def build_catalog_indexes(catalog: List[Dict[str, object]]) -> tuple[Dict[str, Dict[str, object]], Dict[str, Dict[str, object]], Dict[str, Dict[str, object]]]:
    """Build reusable station lookup maps for Meteostat identifier resolution."""
    by_id = {item.get("id"): item for item in catalog if item.get("id")}
    by_national: Dict[str, Dict[str, object]] = {}
    by_icao: Dict[str, Dict[str, object]] = {}
    for item in catalog:
        identifiers = item.get("identifiers") or {}
        national = identifiers.get("national")
        icao = identifiers.get("icao")
        if national:
            by_national[national] = item
        if icao:
            by_icao[icao] = item
    return by_id, by_national, by_icao


def resolve_meteostat_station(
    station: Dict[str, str],
    catalog: List[Dict[str, object]],
    *,
    by_id: Dict[str, Dict[str, object]],
    by_national: Dict[str, Dict[str, object]],
    by_icao: Dict[str, Dict[str, object]],
    start_date: date,
    end_date: date,
) -> Dict[str, object]:
    """Resolve one manifest row to a Meteostat station using IDs or nearest coverage."""
    def finalize_candidate(candidate: Dict[str, object], *, matched_by: str) -> Dict[str, object]:
        resolved = dict(candidate)
        resolved["resolved_match_type"] = matched_by
        return resolved

    explicit_id = station.get("meteostat_station_id")
    if explicit_id and explicit_id in by_id:
        candidate = by_id[explicit_id]
        if station_covers_daily_start(candidate, start_date):
            return finalize_candidate(candidate, matched_by="meteostat_station_id")

    noaa_station_id = station.get("noaa_station_id", "")
    noaa_code = noaa_station_id.split(":", 1)[-1] if noaa_station_id else ""
    if noaa_code and noaa_code in by_national:
        candidate = by_national[noaa_code]
        if station_covers_daily_start(candidate, start_date):
            return finalize_candidate(candidate, matched_by="national")

    nws_station_id = station.get("nws_station_id", "")
    if nws_station_id and nws_station_id in by_icao:
        candidate = by_icao[nws_station_id]
        if station_covers_daily_start(candidate, start_date):
            return finalize_candidate(candidate, matched_by="icao")

    query_latitude = parse_optional_float(
        station.get("meteostat_query_latitude")
        or station.get("latitude")
    )
    query_longitude = parse_optional_float(
        station.get("meteostat_query_longitude")
        or station.get("longitude")
    )
    if query_latitude is None or query_longitude is None:
        raise ValueError(f"Station is missing identifiers and coordinates for Meteostat: {station.get('noaa_station_id')}")

    state = station.get("state")

    def collect_nearest_candidates(country_filter: Optional[str]) -> list[tuple[float, Dict[str, object]]]:
        candidates = []
        for item in catalog:
            if not station_covers_daily_start(item, start_date):
                continue
            if country_filter and item.get("country") and item.get("country") != country_filter:
                continue
            if state and item.get("region") and item.get("region") != state:
                continue
            location = item.get("location") or {}
            item_latitude = location.get("latitude")
            item_longitude = location.get("longitude")
            if item_latitude is None or item_longitude is None:
                continue
            distance_km = haversine_km(query_latitude, query_longitude, float(item_latitude), float(item_longitude))
            candidates.append((distance_km, item))
        return candidates

    station_country = station.get("country")
    candidates = collect_nearest_candidates(station_country)
    if not candidates and station_country:
        candidates = collect_nearest_candidates(None)

    if not candidates:
        raise ValueError(f"No Meteostat station with daily coverage found for {station.get('noaa_station_id')}")

    candidates.sort(key=lambda item: item[0])
    nearest_distance_km, nearest_station = candidates[0]
    if nearest_distance_km > 100:
        raise ValueError(
            f"Nearest Meteostat station for {station.get('noaa_station_id')} is too far away "
            f"({nearest_distance_km:.1f} km)"
        )
    nearest_station = dict(nearest_station)
    nearest_station["resolved_distance_km"] = round(nearest_distance_km, 2)
    nearest_station["resolved_match_type"] = "nearest"
    return nearest_station


def iter_years(start_date: date, end_date: date) -> Iterable[int]:
    """Yield all calendar years covered by the requested date range."""
    for year in range(start_date.year, end_date.year + 1):
        yield year


def fetch_station_year_bytes(
    session: requests.Session,
    *,
    station_id: str,
    year: int,
    daily_base_url: str,
    timeout: int = 120,
    max_attempts: int = 5,
    backoff_seconds: float = 1.0,
) -> Optional[bytes]:
    """Download one Meteostat daily bulk CSV and return None when that year is absent."""
    station_url = daily_base_url.format(year=year, station=station_id)

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(station_url, timeout=timeout)
            logger.debug(
                "HTTP %s %s station=%s year=%s attempt=%s/%s",
                response.status_code,
                station_url,
                station_id,
                year,
                attempt,
                max_attempts,
            )

            if response.status_code == 404:
                return None

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == max_attempts:
                    response.raise_for_status()

                sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Retrying Meteostat bulk download after HTTP %s for station=%s year=%s in %.1fs",
                    response.status_code,
                    station_id,
                    year,
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
                "Retrying Meteostat bulk download after %s for station=%s year=%s in %.1fs",
                type(exc).__name__,
                station_id,
                year,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to fetch Meteostat bulk data after {max_attempts} attempts for station={station_id} year={year}")


def iter_station_records(
    session: requests.Session,
    station: Dict[str, str],
    resolved_station: Dict[str, object],
    *,
    run_date: str,
    start_date: date,
    end_date: date,
    daily_base_url: str,
) -> Iterable[Dict[str, object]]:
    """Yield raw Meteostat JSONL records from annual station CSV dumps."""
    resolved_station_id = resolved_station["id"]
    resolved_name = resolved_station.get("name")
    if isinstance(resolved_name, dict):
        resolved_name = resolved_name.get("en")
    resolved_location = resolved_station.get("location") or {}

    for year in iter_years(start_date, end_date):
        payload = fetch_station_year_bytes(
            session,
            station_id=resolved_station_id,
            year=year,
            daily_base_url=daily_base_url,
        )
        if payload is None:
            continue

        text_stream = gzip.decompress(payload).decode("utf-8")
        reader = csv.DictReader(text_stream.splitlines())
        for row in reader:
            row_date = row.get("date") or row.get("time")
            if row_date:
                day = parse_date(row_date)
            else:
                year_value = row.get("year")
                month_value = row.get("month")
                day_value = row.get("day")
                if not year_value or not month_value or not day_value:
                    continue
                day = date(int(year_value), int(month_value), int(day_value))
            if day < start_date or day > end_date:
                continue

            yield {
                "source": "meteostat",
                "run_date": run_date,
                "provider": "meteostat_bulk",
                "noaa_station_id": station.get("noaa_station_id"),
                "nws_station_id": station.get("nws_station_id"),
                "station_name": station.get("station_name"),
                "state": station.get("state"),
                "country": station.get("country"),
                "latitude": station.get("latitude"),
                "longitude": station.get("longitude"),
                "meteostat_station_id": resolved_station_id,
                "meteostat_station_name": resolved_name,
                "meteostat_station_latitude": resolved_location.get("latitude"),
                "meteostat_station_longitude": resolved_location.get("longitude"),
                "meteostat_station_elevation_m": resolved_location.get("elevation"),
                "meteostat_station_distance_km": resolved_station.get("resolved_distance_km"),
                "meteostat_match_type": resolved_station.get("resolved_match_type"),
                "date": day.isoformat(),
                "temp": normalize_value(parse_optional_float(row.get("temp"))),
                "tmin": normalize_value(parse_optional_float(row.get("tmin"))),
                "tmax": normalize_value(parse_optional_float(row.get("tmax"))),
                "prcp": normalize_value(parse_optional_float(row.get("prcp"))),
                "snwd": normalize_value(parse_optional_float(row.get("snwd"))),
                "wspd": normalize_value(parse_optional_float(row.get("wspd"))),
                "wpgt": normalize_value(parse_optional_float(row.get("wpgt"))),
            }


def load_completed_station_ids(out_path: Path) -> Set[str]:
    """Read an existing Meteostat raw file and return completed station ids."""
    if not out_path.exists() or out_path.stat().st_size == 0:
        return set()

    station_ids: Set[str] = set()
    with out_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed Meteostat row while scanning %s", out_path)
                continue

            station_id = record.get("noaa_station_id")
            if station_id:
                station_ids.add(station_id)

    return station_ids


def append_failed_station(
    failed_path: Path,
    *,
    run_date: str,
    station: Dict[str, str],
    start_date: str,
    end_date: str,
    error: Exception,
) -> None:
    """Record station-level Meteostat failures for later replay."""
    payload = {
        "run_date": run_date,
        "source": "meteostat",
        "noaa_station_id": station.get("noaa_station_id"),
        "nws_station_id": station.get("nws_station_id"),
        "station_name": station.get("station_name"),
        "state": station.get("state"),
        "start_date": start_date,
        "end_date": end_date,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "logged_at": datetime.now(timezone.utc).isoformat(),
    }
    with failed_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def fetch_station_history(
    station: Dict[str, str],
    *,
    start_date: date,
    end_date: date,
    fields: List[str],
):
    """Fetch Meteostat daily point data for one station location."""
    try:
        import meteostat as ms
    except ModuleNotFoundError as exc:
        raise RuntimeError("Meteostat is not installed. Run `pip install -r requirements.txt`.") from exc

    query_latitude, query_longitude = build_query_coordinates(station)
    point = ms.Point(query_latitude, query_longitude)
    timeseries = ms.daily(point, start_date, end_date, parameters=fields)
    return timeseries.fetch()


def main(argv: Optional[list] = None) -> int:
    """Fetch Meteostat daily history from bulk station files for each configured station."""
    parser = argparse.ArgumentParser(description="Fetch Meteostat historical daily bulk data and save raw JSONL")
    parser.add_argument("--stations-csv", required=True, help="CSV manifest with Meteostat-ready coordinates")
    parser.add_argument("--start-date", required=True, help="Historical window start date in YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Historical window end date in YYYY-MM-DD")
    parser.add_argument("--out-dir", default=None, help="Directory to save raw Meteostat JSONL")
    parser.add_argument("--date", default=None, help="Run date in YYYY-MM-DD (defaults to today UTC)")
    parser.add_argument("--limit-stations", type=int, default=None, help="Maximum number of stations to ingest from the manifest")
    parser.add_argument("--station-offset", type=int, default=0, help="Zero-based enabled-station offset for batch runs")
    parser.add_argument("--cache-dir", default=None, help="Directory to cache Meteostat catalog downloads")
    parser.add_argument("--stations-url", default=METEOSTAT_STATIONS_URL, help="Bulk URL for the Meteostat station catalog")
    parser.add_argument("--daily-base-url", default=METEOSTAT_DAILY_BULK_URL, help="Template URL for Meteostat daily bulk CSV files")
    parser.add_argument("--fail-on-station-error", action="store_true", help="Stop the run when any station fails")
    parser.add_argument("--no-resume", action="store_true", help="Start a fresh Meteostat raw file instead of appending to an existing one")
    args = parser.parse_args(argv)

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path(args.out_dir or (DATA_DIR / "raw" / "meteostat" / f"run_date={run_date}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "meteostat_daily.jsonl"
    failed_path = out_dir / "meteostat_failed_stations.jsonl"
    cache_dir = Path(args.cache_dir or (DATA_DIR / "cache" / "meteostat"))
    catalog_cache_path = cache_dir / "stations_lite.json.gz"

    stations = load_station_manifest(
        args.stations_csv,
        start_index=args.station_offset,
        limit=args.limit_stations,
    )
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    session = requests.Session()
    catalog = download_stations_catalog(
        session,
        cache_path=catalog_cache_path,
        stations_url=args.stations_url,
    )
    by_id, by_national, by_icao = build_catalog_indexes(catalog)

    resume_enabled = not args.no_resume
    completed_station_ids = load_completed_station_ids(out_path) if resume_enabled else set()
    open_mode = "a" if resume_enabled and out_path.exists() else "w"
    if not resume_enabled and failed_path.exists():
        failed_path.unlink()

    record_count = 0
    skipped_existing = 0
    failed_stations = 0
    with out_path.open(open_mode, encoding="utf-8") as fh:
        for station_index, station in enumerate(stations, start=1):
            station_id = station.get("noaa_station_id") or station.get("nws_station_id") or station.get("station_name") or "unknown"
            if station_id in completed_station_ids:
                skipped_existing += 1
                logger.debug(
                    "Skipping Meteostat station=%s (%s/%s) because it is already present in %s",
                    station_id,
                    station_index,
                    len(stations),
                    out_path,
                )
                continue

            try:
                resolved_station = resolve_meteostat_station(
                    station,
                    catalog,
                    by_id=by_id,
                    by_national=by_national,
                    by_icao=by_icao,
                    start_date=start_date,
                    end_date=end_date,
                )
                station_records = list(
                    iter_station_records(
                        session,
                        station,
                        resolved_station,
                        run_date=run_date,
                        start_date=start_date,
                        end_date=end_date,
                        daily_base_url=args.daily_base_url,
                    )
                )
            except Exception as exc:
                failed_stations += 1
                append_failed_station(
                    failed_path,
                    run_date=run_date,
                    station=station,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    error=exc,
                )
                if args.fail_on_station_error:
                    raise
                logger.error(
                    "Skipping Meteostat station=%s after %s: %s",
                    station_id,
                    type(exc).__name__,
                    exc,
                )
                continue

            for record in station_records:
                fh.write(json.dumps(record, sort_keys=True) + "\n")

            station_record_count = len(station_records)
            record_count += station_record_count
            fh.flush()
            completed_station_ids.add(station_id)
    logger.debug(
        "Saved %d Meteostat rows to %s (skipped_existing=%s failed_stations=%s)",
        record_count,
        out_path,
        skipped_existing,
        failed_stations,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
