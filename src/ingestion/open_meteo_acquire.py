import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

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
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class OpenMeteoRateLimitError(RuntimeError):
    """Carry Open-Meteo cooldown guidance after repeated 429 responses."""

    def __init__(self, station_id: str, cooldown_seconds: float, message: str):
        super().__init__(message)
        self.station_id = station_id
        self.cooldown_seconds = cooldown_seconds


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


def parse_retry_after_seconds(value: Optional[str]) -> Optional[float]:
    """Convert a Retry-After header value into seconds when possible."""
    if not value:
        return None

    try:
        return max(float(value), 0.0)
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    now = datetime.now(timezone.utc)
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max((retry_at - now).total_seconds(), 0.0)


def load_completed_station_ids(out_path: Path) -> Set[str]:
    """Read an existing Open-Meteo raw file and return completed station ids."""
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
                logger.warning("Skipping malformed Open-Meteo row while scanning %s", out_path)
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
    """Record station-level Open-Meteo failures for later replay."""
    payload = {
        "run_date": run_date,
        "source": "open_meteo",
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


def fetch_history_page(
    session: requests.Session,
    *,
    station: Dict[str, str],
    params: Dict[str, object],
    timeout: int = 60,
    max_attempts: int = 5,
    backoff_seconds: float = 1.0,
    rate_limit_backoff_seconds: float = 15.0,
) -> Dict[str, object]:
    """Fetch one Open-Meteo history response with retries for transient failures."""
    station_id = station.get("noaa_station_id") or "unknown"

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(OPEN_METEO_API_URL, params=params, timeout=timeout)
            logger.info(
                "HTTP %s %s station=%s lat=%s lon=%s attempt=%s/%s",
                response.status_code,
                OPEN_METEO_API_URL,
                station.get("noaa_station_id"),
                params["latitude"],
                params["longitude"],
                attempt,
                max_attempts,
            )

            if response.status_code in RETRYABLE_STATUS_CODES:
                retry_after_seconds = parse_retry_after_seconds(response.headers.get("Retry-After"))
                if response.status_code == 429:
                    sleep_seconds = max(
                        retry_after_seconds or 0.0,
                        rate_limit_backoff_seconds * attempt,
                    )
                else:
                    sleep_seconds = max(
                        retry_after_seconds or 0.0,
                        backoff_seconds * (2 ** (attempt - 1)),
                    )

                if attempt == max_attempts:
                    if response.status_code == 429:
                        raise OpenMeteoRateLimitError(
                            station_id,
                            cooldown_seconds=sleep_seconds,
                            message=(
                                f"Open-Meteo rate limit persisted for station={station_id} "
                                f"after {max_attempts} attempts"
                            ),
                        )
                    response.raise_for_status()

                logger.warning(
                    "Retrying Open-Meteo request after HTTP %s for station=%s lat=%s lon=%s in %.1fs",
                    response.status_code,
                    station_id,
                    params["latitude"],
                    params["longitude"],
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                continue

            response.raise_for_status()
            return response.json()

        except OpenMeteoRateLimitError:
            raise
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == max_attempts:
                raise

            sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Retrying Open-Meteo request after %s for station=%s lat=%s lon=%s in %.1fs",
                type(exc).__name__,
                station_id,
                params["latitude"],
                params["longitude"],
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"Failed to fetch Open-Meteo data after {max_attempts} attempts "
        f"for station={station_id} lat={params['latitude']} lon={params['longitude']}"
    )


def apply_rate_limit_cooldown(
    sleep_seconds: float,
    *,
    station_id: str,
) -> None:
    """Pause the next station fetches after sustained Open-Meteo rate limiting."""
    if sleep_seconds <= 0:
        return

    logger.warning(
        "Cooling down Open-Meteo for %.1fs after sustained rate limiting on station=%s",
        sleep_seconds,
        station_id,
    )
    time.sleep(sleep_seconds)


def main(argv: Optional[list] = None) -> int:
    """Fetch Open-Meteo daily history for each configured station."""
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
    parser.add_argument("--station-offset", type=int, default=0, help="Zero-based enabled-station offset for batch runs")
    parser.add_argument("--request-delay-seconds", type=float, default=30.0, help="Delay between successful station requests to reduce API throttling")
    parser.add_argument("--rate-limit-backoff-seconds", type=float, default=60.0, help="Minimum backoff to apply after Open-Meteo rate limiting")
    parser.add_argument("--post-rate-limit-cooldown-seconds", type=float, default=180.0, help="Cooldown to apply before the next station after repeated Open-Meteo 429 responses")
    parser.add_argument("--fail-on-station-error", action="store_true", help="Stop the run when any station exhausts retries")
    parser.add_argument("--no-resume", action="store_true", help="Start a fresh Open-Meteo raw file instead of appending to an existing one")
    args = parser.parse_args(argv)

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path(args.out_dir or (DATA_DIR / "raw" / "open_meteo" / f"run_date={run_date}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "open_meteo_daily.jsonl"
    failed_path = out_dir / "open_meteo_failed_stations.jsonl"

    stations = load_station_manifest(
        args.stations_csv,
        start_index=args.station_offset,
        limit=args.limit_stations,
    )
    daily_fields = [part.strip() for part in args.daily_fields.split(",") if part.strip()] or DEFAULT_DAILY_FIELDS

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent, "Accept": "application/json"})

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
                logger.info(
                    "Skipping Open-Meteo station=%s (%s/%s) because it is already present in %s",
                    station_id,
                    station_index,
                    len(stations),
                    out_path,
                )
                continue

            logger.info(
                "Fetching Open-Meteo history for station=%s (%s/%s)",
                station_id,
                station_index,
                len(stations),
            )
            params = build_params(
                station,
                start_date=args.start_date,
                end_date=args.end_date,
                model=args.model,
                timezone_name=args.timezone,
                daily_fields=daily_fields,
            )

            started_at = time.monotonic()
            try:
                payload = fetch_history_page(
                    session,
                    station=station,
                    params=params,
                    rate_limit_backoff_seconds=args.rate_limit_backoff_seconds,
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
                if isinstance(exc, OpenMeteoRateLimitError):
                    apply_rate_limit_cooldown(
                        max(args.post_rate_limit_cooldown_seconds, exc.cooldown_seconds),
                        station_id=station_id,
                    )
                if args.fail_on_station_error:
                    raise
                logger.error(
                    "Skipping Open-Meteo station=%s after %s: %s",
                    station_id,
                    type(exc).__name__,
                    exc,
                )
                continue

            daily = payload.get("daily", {})
            dates = daily.get("time", [])
            station_record_count = 0
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
                station_record_count += 1

            fh.flush()
            completed_station_ids.add(station_id)
            logger.info(
                "Saved %s Open-Meteo rows for station=%s in %.1fs",
                station_record_count,
                station_id,
                time.monotonic() - started_at,
            )

            if args.request_delay_seconds > 0:
                time.sleep(args.request_delay_seconds)

    logger.info(
        "Saved %d Open-Meteo rows to %s (skipped_existing=%s failed_stations=%s)",
        record_count,
        out_path,
        skipped_existing,
        failed_stations,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
