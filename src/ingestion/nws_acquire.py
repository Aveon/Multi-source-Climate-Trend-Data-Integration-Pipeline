import os
import json
import argparse
import shutil
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
import logging
from typing import Tuple, Dict, Any, Optional, Set
import requests

THIS_DIR = Path(__file__).resolve().parent
SRC_ROOT = THIS_DIR.parent
PROJECT_ROOT = SRC_ROOT.parent
for path in (str(PROJECT_ROOT), str(SRC_ROOT), str(THIS_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

try:
    from src.ingestion.station_manifest import load_station_manifest, first_non_empty
except ModuleNotFoundError:
    try:
        from ingestion.station_manifest import load_station_manifest, first_non_empty
    except ModuleNotFoundError:
        from station_manifest import load_station_manifest, first_non_empty


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
DATA_DIR = PROJECT_ROOT / "data"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def build_nws_request(station_id: str, user_agent: str, limit: int = 100) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
    """Build the URL, headers, and query params for one NWS observations request."""
    url = f"https://api.weather.gov/stations/{station_id}/observations"
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/geo+json"
    }
    params = {"limit": limit}
    return url, headers, params


def fetch_json(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    timeout: int = 30,
    max_attempts: int = 5,
    backoff_seconds: float = 1.0,
) -> Dict[str, Any]:
    """Fetch one NWS observations payload with retries for transient failures."""
    station_id = url.rstrip("/").split("/")[-2] if "/observations" in url else "unknown"

    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, headers=headers, params=params, timeout=timeout)
            logger.debug(
                "HTTP %s %s station=%s attempt=%s/%s",
                resp.status_code,
                url,
                station_id,
                attempt,
                max_attempts,
            )

            if resp.status_code in RETRYABLE_STATUS_CODES:
                if attempt == max_attempts:
                    resp.raise_for_status()

                sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Retrying NWS request after HTTP %s for station=%s in %.1fs",
                    resp.status_code,
                    station_id,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                continue

            resp.raise_for_status()
            return resp.json()

        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == max_attempts:
                raise

            sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Retrying NWS request after %s for station=%s in %.1fs",
                type(exc).__name__,
                station_id,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to fetch NWS data after {max_attempts} attempts for station={station_id}")


def ensure_dir(path: str) -> None:
    """Create an output directory if it does not already exist."""
    os.makedirs(path, exist_ok=True)


def prune_stale_station_dirs(out_dir: Path, expected_station_ids: Set[str]) -> int:
    """Remove leftover station folders that are not part of the current batch run."""
    removed = 0
    for child in out_dir.iterdir():
        if not child.is_dir() or not child.name.startswith("station="):
            continue

        station_id = child.name.split("=", 1)[-1]
        if station_id in expected_station_ids:
            continue

        shutil.rmtree(child)
        removed += 1

    return removed


def _save_raw_json(out_path: str, data: Dict[str, Any]) -> None:
    """Persist one raw NWS response to disk for later processing."""
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.debug("Saved raw JSON to: %s", out_path)


def append_failed_station(
    failed_path: Path,
    *,
    run_date: str,
    station_id: str,
    station: Optional[Dict[str, str]],
    error: Exception,
) -> None:
    """Record station-level NWS failures for later replay."""
    payload = {
        "run_date": run_date,
        "source": "nws",
        "nws_station_id": station_id,
        "noaa_station_id": station.get("noaa_station_id") if station else None,
        "station_name": station.get("station_name") if station else None,
        "state": station.get("state") if station else None,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "logged_at": datetime.now(timezone.utc).isoformat(),
    }
    with failed_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def print_sample(data: Dict[str, Any], n: int = 10, convert_wind_to_mps: bool = True) -> None:
    """Log a small human-readable sample of the fetched NWS observations."""
    features = data.get("features", [])
    logger.info("Features returned: %d", len(features))
    logger.info("---- Sample ----")
    for i, feat in enumerate(features[:n]):
        props = feat.get("properties", {})
        ts = props.get("timestamp")

        # temperature
        t = props.get("temperature")
        tval = None
        if isinstance(t, dict):
            tval = t.get("value")

        # wind
        ws = props.get("windSpeed")
        wval = None
        wunit = None
        if isinstance(ws, dict):
            wval = ws.get("value")
            wunit = ws.get("unitCode")

        # format values
        temp_str = "N/A"
        if tval is not None:
            try:
                temp_str = f"{float(tval):.1f}°C"
            except Exception:
                temp_str = str(tval)

        wind_str = "N/A"
        if wval is not None:
            try:
                fv = float(wval)
                if convert_wind_to_mps and isinstance(wunit, str) and "km_h" in wunit:
                    fv = fv / 3.6
                    wind_str = f"{fv:.2f} m/s"
                else:
                    # best-effort unit label
                    unit_label = ""
                    if isinstance(wunit, str) and "km_h" in wunit:
                        unit_label = "km/h"
                    wind_str = f"{fv:.3f} {unit_label}".strip()
            except Exception:
                wind_str = str(wval)

        logger.info("%d. Time: %s, Temp: %s, Wind: %s", i + 1, ts, temp_str, wind_str)
    logger.info("---- End Sample ----")


def main(argv: Optional[list] = None) -> int:
    """Fetch recent NWS station observations and save the raw payloads."""
    parser = argparse.ArgumentParser(description="Fetch NWS station observations and save raw JSON")
    parser.add_argument("--station", type=str, default="KATL", help="NWS station ID (e.g. KATL for Atlanta)")
    parser.add_argument("--stations-csv", default=None, help="CSV manifest with nws_station_id values to ingest in batch")
    parser.add_argument("--user-agent", type=str, default="BigDataProject (mailto:joshua.young96@gmail.com)", help="User-Agent string with contact info")
    parser.add_argument("--out-dir", default=None, help="Directory to save raw JSON (defaults to data/raw/<date>)")
    parser.add_argument("--date", default=None, help="Run date in YYYY-MM-DD (defaults to today UTC)")
    parser.add_argument("--limit", type=int, default=100, help="Number of observations to request")
    parser.add_argument("--limit-stations", type=int, default=None, help="Maximum number of stations to ingest from the manifest")
    parser.add_argument("--fail-on-station-error", action="store_true", help="Stop the run when any station fails")
    args = parser.parse_args(argv)

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = args.out_dir or str(DATA_DIR / "raw" / "nws" / f"run_date={run_date}")
    ensure_dir(out_dir)
    session = requests.Session()
    failed_path = Path(out_dir) / "nws_failed_stations.jsonl"
    if failed_path.exists():
        failed_path.unlink()

    if args.stations_csv:
        stations = load_station_manifest(args.stations_csv, limit=args.limit_stations)
        target_station_ids = {
            first_non_empty(row, "nws_station_id", "station_id")
            for row in stations
            if first_non_empty(row, "nws_station_id", "station_id")
        }
        removed_stale_dirs = prune_stale_station_dirs(Path(out_dir), target_station_ids)
        if removed_stale_dirs:
            logger.debug("Removed %d stale NWS station folders from %s", removed_stale_dirs, out_dir)
        fetched = 0
        failed_stations = 0
        for row in stations:
            station_id = first_non_empty(row, "nws_station_id", "station_id")
            if not station_id:
                continue

            station_out_dir = Path(out_dir) / f"station={station_id}"
            ensure_dir(str(station_out_dir))
            try:
                url, headers, params = build_nws_request(station_id, args.user_agent, limit=args.limit)
                data = fetch_json(session, url, headers, params)
                _save_raw_json(str(station_out_dir / "nws_raw.json"), data)
                fetched += 1
            except Exception as exc:
                failed_stations += 1
                append_failed_station(
                    failed_path,
                    run_date=run_date,
                    station_id=station_id,
                    station=row,
                    error=exc,
                )
                if args.fail_on_station_error:
                    raise
                logger.error(
                    "Skipping NWS station=%s after %s: %s",
                    station_id,
                    type(exc).__name__,
                    exc,
                )

        logger.debug(
            "Saved NWS raw files for %d stations to %s (failed_stations=%s)",
            fetched,
            out_dir,
            failed_stations,
        )
        if failed_stations and fetched == 0:
            return 1
        return 0

    raw_path = os.path.join(out_dir, "nws_raw.json")
    try:
        url, headers, params = build_nws_request(args.station, args.user_agent, limit=args.limit)
        data = fetch_json(session, url, headers, params)
        _save_raw_json(raw_path, data)
        print_sample(data, n=10)
        return 0
    except Exception as exc:
        append_failed_station(
            failed_path,
            run_date=run_date,
            station_id=args.station,
            station=None,
            error=exc,
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
