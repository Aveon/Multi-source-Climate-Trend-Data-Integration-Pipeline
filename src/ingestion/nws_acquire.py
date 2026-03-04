import os
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
import logging
from typing import Tuple, Dict, Any, Optional
import requests



logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"


def build_nws_request(station_id: str, user_agent: str, limit: int = 100) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
    url = f"https://api.weather.gov/stations/{station_id}/observations"
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/geo+json"
    }
    params = {"limit": limit}
    return url, headers, params


def fetch_json(url: str, headers: Dict[str, str], params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    logger.info("HTTP %s %s", resp.status_code, url)
    resp.raise_for_status()
    return resp.json()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _save_raw_json(out_path: str, data: Dict[str, Any]) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info("Saved raw JSON to: %s", out_path)


def print_sample(data: Dict[str, Any], n: int = 10, convert_wind_to_mps: bool = True) -> None:
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
    parser = argparse.ArgumentParser(description="Fetch NWS station observations and save raw JSON")
    parser.add_argument("--station", type=str, default="KATL", help="NWS station ID (e.g. KATL for Atlanta)")
    parser.add_argument("--user-agent", type=str, default="BigDataProject (mailto:joshua.young96@gmail.com)", help="User-Agent string with contact info")
    parser.add_argument("--out-dir", default=None, help="Directory to save raw JSON (defaults to data/raw/<date>)")
    parser.add_argument("--date", default=None, help="Run date in YYYY-MM-DD (defaults to today UTC)")
    parser.add_argument("--limit", type=int, default=100, help="Number of observations to request")
    args = parser.parse_args(argv)

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = args.out_dir or str(DATA_DIR / "raw" / run_date)
    ensure_dir(out_dir)

    raw_path = os.path.join(out_dir, "nws_raw.json")

    url, headers, params = build_nws_request(args.station, args.user_agent, limit=args.limit)
    data = fetch_json(url, headers, params)

    _save_raw_json(raw_path, data)
    print_sample(data, n=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
