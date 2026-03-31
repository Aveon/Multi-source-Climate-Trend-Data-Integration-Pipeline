import csv
from pathlib import Path
from typing import Dict, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATIONS_CSV = PROJECT_ROOT / "data" / "reference" / "weather_stations_master.csv"


def _parse_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def load_station_manifest(
    csv_path: Optional[str] = None,
    *,
    only_enabled: bool = True,
    start_index: int = 0,
    limit: Optional[int] = None,
) -> List[Dict[str, str]]:
    """Load enabled station rows with optional offset and limit slicing."""
    manifest_path = Path(csv_path) if csv_path else DEFAULT_STATIONS_CSV
    if not manifest_path.exists():
        raise FileNotFoundError(f"Station manifest not found: {manifest_path}")

    if start_index < 0:
        raise ValueError("start_index must be zero or greater")

    stations: List[Dict[str, str]] = []
    matched_rows = 0
    with manifest_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if only_enabled and not _parse_bool(row.get("ingest_enabled"), default=True):
                continue

            if matched_rows < start_index:
                matched_rows += 1
                continue

            stations.append(row)
            if limit is not None and len(stations) >= limit:
                break
            matched_rows += 1

    return stations


def first_non_empty(row: Dict[str, str], *field_names: str) -> str:
    for field_name in field_names:
        value = row.get(field_name)
        if value is not None and value != "":
            return value
    return ""
