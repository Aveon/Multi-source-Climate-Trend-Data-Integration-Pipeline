import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

MONGO_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGODB_DB", "climate")
MONGO_COLLECTION = os.getenv("MONGODB_COLLECTION", "climate_daily")

DEFAULT_STATION = os.getenv("STATION", "KATL")  # Atlanta Hartsfield-Jackson Intl Airport
USER_AGENT = os.getenv("USER_AGENT", "BigDataProject/1.0 (joshua.young96@gmail.com)")
STATIONS_CSV = os.getenv(
    "STATIONS_CSV",
    str(PROJECT_ROOT / "data" / "reference" / "weather_stations_master.csv"),
)
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN") or os.getenv("NOAA_TOKEN", "")
NOAA_DATASET_ID = os.getenv("NOAA_DATASET_ID", "GHCND")
OPEN_METEO_MODEL = os.getenv("OPEN_METEO_MODEL", "era5")
OPEN_METEO_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "auto")
