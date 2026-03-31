import calendar
import importlib
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import argparse
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("main")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
ENV_PATH = PROJECT_ROOT / ".env"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(dotenv_path=ENV_PATH, override=True)


def first(*vals):
    for value in vals:
        if value is not None and value != "":
            return value
    return None


def optional_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    return int(value)


def load_config() -> Dict[str, Any]:
    return {
        "MONGODB_URI": os.getenv("MONGODB_URI", ""),
        "MONGODB_DB": first(os.getenv("MONGODB_DB"), "climate"),
        "MONGODB_COLLECTION": first(os.getenv("MONGODB_COLLECTION"), "climate_daily"),
        "STATION": first(os.getenv("STATION"), "KATL"),
        "STATIONS_CSV": first(
            os.getenv("STATIONS_CSV"),
            str(DATA_DIR / "reference" / "weather_stations_master.csv"),
        ),
        "USER_AGENT": first(
            os.getenv("USER_AGENT"),
            "BigDataProject (mailto:joshua.young96@gmail.com)",
        ),
        "NOAA_API_TOKEN": first(os.getenv("NOAA_API_TOKEN"), os.getenv("NOAA_TOKEN"), ""),
        "NOAA_DATASET_ID": first(os.getenv("NOAA_DATASET_ID"), "GHCND"),
        "NOAA_INGEST_MODE": first(os.getenv("NOAA_INGEST_MODE"), "bulk_station"),
        "NOAA_BULK_BASE_URL": first(
            os.getenv("NOAA_BULK_BASE_URL"),
            "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/{station_code}.csv.gz",
        ),
        "OPEN_METEO_MODEL": first(os.getenv("OPEN_METEO_MODEL"), "era5"),
        "OPEN_METEO_TIMEZONE": first(os.getenv("OPEN_METEO_TIMEZONE"), "auto"),
        "OPEN_METEO_REQUEST_DELAY_SECONDS": first(os.getenv("OPEN_METEO_REQUEST_DELAY_SECONDS"), "30.0"),
        "OPEN_METEO_RATE_LIMIT_BACKOFF_SECONDS": first(os.getenv("OPEN_METEO_RATE_LIMIT_BACKOFF_SECONDS"), "60.0"),
        "OPEN_METEO_POST_RATE_LIMIT_COOLDOWN_SECONDS": first(
            os.getenv("OPEN_METEO_POST_RATE_LIMIT_COOLDOWN_SECONDS"),
            "180.0",
        ),
        "OPEN_METEO_BATCH_SIZE": optional_int(os.getenv("OPEN_METEO_BATCH_SIZE")),
        "OPEN_METEO_BATCH_INDEX": optional_int(first(os.getenv("OPEN_METEO_BATCH_INDEX"), "1")),
    }


def import_module_flexible(module_a: str, module_b: str):
    try:
        return importlib.import_module(module_a)
    except ModuleNotFoundError:
        return importlib.import_module(module_b)


def shift_years(value: date, years: int) -> date:
    target_year = value.year + years
    target_day = min(value.day, calendar.monthrange(target_year, value.month)[1])
    return date(target_year, value.month, target_day)


def default_historical_window() -> tuple[str, str]:
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = shift_years(end_date, -10) + timedelta(days=1)
    return start_date.isoformat(), end_date.isoformat()


def parse_sources(raw_sources: str) -> List[str]:
    valid_sources = {"nws", "noaa", "open_meteo"}
    sources = [item.strip() for item in raw_sources.split(",") if item.strip()]
    invalid = [item for item in sources if item not in valid_sources]
    if invalid:
        raise ValueError(f"Unsupported sources requested: {invalid}")
    return sources


def run_module(module_name: str, fallback_name: str, args: List[str]) -> int:
    module = import_module_flexible(module_name, fallback_name)
    rc = module.main(args)
    if rc:
        raise RuntimeError(f"{module_name} failed with exit code {rc}")
    return rc


def run_ingestion(
    source: str,
    cfg: Dict[str, Any],
    *,
    run_date: str,
    start_date: str,
    end_date: str,
    stations_csv: str,
    limit_stations: Optional[int],
    open_meteo_limit_stations: Optional[int],
    open_meteo_station_offset: int,
    station_override: Optional[str],
) -> Optional[str]:
    limit_station_args = ["--limit-stations", str(limit_stations)] if limit_stations else []

    if source == "nws":
        out_dir = str(DATA_DIR / "raw" / "nws" / f"run_date={run_date}")
        args = [
            "--user-agent", cfg["USER_AGENT"],
            "--out-dir", out_dir,
            "--date", run_date,
            *limit_station_args,
        ]
        if station_override:
            args.extend(["--station", station_override])
        else:
            args.extend(["--stations-csv", stations_csv])
        run_module("src.ingestion.nws_acquire", "ingestion.nws_acquire", args)
        if station_override:
            return os.path.join(out_dir, "nws_raw.json")
        return None

    if source == "noaa":
        if cfg["NOAA_INGEST_MODE"] == "api" and not cfg["NOAA_API_TOKEN"]:
            raise ValueError("NOAA_API_TOKEN is required for NOAA ingestion")
        out_dir = str(DATA_DIR / "raw" / "noaa" / f"run_date={run_date}")
        args = [
            "--stations-csv", stations_csv,
            "--dataset-id", cfg["NOAA_DATASET_ID"],
            "--ingest-mode", cfg["NOAA_INGEST_MODE"],
            "--bulk-base-url", cfg["NOAA_BULK_BASE_URL"],
            "--user-agent", cfg["USER_AGENT"],
            "--start-date", start_date,
            "--end-date", end_date,
            "--out-dir", out_dir,
            "--date", run_date,
            *limit_station_args,
        ]
        if cfg["NOAA_API_TOKEN"]:
            args.extend(["--token", cfg["NOAA_API_TOKEN"]])
        run_module("src.ingestion.noaa_acquire", "ingestion.noaa_acquire", args)
        return os.path.join(out_dir, "noaa_daily.jsonl")

    if source == "open_meteo":
        out_dir = str(DATA_DIR / "raw" / "open_meteo" / f"run_date={run_date}")
        open_meteo_limit = open_meteo_limit_stations if open_meteo_limit_stations is not None else limit_stations
        open_meteo_limit_args = ["--limit-stations", str(open_meteo_limit)] if open_meteo_limit else []
        args = [
            "--stations-csv", stations_csv,
            "--start-date", start_date,
            "--end-date", end_date,
            "--model", cfg["OPEN_METEO_MODEL"],
            "--timezone", cfg["OPEN_METEO_TIMEZONE"],
            "--request-delay-seconds", str(cfg["OPEN_METEO_REQUEST_DELAY_SECONDS"]),
            "--rate-limit-backoff-seconds", str(cfg["OPEN_METEO_RATE_LIMIT_BACKOFF_SECONDS"]),
            "--post-rate-limit-cooldown-seconds", str(cfg["OPEN_METEO_POST_RATE_LIMIT_COOLDOWN_SECONDS"]),
            "--user-agent", cfg["USER_AGENT"],
            "--out-dir", out_dir,
            "--date", run_date,
            "--station-offset", str(open_meteo_station_offset),
            *open_meteo_limit_args,
        ]
        run_module("src.ingestion.open_meteo_acquire", "ingestion.open_meteo_acquire", args)
        return os.path.join(out_dir, "open_meteo_daily.jsonl")

    raise ValueError(f"Unsupported ingestion source: {source}")


def run_processing(
    sources: List[str],
    *,
    run_date: str,
    stations_csv: str,
) -> None:
    if "nws" in sources:
        run_module(
            "src.processing.nws_daily",
            "processing.nws_daily",
            ["--stations-csv", stations_csv, "--run-date", run_date],
        )
    if "noaa" in sources:
        run_module("src.processing.noaa_daily", "processing.noaa_daily", ["--run-date", run_date])
    if "open_meteo" in sources:
        run_module("src.processing.open_meteo_daily", "processing.open_meteo_daily", ["--run-date", run_date])

    run_module(
        "src.processing.climate_unified",
        "processing.climate_unified",
        ["--sources", ",".join(sources)],
    )


def run_ingestion_batch(
    sources: List[str],
    cfg: Dict[str, Any],
    *,
    run_date: str,
    start_date: str,
    end_date: str,
    stations_csv: str,
    limit_stations: Optional[int],
    open_meteo_limit_stations: Optional[int],
    open_meteo_station_offset: int,
    station_override: Optional[str],
) -> Dict[str, Optional[str]]:
    """Run source ingestion in parallel so network-bound APIs can overlap."""
    raw_outputs: Dict[str, Optional[str]] = {}
    max_workers = min(len(sources), 3)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_source = {}
        for source in sources:
            logger.info("Running ingestion for source=%s", source)
            future = executor.submit(
                run_ingestion,
                source,
                cfg,
                run_date=run_date,
                start_date=start_date,
                end_date=end_date,
                stations_csv=stations_csv,
                limit_stations=limit_stations,
                open_meteo_limit_stations=open_meteo_limit_stations,
                open_meteo_station_offset=open_meteo_station_offset,
                station_override=station_override if source == "nws" else None,
            )
            future_to_source[future] = source

        for future in as_completed(future_to_source):
            source = future_to_source[future]
            raw_outputs[source] = future.result()

    return raw_outputs


def store_curated_to_mongo(cfg: Dict[str, Any]) -> Dict[str, int]:
    if not cfg.get("MONGODB_URI"):
        raise ValueError("MONGODB_URI is missing. Put it in .env or your shell environment.")

    storage = import_module_flexible("src.storage.mongo_handler", "storage.mongo_handler")
    get_db = getattr(storage, "get_db")
    write_curated_parquet_to_mongo = getattr(storage, "write_curated_parquet_to_mongo")
    mongo_db = get_db(mongo_uri=cfg["MONGODB_URI"], db_name=cfg["MONGODB_DB"])
    parquet_path = str(DATA_DIR / "curated" / "climate_daily" / "parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Expected curated parquet at {parquet_path}")
    return write_curated_parquet_to_mongo(
        parquet_path,
        mongo_db=mongo_db,
        collection_name=cfg["MONGODB_COLLECTION"],
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = load_config()
    default_start, default_end = default_historical_window()

    parser = argparse.ArgumentParser(description="Project pipeline orchestrator")
    parser.add_argument("--date", default=None, help="Run date in YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--start-date", default=default_start, help="Historical window start date")
    parser.add_argument("--end-date", default=default_end, help="Historical window end date")
    parser.add_argument("--sources", default="nws,noaa,open_meteo", help="Comma-separated sources to ingest/process")
    parser.add_argument("--station", default=None, help="Single NWS station override")
    parser.add_argument("--stations-csv", default=cfg["STATIONS_CSV"], help="Station manifest CSV")
    parser.add_argument("--limit-stations", type=int, default=None, help="Limit station count for testing")
    parser.add_argument("--open-meteo-batch-size", type=int, default=cfg["OPEN_METEO_BATCH_SIZE"], help="Optional Open-Meteo station batch size")
    parser.add_argument("--open-meteo-batch-index", type=int, default=cfg["OPEN_METEO_BATCH_INDEX"], help="1-based Open-Meteo station batch number")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip source ingestion")
    parser.add_argument("--skip-process", action="store_true", help="Skip Spark processing")
    parser.add_argument("--skip-mongo", action="store_true", help="Skip MongoDB storage step")
    args = parser.parse_args(argv)

    try:
        sources = parse_sources(args.sources)
    except ValueError:
        logger.exception("Invalid source selection")
        return 1

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_outputs: Dict[str, Optional[str]] = {}
    open_meteo_limit_stations = args.open_meteo_batch_size
    open_meteo_station_offset = 0

    if open_meteo_limit_stations is not None:
        if open_meteo_limit_stations <= 0:
            logger.error("--open-meteo-batch-size must be greater than zero")
            return 1
        if args.open_meteo_batch_index is None or args.open_meteo_batch_index <= 0:
            logger.error("--open-meteo-batch-index must be greater than zero")
            return 1
        open_meteo_station_offset = (args.open_meteo_batch_index - 1) * open_meteo_limit_stations
        logger.info(
            "Open-Meteo batching enabled: batch=%s size=%s offset=%s",
            args.open_meteo_batch_index,
            open_meteo_limit_stations,
            open_meteo_station_offset,
        )

    try:
        if not args.skip_ingest:
            raw_outputs = run_ingestion_batch(
                sources,
                cfg,
                run_date=run_date,
                start_date=args.start_date,
                end_date=args.end_date,
                stations_csv=args.stations_csv,
                limit_stations=args.limit_stations,
                open_meteo_limit_stations=open_meteo_limit_stations,
                open_meteo_station_offset=open_meteo_station_offset,
                station_override=args.station,
            )
        else:
            logger.info("Skipping ingestion for sources=%s", sources)
    except Exception:
        logger.exception("Error during ingestion")
        return 2

    try:
        if not args.skip_process:
            logger.info(
                "Running processing for sources=%s over %s..%s",
                sources,
                args.start_date,
                args.end_date,
            )
            run_processing(sources, run_date=run_date, stations_csv=args.stations_csv)
        else:
            logger.info("Skipping Spark processing")
    except Exception:
        logger.exception("Error during processing")
        return 3

    try:
        if args.skip_mongo:
            logger.info("Skipping MongoDB storage step")
        else:
            summary = store_curated_to_mongo(cfg)
            logger.info("MongoDB storage summary: %s", summary)
    except Exception:
        logger.exception("Error during MongoDB storage")
        return 4

    logger.info("Pipeline completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
