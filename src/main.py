import calendar
import importlib
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
import time
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
    """Return the first non-empty value from a list of fallbacks."""
    for value in vals:
        if value is not None and value != "":
            return value
    return None


def optional_int(value: Optional[str]) -> Optional[int]:
    """Convert a string config value to int while preserving empty values as None."""
    if value is None or value == "":
        return None
    return int(value)


def load_config() -> Dict[str, Any]:
    """Load pipeline settings from environment variables with project defaults."""
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
        "METEOSTAT_BATCH_SIZE": optional_int(first(os.getenv("METEOSTAT_BATCH_SIZE"), os.getenv("OPEN_METEO_BATCH_SIZE"))),
        "METEOSTAT_BATCH_INDEX": optional_int(first(os.getenv("METEOSTAT_BATCH_INDEX"), os.getenv("OPEN_METEO_BATCH_INDEX"), "1")),
    }


def import_module_flexible(module_a: str, module_b: str):
    """Import a module using a fallback name to support script and package execution."""
    try:
        return importlib.import_module(module_a)
    except ModuleNotFoundError:
        return importlib.import_module(module_b)


def shift_years(value: date, years: int) -> date:
    """Move a date by whole years while keeping the day valid for the target month."""
    target_year = value.year + years
    target_day = min(value.day, calendar.monthrange(target_year, value.month)[1])
    return date(target_year, value.month, target_day)


def default_historical_window() -> tuple[str, str]:
    """Return the default trailing 10-year historical window for backfill runs."""
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = shift_years(end_date, -10) + timedelta(days=1)
    return start_date.isoformat(), end_date.isoformat()


def display_source_name(source: str) -> str:
    """Return a user-friendly source name for live run logging."""
    return {
        "nws": "NWS",
        "noaa": "NOAA",
        "meteostat": "Meteostat",
    }.get(source, source)


def format_count(value: int) -> str:
    """Format integer counts with separators for easier scanning in logs."""
    return f"{value:,}"


def format_duration(seconds: float) -> str:
    """Format elapsed seconds into a compact human-readable duration."""
    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def format_run_timestamp(value: datetime) -> str:
    """Format the run start timestamp for the live console summary."""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def count_jsonl_records(path: str) -> int:
    """Count line-delimited JSON records in a raw source output file."""
    with open(path, "r", encoding="utf-8") as fh:
        return sum(1 for _ in fh)


def count_nws_raw_records(run_date: str) -> int:
    """Count raw NWS observation features saved for a given run date."""
    raw_dir = DATA_DIR / "raw" / "nws" / f"run_date={run_date}"
    if not raw_dir.exists():
        return 0

    total = 0
    for raw_path in raw_dir.rglob("nws_raw.json"):
        with raw_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        total += len(payload.get("features", [])) if isinstance(payload, dict) else 0
    return total


def count_ingested_records(source: str, raw_output: Optional[str], *, run_date: str) -> int:
    """Count the retrieved raw records for one ingestion source."""
    if source in {"noaa", "meteostat"}:
        candidate_path = raw_output
        if not candidate_path:
            candidate_path = str(DATA_DIR / "raw" / source / f"run_date={run_date}" / f"{source}_daily.jsonl")
        if not os.path.exists(candidate_path):
            return 0
        return count_jsonl_records(candidate_path)
    if source == "nws":
        return count_nws_raw_records(run_date)
    return 0


def calculate_retrieved_record_count(source: str, *, before_count: int, after_count: int) -> int:
    """Report newly retrieved records while respecting each source's write pattern."""
    if source == "meteostat":
        return max(0, after_count - before_count)
    return after_count


def count_curated_records() -> int:
    """Count the transformed curated climate rows ready for storage."""
    parquet_path = DATA_DIR / "curated" / "climate_daily" / "parquet"
    if not parquet_path.exists():
        return 0

    spark_common = import_module_flexible("src.processing.spark_common", "processing.spark_common")
    get_spark = getattr(spark_common, "get_spark")
    spark = get_spark("count_curated_records")
    try:
        return spark.read.parquet(str(parquet_path)).count()
    finally:
        spark.stop()


def load_transformation_metrics(sources: List[str], run_date: str) -> Dict[str, int]:
    """Load per-source transformation metrics and return summed counts."""
    processing_metrics = import_module_flexible("src.processing.metrics", "processing.metrics")
    load_metrics = getattr(processing_metrics, "load_metrics")

    summary = {
        "raw_input_rows": 0,
        "invalid_rows_removed": 0,
        "valid_input_rows": 0,
        "output_rows": 0,
        "rows_consolidated_by_aggregation": 0,
        "sources_with_metrics": 0,
    }

    for source in sources:
        metrics = load_metrics(source, run_date)
        if not metrics:
            continue
        summary["raw_input_rows"] += int(metrics.get("raw_input_rows", 0))
        summary["invalid_rows_removed"] += int(metrics.get("invalid_rows_removed", 0))
        summary["valid_input_rows"] += int(metrics.get("valid_input_rows", 0))
        summary["output_rows"] += int(metrics.get("output_rows", 0))
        summary["rows_consolidated_by_aggregation"] += int(metrics.get("rows_consolidated_by_aggregation", 0))
        summary["sources_with_metrics"] += 1

    return summary


def parse_sources(raw_sources: str) -> List[str]:
    """Validate and normalize the comma-separated source list from the CLI."""
    valid_sources = {"nws", "noaa", "meteostat"}
    sources = [item.strip() for item in raw_sources.split(",") if item.strip()]
    invalid = [item for item in sources if item not in valid_sources]
    if invalid:
        raise ValueError(f"Unsupported sources requested: {invalid}")
    return list(dict.fromkeys(sources))


def run_module(module_name: str, fallback_name: str, args: List[str]) -> int:
    """Execute another pipeline module through its main function."""
    module = import_module_flexible(module_name, fallback_name)
    rc = module.main(args)
    if rc:
        raise RuntimeError(f"{module_name} failed with exit code {rc}")
    return rc


def run_module_with_retry(
    module_name: str,
    fallback_name: str,
    args: List[str],
    *,
    step_label: str,
    max_attempts: int = 3,
    backoff_seconds: float = 5.0,
) -> int:
    """Retry a processing module when transient Spark or filesystem failures occur."""
    for attempt in range(1, max_attempts + 1):
        try:
            return run_module(module_name, fallback_name, args)
        except Exception as exc:
            if attempt == max_attempts:
                raise

            sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Retrying processing step %s after %s: %s (attempt=%s/%s, next_retry_in=%.1fs)",
                step_label,
                type(exc).__name__,
                exc,
                attempt + 1,
                max_attempts,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)


def run_ingestion(
    source: str,
    cfg: Dict[str, Any],
    *,
    run_date: str,
    start_date: str,
    end_date: str,
    stations_csv: str,
    limit_stations: Optional[int],
    meteostat_limit_stations: Optional[int],
    meteostat_station_offset: int,
    station_override: Optional[str],
) -> Optional[str]:
    """Run one source ingestion job and return its main raw output path when relevant."""
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

    if source == "meteostat":
        out_dir = str(DATA_DIR / "raw" / "meteostat" / f"run_date={run_date}")
        meteostat_limit = meteostat_limit_stations if meteostat_limit_stations is not None else limit_stations
        meteostat_limit_args = ["--limit-stations", str(meteostat_limit)] if meteostat_limit else []
        args = [
            "--stations-csv", stations_csv,
            "--start-date", start_date,
            "--end-date", end_date,
            "--out-dir", out_dir,
            "--date", run_date,
            "--station-offset", str(meteostat_station_offset),
            *meteostat_limit_args,
        ]
        run_module("src.ingestion.meteostat_acquire", "ingestion.meteostat_acquire", args)
        return os.path.join(out_dir, "meteostat_daily.jsonl")

    raise ValueError(f"Unsupported ingestion source: {source}")


def run_processing(
    sources: List[str],
    *,
    run_date: str,
    stations_csv: str,
    max_attempts: int = 3,
    backoff_seconds: float = 5.0,
) -> None:
    """Run source-specific Spark jobs and then build the unified curated outputs."""
    if "nws" in sources:
        run_module_with_retry(
            "src.processing.nws_daily",
            "processing.nws_daily",
            ["--stations-csv", stations_csv, "--run-date", run_date],
            step_label="NWS transformation",
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
    if "noaa" in sources:
        run_module_with_retry(
            "src.processing.noaa_daily",
            "processing.noaa_daily",
            ["--run-date", run_date],
            step_label="NOAA transformation",
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
    if "meteostat" in sources:
        run_module_with_retry(
            "src.processing.meteostat_daily",
            "processing.meteostat_daily",
            ["--run-date", run_date],
            step_label="Meteostat transformation",
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )

    run_module_with_retry(
        "src.processing.climate_unified",
        "processing.climate_unified",
        ["--sources", ",".join(sources)],
        step_label="Unified climate transformation",
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
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
    meteostat_limit_stations: Optional[int],
    meteostat_station_offset: int,
    station_override: Optional[str],
) -> tuple[Dict[str, Optional[str]], Dict[str, int]]:
    """Run source ingestion in parallel so network-bound APIs can overlap."""
    raw_outputs: Dict[str, Optional[str]] = {}
    raw_counts: Dict[str, int] = {}
    total_raw_records_available = 0
    total_available_counts: Dict[str, int] = {}
    before_counts: Dict[str, int] = {}
    max_workers = min(len(sources), 3)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_source = {}
        for source in sources:
            logger.info("Fetching data from %s...", display_source_name(source))
            before_counts[source] = count_ingested_records(source, None, run_date=run_date)
            future = executor.submit(
                run_ingestion,
                source,
                cfg,
                run_date=run_date,
                start_date=start_date,
                end_date=end_date,
                stations_csv=stations_csv,
                limit_stations=limit_stations,
                meteostat_limit_stations=meteostat_limit_stations,
                meteostat_station_offset=meteostat_station_offset,
                station_override=station_override if source == "nws" else None,
            )
            future_to_source[future] = source

        for future in as_completed(future_to_source):
            source = future_to_source[future]
            raw_output = future.result()
            raw_outputs[source] = raw_output
            raw_count = calculate_retrieved_record_count(
                source,
                before_count=before_counts.get(source, 0),
                after_count=count_ingested_records(source, raw_output, run_date=run_date),
            )
            after_count = count_ingested_records(source, raw_output, run_date=run_date)
            raw_counts[source] = raw_count
            total_available_counts[source] = after_count

            if raw_count == 0 and after_count > 0:
                logger.info(
                    "Using %s existing records from %s",
                    format_count(after_count),
                    display_source_name(source),
                )
            elif before_counts.get(source, 0) > 0 and raw_count > 0:
                logger.info(
                    "Retrieved %s new records from %s (%s total available)",
                    format_count(raw_count),
                    display_source_name(source),
                    format_count(after_count),
                )
            else:
                logger.info(
                    "Retrieved %s records from %s",
                    format_count(raw_count),
                    display_source_name(source),
                )

    raw_counts["_total_available"] = sum(total_available_counts.values())
    return raw_outputs, raw_counts


def store_curated_to_mongo(cfg: Dict[str, Any]) -> Dict[str, int]:
    """Load curated daily parquet data and upsert it into MongoDB."""
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
    """Coordinate ingestion, processing, and MongoDB storage for the full pipeline."""
    started_at = datetime.now()
    run_started_at = perf_counter()
    cfg = load_config()
    default_start, default_end = default_historical_window()

    parser = argparse.ArgumentParser(description="Project pipeline orchestrator")
    parser.add_argument("--date", default=None, help="Run date in YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--start-date", default=default_start, help="Historical window start date")
    parser.add_argument("--end-date", default=default_end, help="Historical window end date")
    parser.add_argument("--sources", default="nws,noaa,meteostat", help="Comma-separated sources to ingest/process")
    parser.add_argument("--station", default=None, help="Single NWS station override")
    parser.add_argument("--stations-csv", default=cfg["STATIONS_CSV"], help="Station manifest CSV")
    parser.add_argument("--limit-stations", type=int, default=None, help="Limit station count for testing")
    parser.add_argument("--meteostat-batch-size", dest="meteostat_batch_size", type=int, default=cfg["METEOSTAT_BATCH_SIZE"], help="Optional Meteostat station batch size")
    parser.add_argument("--meteostat-batch-index", dest="meteostat_batch_index", type=int, default=cfg["METEOSTAT_BATCH_INDEX"], help="1-based Meteostat station batch number")
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
    logger.info(
        "Starting pipeline run: %s (run_date=%s, window=%s to %s)",
        format_run_timestamp(started_at),
        run_date,
        args.start_date,
        args.end_date,
    )

    raw_outputs: Dict[str, Optional[str]] = {}
    raw_counts: Dict[str, int] = {}
    meteostat_limit_stations = args.meteostat_batch_size
    meteostat_station_offset = 0

    if meteostat_limit_stations is not None:
        if meteostat_limit_stations <= 0:
            logger.error("--meteostat-batch-size must be greater than zero")
            return 1
        if args.meteostat_batch_index is None or args.meteostat_batch_index <= 0:
            logger.error("--meteostat-batch-index must be greater than zero")
            return 1
        meteostat_station_offset = (args.meteostat_batch_index - 1) * meteostat_limit_stations
        logger.info(
            "Meteostat batching enabled: batch=%s size=%s offset=%s",
            args.meteostat_batch_index,
            meteostat_limit_stations,
            meteostat_station_offset,
        )

    try:
        if not args.skip_ingest:
            raw_outputs, raw_counts = run_ingestion_batch(
                sources,
                cfg,
                run_date=run_date,
                start_date=args.start_date,
                end_date=args.end_date,
                stations_csv=args.stations_csv,
                limit_stations=args.limit_stations,
                meteostat_limit_stations=meteostat_limit_stations,
                meteostat_station_offset=meteostat_station_offset,
                station_override=args.station,
            )
            total_available = raw_counts.pop("_total_available", 0)
            total_raw_records_available = total_available
            total_retrieved = sum(raw_counts.values())
            if total_retrieved != total_available:
                logger.info(
                    "Finished data retrieval. %s raw records available for transformation (%s newly retrieved)",
                    format_count(total_available),
                    format_count(total_retrieved),
                )
            else:
                logger.info(
                    "Finished data retrieval. Total raw records: %s",
                    format_count(total_retrieved),
                )
        else:
            logger.info("Skipping ingestion for sources=%s", sources)
    except Exception:
        logger.exception("Error during ingestion")
        return 2

    try:
        if not args.skip_process:
            if total_raw_records_available:
                logger.info(
                    "Applying transformations to %s raw records...",
                    format_count(total_raw_records_available),
                )
            else:
                logger.info("Applying transformations...")
            run_processing(sources, run_date=run_date, stations_csv=args.stations_csv)
            transformed_records = count_curated_records()
            metrics_summary = load_transformation_metrics(sources, run_date)
            if total_raw_records_available and metrics_summary.get("sources_with_metrics") == len(sources):
                logger.info(
                    "Transformation complete. Produced %s curated daily records from %s raw records (%s invalid removed, %s consolidated during aggregation)",
                    format_count(transformed_records),
                    format_count(total_raw_records_available),
                    format_count(metrics_summary.get("invalid_rows_removed", 0)),
                    format_count(metrics_summary.get("rows_consolidated_by_aggregation", 0)),
                )
            else:
                logger.info(
                    "Transformation complete. Produced %s curated daily records",
                    format_count(transformed_records),
                )
        else:
            logger.info("Skipping Spark processing")
    except Exception:
        logger.exception("Error during processing")
        return 3

    try:
        if args.skip_mongo:
            logger.info("Skipping MongoDB storage step")
        else:
            transformed_records = count_curated_records()
            logger.info("Writing %s records to database...", format_count(transformed_records))
            summary = store_curated_to_mongo(cfg)
            logger.info(
                "Database write complete. Processed=%s, upserted=%s, modified=%s, matched=%s",
                format_count(summary.get("to_process", 0)),
                format_count(summary.get("upserted", 0)),
                format_count(summary.get("modified", 0)),
                format_count(summary.get("matched", 0)),
            )
    except Exception:
        logger.exception("Error during MongoDB storage")
        return 4

    logger.info("Pipeline complete. Duration: %s", format_duration(perf_counter() - run_started_at))
    return 0


if __name__ == "__main__":
    sys.exit(main())
