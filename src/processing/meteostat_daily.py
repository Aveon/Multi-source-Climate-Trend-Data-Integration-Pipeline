import argparse
import glob
import logging
from pathlib import Path
from typing import Optional, Sequence

from pyspark.sql.functions import col, lit

try:
    from src.processing.spark_common import get_spark
    from src.processing.metrics import write_metrics
except ModuleNotFoundError:
    from spark_common import get_spark
    from metrics import write_metrics


logger = logging.getLogger(__name__)


REQUIRED_METEOSTAT_COLUMNS = (
    "date",
    "noaa_station_id",
    "station_name",
    "state",
    "country",
    "latitude",
    "longitude",
    "meteostat_station_id",
    "temp",
    "tmin",
    "tmax",
    "prcp",
    "snwd",
    "wspd",
    "run_date",
)


def find_usable_raw_paths(raw_glob: str) -> list[str]:
    """Return matched Meteostat raw files that contain actual JSON rows."""
    return [
        path
        for path in sorted(glob.glob(raw_glob))
        if Path(path).is_file() and Path(path).stat().st_size > 0
    ]


def get_missing_columns(columns: Sequence[str]) -> list[str]:
    """List required Meteostat fields that are missing from the loaded schema."""
    return [column for column in REQUIRED_METEOSTAT_COLUMNS if column not in columns]


def main(argv: Optional[list] = None) -> int:
    """Build the Meteostat daily parquet dataset from raw JSONL files."""
    parser = argparse.ArgumentParser(description="Normalize raw Meteostat daily records into curated daily climate data")
    parser.add_argument("--run-date", default=None, help="Run date to process from raw/meteostat/run_date=YYYY-MM-DD")
    parser.add_argument("--raw-glob", default=None, help="Glob for raw Meteostat JSONL files")
    parser.add_argument("--out-dir", default=None, help="Directory to save curated Meteostat parquet")
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    raw_glob = args.raw_glob or str(
        data_dir / "raw" / "meteostat" / f"run_date={args.run_date or '*'}" / "meteostat_daily.jsonl"
    )
    out_dir = args.out_dir or str(data_dir / "processed" / "meteostat_daily" / "parquet")
    run_date = args.run_date or "*"
    raw_paths = find_usable_raw_paths(raw_glob)
    if not raw_paths:
        print(f"No non-empty raw Meteostat files matched {raw_glob}")
        return 1

    spark = get_spark("meteostat_daily")
    try:
        df_raw = spark.read.json(raw_paths)
        raw_input_rows = df_raw.count()
        missing_columns = get_missing_columns(df_raw.columns)
        if missing_columns:
            print(
                "Skipping Meteostat processing because required columns are missing: "
                + ", ".join(missing_columns)
            )
            return 1

        max_wind_expr = col("wpgt").cast("double") if "wpgt" in df_raw.columns else lit(None).cast("double")

        df_normalized = (
            df_raw
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))
            .withColumn("parsed_date", col("date").cast("date"))
            .withColumn("avg_temp_value", col("temp").cast("double"))
            .withColumn("min_temp_value", col("tmin").cast("double"))
            .withColumn("max_temp_value", col("tmax").cast("double"))
            .withColumn("precip_value", col("prcp").cast("double"))
            .withColumn("avg_wind_value", col("wspd").cast("double"))
            .withColumn("max_wind_value", max_wind_expr)
            .withColumn("snow_value", col("snwd").cast("double"))
        )
        invalid_condition = (
            col("noaa_station_id").isNull()
            | col("meteostat_station_id").isNull()
            | col("parsed_date").isNull()
            | (
                col("avg_temp_value").isNull()
                & col("min_temp_value").isNull()
                & col("max_temp_value").isNull()
                & col("precip_value").isNull()
                & col("avg_wind_value").isNull()
                & col("max_wind_value").isNull()
                & col("snow_value").isNull()
            )
        )
        invalid_rows_removed = df_normalized.filter(invalid_condition).count()
        df_valid = df_normalized.filter(~invalid_condition).cache()
        valid_input_rows = df_valid.count()

        df_daily = (
            df_valid
            .select(
                lit("meteostat").alias("source"),
                col("parsed_date").alias("date"),
                col("noaa_station_id"),
                col("meteostat_station_id").alias("source_station_id"),
                col("station_name"),
                col("state"),
                col("country"),
                col("latitude"),
                col("longitude"),
                col("avg_temp_value").alias("avg_temp_c"),
                col("min_temp_value").alias("min_temp_c"),
                col("max_temp_value").alias("max_temp_c"),
                col("precip_value").alias("precip_mm"),
                (col("avg_wind_value") / lit(3.6)).alias("avg_wind_mps"),
                (col("max_wind_value") / lit(3.6)).alias("max_wind_mps"),
                (col("snow_value") * lit(10.0)).alias("snow_mm"),
                lit(1).alias("obs_count"),
                col("run_date").alias("ingest_run_date"),
            )
            .orderBy(col("date").desc(), col("noaa_station_id"))
            .cache()
        )
        output_rows = df_daily.count()

        df_daily.write.mode("overwrite").parquet(out_dir)
        if run_date != "*":
            write_metrics(
                "meteostat",
                run_date,
                {
                    "source": "meteostat",
                    "run_date": run_date,
                    "raw_input_rows": raw_input_rows,
                    "invalid_rows_removed": invalid_rows_removed,
                    "valid_input_rows": valid_input_rows,
                    "output_rows": output_rows,
                    "rows_consolidated_by_aggregation": max(0, valid_input_rows - output_rows),
                },
            )
        logger.debug("Saved curated Meteostat parquet to: %s", out_dir)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
