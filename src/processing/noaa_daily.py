import argparse
import glob
from pathlib import Path
from typing import Optional, Sequence

from pyspark.sql.functions import (
    col,
    coalesce,
    count,
    lit,
    max as smax,
    round as sround,
    when,
)

try:
    from src.processing.spark_common import get_spark
except ModuleNotFoundError:
    from spark_common import get_spark


REQUIRED_NOAA_COLUMNS = (
    "noaa_station_id",
    "station_name",
    "state",
    "country",
    "latitude",
    "longitude",
    "run_date",
    "date",
    "datatype",
    "value",
)


def find_usable_raw_paths(raw_glob: str) -> list[str]:
    """Return matched NOAA raw files that contain actual JSON rows."""
    return [
        path
        for path in sorted(glob.glob(raw_glob))
        if Path(path).is_file() and Path(path).stat().st_size > 0
    ]


def get_missing_columns(columns: Sequence[str]) -> list[str]:
    """List required NOAA fields that are missing from the loaded schema."""
    return [column for column in REQUIRED_NOAA_COLUMNS if column not in columns]


def main(argv: Optional[list] = None) -> int:
    """Build the NOAA daily parquet dataset from raw JSONL files."""
    parser = argparse.ArgumentParser(description="Normalize raw NOAA daily records into curated daily climate data")
    parser.add_argument("--run-date", default=None, help="Run date to process from raw/noaa/run_date=YYYY-MM-DD")
    parser.add_argument("--raw-glob", default=None, help="Glob for raw NOAA JSONL files")
    parser.add_argument("--out-dir", default=None, help="Directory to save curated NOAA parquet")
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    raw_glob = args.raw_glob or str(
        data_dir / "raw" / "noaa" / f"run_date={args.run_date or '*'}" / "noaa_daily.jsonl"
    )
    out_dir = args.out_dir or str(data_dir / "processed" / "noaa_daily" / "parquet")
    raw_paths = find_usable_raw_paths(raw_glob)
    if not raw_paths:
        print(f"No non-empty raw NOAA files matched {raw_glob}")
        return 0

    spark = get_spark("noaa_daily")
    try:
        df_raw = spark.read.json(raw_paths)
        missing_columns = get_missing_columns(df_raw.columns)
        if missing_columns:
            print(
                "Skipping NOAA processing because required columns are missing: "
                + ", ".join(missing_columns)
            )
            return 0

        df_pivoted = (
            df_raw
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))
            .groupBy(
                "noaa_station_id",
                "station_name",
                "state",
                "country",
                "latitude",
                "longitude",
                "run_date",
                "date",
            )
            .agg(
                count("*").alias("obs_count"),
                smax(when(col("datatype") == "TAVG", col("value"))).alias("tavg_c"),
                smax(when(col("datatype") == "TMIN", col("value"))).alias("tmin_c"),
                smax(when(col("datatype") == "TMAX", col("value"))).alias("tmax_c"),
                smax(when(col("datatype") == "PRCP", col("value"))).alias("precip_mm"),
                smax(when(col("datatype") == "AWND", col("value"))).alias("avg_wind_mps"),
                smax(when(col("datatype") == "SNOW", col("value"))).alias("snow_mm"),
            )
            .select(
                lit("noaa").alias("source"),
                col("date").cast("date").alias("date"),
                col("noaa_station_id"),
                col("noaa_station_id").alias("source_station_id"),
                col("station_name"),
                col("state"),
                col("country"),
                col("latitude"),
                col("longitude"),
                coalesce(col("tavg_c"), sround((col("tmin_c") + col("tmax_c")) / 2.0, 2)).alias("avg_temp_c"),
                col("tmin_c").alias("min_temp_c"),
                col("tmax_c").alias("max_temp_c"),
                col("precip_mm"),
                col("avg_wind_mps"),
                lit(None).cast("double").alias("max_wind_mps"),
                col("snow_mm"),
                col("obs_count"),
                col("run_date").alias("ingest_run_date"),
            )
            .orderBy(col("date").desc(), col("noaa_station_id"))
        )

        df_pivoted.write.mode("overwrite").parquet(out_dir)
        print(f"Saved curated NOAA parquet to: {out_dir}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
