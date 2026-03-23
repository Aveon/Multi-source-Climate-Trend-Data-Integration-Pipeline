import argparse
import glob
from pathlib import Path
from typing import Optional

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


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize raw NOAA daily records into curated daily climate data")
    parser.add_argument("--raw-glob", default=None, help="Glob for raw NOAA JSONL files")
    parser.add_argument("--out-dir", default=None, help="Directory to save curated NOAA parquet")
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    raw_glob = args.raw_glob or str(data_dir / "raw" / "noaa" / "run_date=*" / "noaa_daily.jsonl")
    out_dir = args.out_dir or str(data_dir / "processed" / "noaa_daily" / "parquet")
    raw_paths = sorted(glob.glob(raw_glob))
    if not raw_paths:
        print(f"No raw NOAA files matched {raw_glob}")
        return 0

    spark = get_spark("noaa_daily")
    df_raw = spark.read.json(raw_paths)

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
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
