import argparse
import glob
from pathlib import Path
from typing import Optional

from pyspark.sql.functions import col, concat_ws, lit

try:
    from src.processing.spark_common import get_spark
except ModuleNotFoundError:
    from spark_common import get_spark


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize raw Open-Meteo daily records into curated daily climate data")
    parser.add_argument("--raw-glob", default=None, help="Glob for raw Open-Meteo JSONL files")
    parser.add_argument("--out-dir", default=None, help="Directory to save curated Open-Meteo parquet")
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    raw_glob = args.raw_glob or str(data_dir / "raw" / "open_meteo" / "run_date=*" / "open_meteo_daily.jsonl")
    out_dir = args.out_dir or str(data_dir / "processed" / "open_meteo_daily" / "parquet")
    raw_paths = sorted(glob.glob(raw_glob))
    if not raw_paths:
        print(f"No raw Open-Meteo files matched {raw_glob}")
        return 0

    spark = get_spark("open_meteo_daily")
    df_raw = spark.read.json(raw_paths)

    df_daily = (
        df_raw
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
        .select(
            lit("open_meteo").alias("source"),
            col("date").cast("date").alias("date"),
            col("noaa_station_id"),
            concat_ws(",", col("open_meteo_query_latitude"), col("open_meteo_query_longitude")).alias("source_station_id"),
            col("station_name"),
            col("state"),
            col("country"),
            col("latitude"),
            col("longitude"),
            col("temperature_2m_mean").cast("double").alias("avg_temp_c"),
            col("temperature_2m_min").cast("double").alias("min_temp_c"),
            col("temperature_2m_max").cast("double").alias("max_temp_c"),
            col("precipitation_sum").cast("double").alias("precip_mm"),
            col("wind_speed_10m_mean").cast("double").alias("avg_wind_mps"),
            col("wind_speed_10m_max").cast("double").alias("max_wind_mps"),
            lit(None).cast("double").alias("snow_mm"),
            lit(1).alias("obs_count"),
            col("run_date").alias("ingest_run_date"),
        )
        .orderBy(col("date").desc(), col("noaa_station_id"))
    )

    df_daily.write.mode("overwrite").parquet(out_dir)
    print(f"Saved curated Open-Meteo parquet to: {out_dir}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
