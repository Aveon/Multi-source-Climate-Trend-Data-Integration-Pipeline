import argparse
import glob
from pathlib import Path
from typing import Optional

from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    explode_outer,
    from_utc_timestamp,
    input_file_name,
    lit,
    max as smax,
    min as smin,
    regexp_extract,
    round as sround,
    to_date,
    when,
)

try:
    from src.processing.spark_common import get_spark
except ModuleNotFoundError:
    from spark_common import get_spark


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize raw NWS observations into a daily climate dataset")
    parser.add_argument("--raw-glob", default=None, help="Glob for raw NWS JSON files")
    parser.add_argument("--stations-csv", default=None, help="Station manifest CSV for metadata enrichment")
    parser.add_argument("--out-dir", default=None, help="Directory to save curated NWS parquet")
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    raw_glob = args.raw_glob or str(data_dir / "raw" / "nws" / "run_date=*" / "station=*" / "nws_raw.json")
    raw_paths = sorted(glob.glob(raw_glob))
    if not raw_paths:
        legacy_glob = str(data_dir / "raw" / "*" / "nws_raw.json")
        raw_paths = sorted(glob.glob(legacy_glob))
    if not raw_paths:
        print(f"No raw NWS files matched {raw_glob}")
        return 0

    stations_csv = args.stations_csv or str(data_dir / "reference" / "weather_stations_master.csv")
    out_dir = args.out_dir or str(data_dir / "processed" / "nws_daily" / "parquet")
    local_tz = "America/New_York"

    spark = get_spark("nws_daily")

    df_raw = spark.read.option("multiLine", True).json(raw_paths)
    df_props = (
        df_raw
        .select(
            explode_outer(col("features")).alias("feature"),
            input_file_name().alias("_src"),
        )
        .select(
            col("feature.properties.*"),
            col("feature.geometry.coordinates").alias("geometry_coordinates"),
            col("_src"),
        )
    )

    manifest_df = (
        spark.read
        .option("header", True)
        .csv(stations_csv)
        .select(
            col("noaa_station_id"),
            col("nws_station_id").alias("manifest_nws_station_id"),
            col("station_name").alias("manifest_station_name"),
            col("state"),
            col("country"),
            col("latitude").cast("double").alias("manifest_latitude"),
            col("longitude").cast("double").alias("manifest_longitude"),
        )
    )

    df_daily = (
        df_props
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        .withColumn("temperature_c", col("temperature.value").cast("double"))
        .withColumn("wind_speed_raw", col("windSpeed.value").cast("double"))
        .withColumn("wind_speed_unit", col("windSpeed.unitCode"))
        .withColumn(
            "wind_speed_mps",
            when(col("wind_speed_raw").isNull(), None)
            .when(col("wind_speed_unit").contains("km_h"), col("wind_speed_raw") / 3.6)
            .otherwise(col("wind_speed_raw")),
        )
        .withColumn("run_date", regexp_extract(col("_src"), r"run_date=([^/]+)/", 1))
        .withColumn(
            "run_date",
            when(col("run_date") == "", regexp_extract(col("_src"), r"raw/([^/]+)/", 1)).otherwise(col("run_date")),
        )
        .withColumn("local_timestamp", from_utc_timestamp(col("timestamp"), local_tz))
        .withColumn("date", to_date(col("local_timestamp")))
        .groupBy("stationId", "stationName", "run_date", "date", "geometry_coordinates")
        .agg(
            count("*").alias("obs_count"),
            sround(avg("temperature_c"), 2).alias("avg_temp_c"),
            smin("temperature_c").alias("min_temp_c"),
            smax("temperature_c").alias("max_temp_c"),
            sround(avg("wind_speed_mps"), 2).alias("avg_wind_mps"),
            sround(smax("wind_speed_mps"), 2).alias("max_wind_mps"),
        )
        .join(
            manifest_df,
            col("stationId") == col("manifest_nws_station_id"),
            "left",
        )
        .select(
            lit("nws").alias("source"),
            col("date"),
            col("noaa_station_id"),
            col("stationId").alias("source_station_id"),
            coalesce(col("manifest_station_name"), col("stationName")).alias("station_name"),
            col("state"),
            col("country"),
            coalesce(col("manifest_latitude"), col("geometry_coordinates")[1].cast("double")).alias("latitude"),
            coalesce(col("manifest_longitude"), col("geometry_coordinates")[0].cast("double")).alias("longitude"),
            col("avg_temp_c"),
            col("min_temp_c"),
            col("max_temp_c"),
            lit(None).cast("double").alias("precip_mm"),
            col("avg_wind_mps"),
            col("max_wind_mps"),
            lit(None).cast("double").alias("snow_mm"),
            col("obs_count"),
            col("run_date").alias("ingest_run_date"),
        )
        .orderBy(col("date").desc(), col("source_station_id"))
    )

    df_daily.write.mode("overwrite").parquet(out_dir)
    print(f"Saved curated NWS parquet to: {out_dir}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
