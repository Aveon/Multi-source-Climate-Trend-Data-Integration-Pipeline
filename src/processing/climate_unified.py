import argparse
import glob
from pathlib import Path
from typing import List, Optional

from pyspark.sql.functions import (
    avg,
    col,
    count,
    first,
    lit,
    month,
    round as sround,
    sum as ssum,
    year,
)

try:
    from src.processing.spark_common import get_spark
except ModuleNotFoundError:
    from spark_common import get_spark


PROCESSED_PATHS = {
    "nws": "nws_daily/parquet",
    "noaa": "noaa_daily/parquet",
    "open_meteo": "open_meteo_daily/parquet",
}


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Union processed source datasets and build curated trend-ready outputs")
    parser.add_argument("--sources", default="nws,noaa,open_meteo", help="Comma-separated processed sources to include")
    parser.add_argument("--out-dir", default=None, help="Directory to save curated unified climate parquet")
    parser.add_argument("--trend-dir", default=None, help="Directory to save yearly trend parquet")
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    out_dir = args.out_dir or str(data_dir / "curated" / "climate_daily" / "parquet")
    trend_dir = args.trend_dir or str(data_dir / "curated" / "climate_trends" / "parquet")

    sources = [item.strip() for item in args.sources.split(",") if item.strip()]
    spark = get_spark("climate_unified")

    frames: List = []
    for source in sources:
        relative_path = PROCESSED_PATHS.get(source)
        if not relative_path:
            continue
        source_path = str(data_dir / "processed" / relative_path)
        if not glob.glob(source_path):
            continue
        frames.append(spark.read.parquet(source_path))

    if not frames:
        print(f"No processed datasets found for sources={sources}")
        spark.stop()
        return 0

    unified = frames[0]
    for frame in frames[1:]:
        unified = unified.unionByName(frame, allowMissingColumns=True)

    climate_daily = (
        unified
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .orderBy(col("date").desc(), col("source"), col("source_station_id"))
    )
    climate_daily.write.mode("overwrite").parquet(out_dir)

    yearly_station = (
        climate_daily
        .groupBy("source", "noaa_station_id", "station_name", "state", "country", "year")
        .agg(
            first("latitude", ignorenulls=True).alias("latitude"),
            first("longitude", ignorenulls=True).alias("longitude"),
            count("*").alias("days_covered"),
            sround(avg("avg_temp_c"), 2).alias("annual_avg_temp_c"),
            sround(avg("min_temp_c"), 2).alias("annual_avg_min_temp_c"),
            sround(avg("max_temp_c"), 2).alias("annual_avg_max_temp_c"),
            sround(ssum("precip_mm"), 2).alias("annual_precip_mm"),
            sround(avg("avg_wind_mps"), 2).alias("annual_avg_wind_mps"),
        )
        .orderBy(col("year").desc(), col("source"), col("noaa_station_id"))
    )
    yearly_station.write.mode("overwrite").parquet(trend_dir)

    source_summary_dir = str(data_dir / "curated" / "climate_trends_source_summary" / "parquet")
    yearly_source = (
        yearly_station
        .groupBy("source", "year")
        .agg(
            count("*").alias("stations_covered"),
            sround(avg("annual_avg_temp_c"), 2).alias("mean_station_temp_c"),
            sround(avg("annual_precip_mm"), 2).alias("mean_station_precip_mm"),
            sround(avg("annual_avg_wind_mps"), 2).alias("mean_station_wind_mps"),
        )
        .orderBy(col("year").desc(), col("source"))
    )
    yearly_source.write.mode("overwrite").parquet(source_summary_dir)

    print(f"Saved unified climate parquet to: {out_dir}")
    print(f"Saved yearly station trends to: {trend_dir}")
    print(f"Saved yearly source trends to: {source_summary_dir}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
