from pyspark.sql.functions import (
    to_date,
    col,
    avg,
    round as sround,
    min as smin,
    max as smax,
    count,
    when,
    from_utc_timestamp,
    input_file_name,
    regexp_extract,
    explode_outer,
)
import os
from pathlib import Path

# Spark session setup
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("nws_daily")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

def main():
    """Main processing: read raw NWS observations, normalize, and aggregate to daily level."""
    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    data_path = str(data_dir / "raw" / "*" / "nws_raw.json")
    
    try:
        # Raw files are GeoJSON FeatureCollection objects formatted across multiple lines.
        df_raw = spark.read.option("multiLine", True).json(data_path)
    except Exception as e:
        print(f"Error reading {data_path}: {e}")
        return

    # Flatten properties and extract source metadata.
    # In NWS raw files, properties lives under features[].properties.
    if "features" in df_raw.columns:
        df_props = (
            df_raw
            .select(explode_outer(col("features")).alias("feature"), input_file_name().alias("_src"))
            .select(col("feature.properties.*"), col("_src"))
        )
    elif "properties" in df_raw.columns:
        df_props = df_raw.select(col("properties.*"), input_file_name().alias("_src"))
    else:
        print(f"Error: expected 'features' or 'properties' in input schema, got columns={df_raw.columns}")
        return

    # Local timezone for daily grouping
    LOCAL_TZ = os.getenv("LOCAL_TZ", os.getenv("TZ", "America/New_York"))

    # Normalize types, create wind_speed_mps (convert km/h -> m/s), extract run_date
    df2 = (
        df_props
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        .withColumn("temperature", col("temperature.value").cast("double"))
        .withColumn("wind_speed_raw", col("windSpeed.value").cast("double"))
        .withColumn("wind_speed_unit", col("windSpeed.unitCode"))
        .withColumn(
            "wind_speed_mps",
            when(col("wind_speed_raw").isNull(), None)
            .when(col("wind_speed_unit").contains("km_h"), col("wind_speed_raw") / 3.6)
            .otherwise(col("wind_speed_raw")),
        )
        .withColumn("description", col("textDescription"))
        .withColumn("station", col("stationId"))
        .withColumn("run_date", regexp_extract(col("_src"), r"raw/([^/]+)/", 1))
        .withColumn("local_timestamp", from_utc_timestamp(col("timestamp"), LOCAL_TZ))
    )

    # Aggregate to daily level
    df_daily = (
        df2
        .withColumn("date", to_date(col("local_timestamp")))
        .groupBy("station", "run_date", "date")
        .agg(
            count("*").alias("obs_count"),
            sround(avg("temperature"), 2).alias("avg_temp_C"),
            smin("temperature").alias("min_temp_C"),
            smax("temperature").alias("max_temp_C"),
            sround(avg("wind_speed_mps"), 2).alias("avg_wind_mps"),
            sround((avg(when(col("wind_speed_mps").isNull(), 1).otherwise(0)) * 100), 2).alias("wind_missing_pct"),
            (avg(when(col("description").isNull(), 1).otherwise(0)) * 100).alias("desc_missing_pct"),
        )
        .orderBy(col("date").desc())
    )

    # Show output
    print("=== Daily Aggregated NWS Data ===")
    df_daily.show(truncate=False)

    curated_out = data_dir / "curated" / "nws_daily" / "parquet"
    (
        df_daily
        .write
        .mode("overwrite")
        .parquet(str(curated_out))
    )
    print(f"Saved curated daily parquet to: {curated_out}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
