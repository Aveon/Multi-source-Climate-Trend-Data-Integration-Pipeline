# Data Dictionary

This document lists the pipeline's final analytical datasets and the MongoDB documents created from them.

## Main Outputs

- `data/curated/climate_daily/parquet`
- `data/curated/climate_trends/parquet`
- `data/curated/climate_trends_source_summary/parquet`
- MongoDB collection: `climate.climate_daily`

`climate_daily` is the main output dataset. It is written to both Parquet and MongoDB.

## `climate_daily`

Purpose:
- Daily climate records combined across NOAA, NWS, and Meteostat

Grain:
- One row per `source` + `source_station_id` + `date`

Fields:

| Field | Type | Notes |
| --- | --- | --- |
| `source` | string | `noaa`, `nws`, or `meteostat` |
| `date` | date | Observation day |
| `noaa_station_id` | string | Shared station key used across sources when available |
| `source_station_id` | string | Source-specific station ID |
| `station_name` | string | Station name |
| `state` | string | State or region code |
| `country` | string | Country code |
| `latitude` | double | Station latitude |
| `longitude` | double | Station longitude |
| `avg_temp_c` | double | Average daily temperature in Celsius |
| `min_temp_c` | double | Minimum daily temperature in Celsius |
| `max_temp_c` | double | Maximum daily temperature in Celsius |
| `precip_mm` | double | Daily precipitation in millimeters |
| `avg_wind_mps` | double | Average wind speed in meters per second |
| `max_wind_mps` | double | Maximum wind speed in meters per second |
| `snow_mm` | double | Snow-related value in millimeters |
| `obs_count` | long | Number of raw observations rolled into the daily row |
| `ingest_run_date` | string | Pipeline run date in `YYYY-MM-DD` format |
| `year` | int | Year derived from `date` |
| `month` | int | Month derived from `date` |

## `climate_trends`

Purpose:
- Yearly station-level climate summary

Grain:
- One row per `source` + `noaa_station_id` + `year`

Fields:

| Field | Type | Notes |
| --- | --- | --- |
| `source` | string | Data source |
| `noaa_station_id` | string | Station key used for grouping |
| `station_name` | string | Station name |
| `state` | string | State or region code |
| `country` | string | Country code |
| `year` | int | Calendar year |
| `latitude` | double | Representative latitude |
| `longitude` | double | Representative longitude |
| `days_covered` | long | Number of daily rows in the yearly summary |
| `annual_avg_temp_c` | double | Mean daily average temperature |
| `annual_avg_min_temp_c` | double | Mean daily minimum temperature |
| `annual_avg_max_temp_c` | double | Mean daily maximum temperature |
| `annual_precip_mm` | double | Total yearly precipitation |
| `annual_avg_wind_mps` | double | Mean daily wind speed |

## `climate_trends_source_summary`

Purpose:
- Yearly source-level summary for comparison across data sources

Grain:
- One row per `source` + `year`

Fields:

| Field | Type | Notes |
| --- | --- | --- |
| `source` | string | Data source |
| `year` | int | Calendar year |
| `stations_covered` | long | Number of station summaries included |
| `mean_station_temp_c` | double | Mean station temperature for the year |
| `mean_station_precip_mm` | double | Mean station precipitation for the year |
| `mean_station_wind_mps` | double | Mean station wind speed for the year |

## MongoDB Fields

MongoDB stores the curated daily dataset only.

Each MongoDB document contains the fields from `climate_daily` plus:

| Field | Type | Notes |
| --- | --- | --- |
| `_id` | string | Stable key used for upserts |
| `dataLevel` | string | Current value: `curated` |
| `recordType` | string | Current value: `climate_daily` |
