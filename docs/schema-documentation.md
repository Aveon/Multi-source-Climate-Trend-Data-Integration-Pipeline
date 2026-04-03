# Schema Documentation

## Design Goal

The final schema is designed around analytical daily climate records rather than raw source payloads. The following below are converted into a daily analytical model that can be queried consistently across sources.

- multi-datatype NOAA rows
- high-frequency NWS observations
- source-specific station identifiers


## Final Output Model

The project has three final analytical outputs:

1. `climate_daily`
2. `climate_trends`
3. `climate_trends_source_summary`

Storage model:

- all three outputs are written locally as curated parquet datasets under `data/curated/...`
- only `climate_daily` is currently loaded into MongoDB
- `climate_trends` and `climate_trends_source_summary` currently exist only as local parquet outputs

## 1. `climate_daily`

Local parquet path:

- `data/curated/climate_daily/parquet`

MongoDB target:

- `climate.climate_daily`

Grain:

- One record per source/station/day

Why this grain:

- keeps the model intuitive for daily climate analysis
- allows direct comparisons across NOAA, Meteostat, and NWS
- avoids exposing source-native row structures to downstream consumers

Important design choices:

- `source_station_id` preserves the source-native identifier
- `noaa_station_id` is retained as a cross-source alignment key when available
- `obs_count` explains how many raw rows contributed to the daily record
- `ingest_run_date` preserves lineage for reruns and debugging
- `year` and `month` are materialized to simplify downstream filtering and aggregation

Why transformed counts can be lower than raw counts:

- NOAA raw data stores multiple rows per station-day, one per datatype
- NWS raw data stores multiple observations per day
- transformation consolidates those into one curated daily row
- some rows are also removed when they are invalid for analytical use

The pipeline tracks both invalid rows removed and rows consolidated during aggregation.

Where it lives:

- local curated parquet for pipeline outputs and reproducibility
- MongoDB for downstream querying and interpretation

## 2. `climate_trends`

Local parquet path:

- `data/curated/climate_trends/parquet`

MongoDB target:

- none currently

Grain:

- One record per source/station/year

Why this exists:

- supports year-over-year station-level trend analysis
- reduces query cost for annual rollups
- keeps station context attached to yearly summaries

Representative metrics:

- annual average temperature
- annual precipitation total
- annual average wind speed
- days covered in that year

Where it lives:

- local curated parquet only

## 3. `climate_trends_source_summary`

Local parquet path:

- `data/curated/climate_trends_source_summary/parquet`

MongoDB target:

- none currently

Grain:

- One record per source/year

Why this exists:

- Supports quick source-level comparisons by year
- Provides a lightweight summary view for dashboards and validation

Representative metrics:

- Stations covered
- Mean station temperature
- Mean station precipitation
- Mean station wind speed

Where it lives:

- local curated parquet only

## Source Harmonization Rationale

### NOAA

NOAA contributes strong historical daily coverage, but raw NOAA data is stored one datatype at a time. The pipeline combines those datatype rows into a single daily record.

### Meteostat

Meteostat provides supplemental daily climate history through station/year bulk files. Its raw structure is already close to the final daily model, so fewer rows are removed or consolidated.

### NWS

NWS provides recent high-frequency observations. The pipeline aggregates many intraday observations into one daily record so it fits the same final schema as NOAA and Meteostat.

## MongoDB Schema Notes

MongoDB currently stores only the curated daily model, not the yearly trend outputs. For `climate_daily`, the pipeline adds:

- `_id` for stable idempotent upserts
- `dataLevel`
- `recordType`

Rationale:

- Reruns should update existing documents instead of duplicating them
- The database should preserve enough metadata for lineage and debugging

## Constraints and Expectations

Stable expectations:

- `source` and `date` should always be present in curated daily output
- `source_station_id` should be populated for all curated daily rows
- `_id` in MongoDB should be unique per source/station/day

Nullable by design:

- Temperature, precipitation, wind, and snow fields can be null when the source did not provide them
- `noaa_station_id` can be null on NWS rows if no manifest mapping exists
- Station metadata such as `state`, `country`, `latitude`, and `longitude` may be null when upstream metadata is incomplete

## Consumer Contract

Treat `climate_daily` as the primary operational analytical contract.

Use:

- MongoDB `climate.climate_daily` for day-level querying
- local `climate_trends` parquet for station-year analysis
- local `climate_trends_source_summary` parquet for source-year summaries

Avoid building against:

- raw source payload formats
- transient raw folder contents
- source-specific ingestion-only metadata fields

