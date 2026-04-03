# Pipeline Documentation

## Overview

This pipeline follows a simple flow:

1. Read the station list and run settings
2. Pull raw data from each source
3. Turn raw data into source-level daily datasets
4. Combine those daily datasets into curated outputs
5. Send the curated daily dataset to MongoDB

Main entry point:

```bash
.venv/bin/python src/main.py
```

End-to-end flow:

`station manifest -> raw source files -> processed daily parquet -> curated parquet -> MongoDB`

## What Goes In

Before the pipeline runs, it needs:

- A station manifest file
- Source settings and date range
- MongoDB connection settings

Main input file:

- `data/reference/weather_stations_master.csv`

Key environment variables:

| Variable | Purpose | Default |
| --- | --- | --- |
| `MONGODB_URI` | MongoDB connection string | none |
| `MONGODB_DB` | MongoDB database name | `climate` |
| `MONGODB_COLLECTION` | MongoDB collection name | `climate_daily` |
| `STATION` | Default single-station override for ad hoc NWS runs | `KATL` |
| `USER_AGENT` | User agent used for NOAA and NWS requests | `BigDataProject (mailto:...)` |
| `STATIONS_CSV` | Station manifest path | `data/reference/weather_stations_master.csv` |
| `NOAA_API_TOKEN` | NOAA token for API mode | empty |
| `NOAA_DATASET_ID` | NOAA dataset id | `GHCND` |
| `METEOSTAT_BATCH_SIZE` | Optional Meteostat batching size | empty |
| `METEOSTAT_BATCH_INDEX` | 1-based Meteostat batch index | `1` |

Main CLI options:

| Option | Purpose |
| --- | --- |
| `--date` | Run date used in raw pathing and lineage |
| `--start-date` | Historical window start date |
| `--end-date` | Historical window end date |
| `--sources` | Comma-separated sources to run |
| `--station` | Single NWS station override |
| `--stations-csv` | Station manifest path |
| `--limit-stations` | Limit station count for tests |
| `--meteostat-batch-size` | Batch size for partitioned Meteostat runs |
| `--meteostat-batch-index` | Batch index for partitioned Meteostat runs |
| `--skip-ingest` | Skip source retrieval |
| `--skip-process` | Skip Spark transformations |
| `--skip-mongo` | Skip MongoDB load |

## Step 1: Load Run Settings

The orchestrator reads:

- Which sources to run
- Which stations to include
- The date range for the run
- Where outputs should be written
- Whether any stages should be skipped

At this point, the pipeline knows what work it needs to do before it starts pulling data.

## Step 2: Pull Raw Source Data

The pipeline starts ingestion for the selected sources. Each source writes raw files to `data/raw/...`.

### NOAA Raw Pull

Module:

- `src/ingestion/noaa_acquire.py`

What it does:

- Pulls historical daily data from NOAA
- Uses bulk station ingestion by default
- Can use API mode when `NOAA_INGEST_MODE=api`
- Retries temporary HTTP and network failures
- Logs failed stations to `noaa_failed_stations.jsonl`
- Keeps going if one station fails

Raw output:

- `data/raw/noaa/run_date=YYYY-MM-DD/noaa_daily.jsonl`

### Meteostat Raw Pull

Module:

- `src/ingestion/meteostat_acquire.py`

What it does:

- Matches each manifest row to a Meteostat station
- Downloads yearly bulk daily files by station and year
- Supports resume behavior for interrupted runs
- Retries temporary HTTP and network failures
- Logs failed stations to `meteostat_failed_stations.jsonl`
- Writes station data only after the full station finishes

Raw output:

- `data/raw/meteostat/run_date=YYYY-MM-DD/meteostat_daily.jsonl`

### NWS Raw Pull

Module:

- `src/ingestion/nws_acquire.py`

What it does:

- Pulls recent observation payloads for each NWS station
- Retries temporary HTTP and network failures
- Logs failed stations to `nws_failed_stations.jsonl`
- Keeps going if one station fails
- Removes stale station folders when rerunning the same date with fewer stations

Raw output:

- `data/raw/nws/run_date=YYYY-MM-DD/station=<NWS_ID>/nws_raw.json`

## Step 3: Build Source-Level Daily Datasets

After raw files are saved, Spark turns each source into a daily dataset with a shared shape.

Source processors:

- `src/processing/noaa_daily.py`
- `src/processing/meteostat_daily.py`
- `src/processing/nws_daily.py`

What each processor does:

- Reads raw files for the selected `run_date`
- Checks that needed columns are present
- Drops rows that are not usable
- Maps source fields into a common daily schema
- Writes daily Parquet output
- Writes run metrics for that source

Processed outputs:

- `data/processed/noaa_daily/parquet`
- `data/processed/meteostat_daily/parquet`
- `data/processed/nws_daily/parquet`

Metrics output:

- `data/processed/<source>_daily/metrics/run_date=YYYY-MM-DD.json`

Tracked metrics:

- `raw_input_rows`
- `invalid_rows_removed`
- `valid_input_rows`
- `output_rows`
- `rows_consolidated_by_aggregation`

## Step 4: Build Curated Datasets

Once the source-level daily datasets are ready, the pipeline combines them into final analytics datasets.

Module:

- `src/processing/climate_unified.py`

What it does:

- Unions the processed daily datasets across sources
- Adds `year` and `month`
- Writes the curated daily dataset
- Builds yearly station trends
- Builds yearly source summary trends

Curated outputs:

- `data/curated/climate_daily/parquet`
- `data/curated/climate_trends/parquet`
- `data/curated/climate_trends_source_summary/parquet`

What each curated dataset is for:

- `climate_daily`: detailed daily rows by source, station, and date
- `climate_trends`: yearly station-level summaries
- `climate_trends_source_summary`: yearly source-level summaries

## Step 5: Send Daily Curated Data to MongoDB

Only the curated daily dataset is written to MongoDB.

Module:

- `src/storage/mongo_handler.py`

What it does:

- Reads rows from `data/curated/climate_daily/parquet`
- Normalizes values before write
- Builds stable `_id` values
- Adds Mongo-only fields used by the app
- Upserts documents into the target collection
- Retries temporary MongoDB connection failures during bulk writes

Default destination:

- database: `climate`
- collection: `climate_daily`

MongoDB gets:

- One document per source + station + date

MongoDB does not get:

- `data/curated/climate_trends/parquet`
- `data/curated/climate_trends_source_summary/parquet`

## Retry and Failure Handling

The pipeline is built to keep going when it can and retry temporary failures.

Handled gracefully:

- temporary HTTP failures
- rate limits
- timeouts and connection errors
- station-level source failures
- resumable Meteostat raw ingestion
- stale NWS station folders on reruns
- missing transformation inputs
- temporary MongoDB write failures
- temporary Spark transformation failures

Failure ledgers:

- `data/raw/noaa/run_date=YYYY-MM-DD/noaa_failed_stations.jsonl`
- `data/raw/meteostat/run_date=YYYY-MM-DD/meteostat_failed_stations.jsonl`
- `data/raw/nws/run_date=YYYY-MM-DD/nws_failed_stations.jsonl`

Spark retry behavior:

- retries NWS transformation
- retries NOAA transformation
- retries Meteostat transformation
- retries unified climate transformation
- up to 3 attempts
- exponential backoff starting at 5 seconds

## What You See During a Run

Normal run output is kept short. You will usually see:

- a start line
- source fetch lines
- a raw data summary
- transformation start and finish lines
- MongoDB write start and finish lines
- a final duration line

Warnings and errors still appear when the pipeline retries, skips a station, or fails a stage.
