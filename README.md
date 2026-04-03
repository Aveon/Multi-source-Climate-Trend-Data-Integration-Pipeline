# Multi-source-Climate-Trend-Data-Integration-Pipeline

## Project Description

This project is a multi-source climate data pipeline that processes historical and recent weather observations into analytics-ready daily climate records. It uses Apache Spark for parallel transformation work and MongoDB for downstream analytical queries. The current pipeline integrates NOAA, Meteostat, and NWS data for a U.S.-focused station set and produces curated daily and yearly climate trend outputs.

## Data Source Identification
The system ingests heterogeneous data from three primary meteorological sources:

1. **National Oceanic and Atmospheric Administration (NOAA) Climate Data Online (CDO) API V2:** Primary source for historical daily summaries and global station metadata.
2. **National Weather Service (NWS) API:** Utilized for high-frequency observations and regional US climate data.
3. **Meteostat Bulk Daily Data:** Provides supplemental historical daily climate records via station/year bulk files.

## Setup Instructions

1. Clone the repository

```bash
git clone https://github.com/Aveon/Multi-source-Climate-Trend-Data-Integration-Pipeline.git
cd Multi-source-Climate-Trend-Data-Integration-Pipeline
```

2. Create a Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Install dependencies

```bash
pip install -r requirements.txt
```

Required libraries include `requests`, `pymongo`, `pyspark`, and `python-dotenv`.

## Environment Setup

Create a `.env` file in the project root.

Required variables:

```env
MONGODB_URI=your_mongodb_connection_string_here
NOAA_API_TOKEN=your_noaa_token_here
```

Default optional variables:

```env
MONGODB_DB=climate
MONGODB_COLLECTION=climate_daily
STATIONS_CSV=data/reference/weather_stations_master.csv
METEOSTAT_BATCH_SIZE=
METEOSTAT_BATCH_INDEX=1
```

## How to Run Pipeline

From the project root directory run:

```bash
.venv/bin/python src/main.py \
  --sources noaa,meteostat,nws
```

This will:

- Retrieve weather data from NOAA, Meteostat, and NWS (Over standard trailing 10 year period)
- Retrieve historical weather data from NOAA and Meteostat, plus recent observation data from NWS
- Save raw source responses locally in `data/raw/...`
- Build processed source-specific daily datasets in `data/processed/...`
- Build unified curated climate outputs in `data/curated/...`
- Send analytics-ready daily climate rows to MongoDB collection `climate.climate_daily`

If you omit `--date`, the pipeline uses the current day as the run date. If you omit `--start-date` and `--end-date`, it uses the default trailing 10-year historical window ending yesterday.

For a smaller test run:

```bash
.venv/bin/python src/main.py \
  --sources noaa,meteostat,nws \
  --limit-stations 3 \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD
```

This test command also uses the current day as the run date unless you add `--date YYYY-MM-DD`.

## Data Layout

Raw data is saved under:

- `data/raw/noaa/run_date=YYYY-MM-DD/noaa_daily.jsonl`
- `data/raw/meteostat/run_date=YYYY-MM-DD/meteostat_daily.jsonl`
- `data/raw/nws/run_date=YYYY-MM-DD/station=<NWS_ID>/nws_raw.json`

Processed data is saved under:

- `data/processed/noaa_daily/parquet`
- `data/processed/meteostat_daily/parquet`
- `data/processed/nws_daily/parquet`

Curated data is saved under:

- `data/curated/climate_daily/parquet`
- `data/curated/climate_trends/parquet`
- `data/curated/climate_trends_source_summary/parquet`

## MongoDB

MongoDB receives only analytics-ready daily climate rows from `data/curated/climate_daily/parquet`.

- Default database: `climate`
- Default collection: `climate_daily`
- Each document represents one source/station/day record with readable fields such as `source`, `station_name`, `date`, `avg_temp_c`, `precip_mm`, and `avg_wind_mps`

## Current Status

The core data pipeline is fully implemented and operational.

- Weather data ingestion is supported for NOAA, NWS, and Meteostat.
- Raw source responses are retained locally to support traceability, debugging, and selective reruns.
- Source-specific Spark transformation stages generate processed daily Parquet datasets.
- Unified curated climate datasets are produced for downstream daily analytics and yearly trend analysis.
- MongoDB stores analytics-ready normalized daily climate records rather than raw source payloads.
- The pipeline orchestrator supports full executions, partial reruns, station-limited test runs, and runtime status reporting.
- Error handling is implemented for transient API failures, station-level source failures, transformation validation errors, MongoDB retry scenarios, and resumable ingestion workflows where applicable.

Future work is focused on operational refinement, performance optimization, and presentation improvements rather than additional core pipeline development.
