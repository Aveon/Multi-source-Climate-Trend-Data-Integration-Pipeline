# Multi-source-Climate-Trend-Data-Integration-Pipeline

## Project Description

The project is multi-level Big Data system stack that aims to process 21.9 million pieces of climate data from 2016 to the current year. It utilizes a distributed system that combines Apache Spark for parallel processing, the Hadoop Distributed File System (HDFS) for fault-tolerant storage, and MongoDB for analytical queries. The project's goal is to identify long-term trends for temperature and precipitation levels from 2,000 worldwide weather stations across the United States. 

## Data Source Identification
The system ingests heterogeneous data from three primary meteorological sources:

1. **National Oceanic and Atmospheric Administration (NOAA) Climate Data Online (CDO) API V2:** Primary source for historical daily summaries and global station metadata.
2. **National Weather Service (NWS) API:** Utilized for high-frequency observations and regional US climate data.
3. **Open-Meteo Historical Weather API:** Provides supplemental high-resolution global climate records.

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
OPEN_METEO_MODEL=era5
OPEN_METEO_TIMEZONE=auto
```

## How to Run Pipeline

From the project root directory run:

```bash
.venv/bin/python src/main.py \
  --sources noaa,open_meteo,nws \
  --start-date 2016-01-01 \
  --end-date 2025-12-31
```

This will:

- Retrieve weather data from NOAA, Open-Meteo, and NWS
- Save raw source responses locally in `data/raw/...`
- Build processed source-specific daily datasets in `data/processed/...`
- Build unified curated climate outputs in `data/curated/...`
- Send analytics-ready daily climate rows to MongoDB collection `climate.climate_daily`

For a smaller test run:

```bash
.venv/bin/python src/main.py \
  --sources noaa,open_meteo \
  --limit-stations 3 \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

## Data Layout

Raw data is saved under:

- `data/raw/noaa/run_date=YYYY-MM-DD/noaa_daily.jsonl`
- `data/raw/open_meteo/run_date=YYYY-MM-DD/open_meteo_daily.jsonl`
- `data/raw/nws/run_date=YYYY-MM-DD/station=<NWS_ID>/nws_raw.json`

Processed data is saved under:

- `data/processed/noaa_daily/parquet`
- `data/processed/open_meteo_daily/parquet`
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

### Completed

- Weather data acquisition is wired for NOAA, NWS, and Open-Meteo.
- Raw source data storage is in place for traceability.
- MongoDB integration stores daily climate rows instead of raw API payloads.
- Modular pipeline structure is in place: ingestion -> processing -> storage.
- Environment configuration support using `.env` is working.
- Source-specific processed datasets and unified curated trend datasets are being generated.

### In Progress

- Full live validation of all sources together across longer historical windows.
- NOAA API reliability handling for temporary service interruptions like `503` responses.
- Documentation and operational cleanup for larger-scale historical runs.

### Planned

- NOAA bulk historical ingestion for better scaling toward 2,000 stations.
- Geographic station expansion across the U.S. with balanced coverage.
- Additional performance tuning for large multi-year backfills and downstream analytics.
