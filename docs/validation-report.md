# Validation Report

This report summarizes the validation evidence for the climate data pipeline. It covers the run artifacts currently available in the project workspace and the tracked query evidence under `docs/evidence/`.

## Run Evidence

The latest local metrics files found for `run_date=2026-04-21` show that the pipeline produced processed outputs for all three sources:

| Source | Raw input rows | Invalid rows removed | Valid input rows | Output rows | Rows consolidated |
| --- | ---: | ---: | ---: | ---: | ---: |
| NOAA | 1,044,178 | 0 | 1,044,178 | 182,585 | 861,593 |
| Meteostat | 182,439 | 23 | 182,416 | 182,416 | 0 |
| NWS | 5,000 | 35 | 4,965 | 63 | 4,902 |

Curated parquet success markers are present for:

- `data/curated/climate_daily/parquet`
- `data/curated/climate_trends/parquet`
- `data/curated/climate_trends_source_summary/parquet`

These generated data files are intentionally ignored by git. The tracked evidence files in `docs/evidence/` provide portable submission examples from MongoDB.

## Completeness

Record counts are captured at each processed source stage in:

- `data/processed/noaa_daily/metrics/run_date=2026-04-21.json`
- `data/processed/meteostat_daily/metrics/run_date=2026-04-21.json`
- `data/processed/nws_daily/metrics/run_date=2026-04-21.json`

MongoDB query evidence shows final records by source in `docs/evidence/samples/source_counts.json`:

| Source | Final MongoDB sample count |
| --- | ---: |
| Meteostat | 182,518 |
| NOAA | 182,384 |
| NWS | 59 |

Small differences between processed counts and MongoDB sample counts can occur when the evidence query is exported from a different completed run than the latest local metrics files. The project keeps `ingest_run_date` in curated records so rerun lineage can be checked.

## Accuracy

Sample final records are tracked in `docs/evidence/samples/sample_climate_daily_documents.json`. These rows verify that the pipeline produces readable daily climate records with expected fields such as:

- `source`
- `date`
- `station_name`
- `avg_temp_c`
- `precip_mm`
- `avg_wind_mps`

The transformation logic also applies source-specific normalization:

- NOAA datatype rows are pivoted into daily temperature, precipitation, wind, and snow columns.
- Meteostat daily records are cast into the shared climate schema.
- NWS intraday observations are aggregated into daily station summaries.

Sample validations were also performed by tracing representative records and result sets through the final output layers:

- sample MongoDB documents confirm that final records contain the expected climate fields
- source-count evidence confirms that all three sources appear in the final stored output
- yearly query evidence confirms that the curated output can be used for comparison and trend-style analysis

## Consistency

The final schema is documented in `docs/data-dictionary.md` and `docs/schema-documentation.md`.

Source processors validate required input columns before writing processed parquet. Rows with missing station keys, invalid dates, or no usable measurement values are excluded and counted in the metrics files.

The MongoDB storage layer adds stable `_id` values for idempotent upserts using source, station, and date.

## Reasonableness

Tracked evidence includes sanity-check queries:

- `docs/evidence/samples/source_counts.json` checks source coverage.
- `docs/evidence/samples/warmest_stations_2024.json` checks that annual temperature rankings are plausible.
- `docs/evidence/screenshots/` contains matching MongoDB Atlas screenshots.

For example, the 2024 warmest-station evidence ranks Phoenix, Honolulu, Miami, New Orleans, and Las Vegas near the top, which is directionally reasonable for U.S. climate data.

## Edge Case Behavior

| Edge case | Current behavior |
| --- | --- |
| Empty or missing input files | Processors return a non-zero exit code and log which raw path was missing. |
| API downtime or rate limiting | Ingestion retries temporary HTTP, timeout, connection, and rate-limit failures. |
| Malformed source records | Required columns and invalid parsed dates are checked before output. Invalid rows are counted and excluded. |
| Duplicate or repeated source rows | NOAA and NWS records are aggregated to daily source/station/date grain. MongoDB uses upserts to avoid duplicate stored documents. |
| Schema changes or unexpected fields | Processors validate required columns and ignore fields that are not part of the final analytical schema. |

## Performance Results

Observed run performance from the validated project runs:

- full `2026-04-21` run completed in `17m 50s`
- smoke run on `2026-05-03` completed in `22s`
- the pipeline processed `1,230,717` raw records into `364,907` curated daily records on the full run
- MongoDB loading completed with separate `processed`, `upserted`, `modified`, and `matched` counts for write visibility

## Known Limitations

- MongoDB evidence exports and local processed metrics may come from different completed run dates, so count comparisons should be interpreted alongside `ingest_run_date`
- `NWS` is a recent-observation source and is not intended to provide the same historical depth as `NOAA` and `Meteostat`
- yearly trend datasets are currently stored locally as curated parquet outputs and are not yet loaded into MongoDB
