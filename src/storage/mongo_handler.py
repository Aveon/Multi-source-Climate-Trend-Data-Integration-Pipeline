import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pymongo import MongoClient, ReplaceOne

try:
    from src.processing.spark_common import get_spark
except ModuleNotFoundError:
    try:
        from spark_common import get_spark
    except ModuleNotFoundError:
        get_spark = None


def get_db(mongo_uri: Optional[str] = None, db_name: Optional[str] = None):
    """Return a MongoDB database handle using args or environment variables."""
    uri = mongo_uri or os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGODB_URI not set in environment variables or argument")

    resolved_db_name = db_name or os.getenv("MONGODB_DB") or os.getenv("MONGO_DB") or "climate"
    client = MongoClient(uri)
    return client[resolved_db_name]


def db_to_records(df, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Collect a Spark DataFrame into Python dict records."""
    if limit:
        df = df.limit(limit)
    return [row.asDict(recursive=True) for row in df.collect()]


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {key: _normalize_scalar(value) for key, value in record.items()}


def build_climate_doc_id(record: Dict[str, Any]) -> str:
    source = record.get("source") or "unknown"
    station_id = record.get("source_station_id") or record.get("noaa_station_id") or "unknown_station"
    record_date = record.get("date") or "unknown_date"
    return f"{source}|{station_id}|{record_date}"


def upsert_documents(
    docs: Iterable[Dict[str, Any]],
    *,
    collection,
    key_field: str = "_id",
    batch_size: int = 1000,
) -> Dict[str, int]:
    total_docs = 0
    affected = 0
    batch: List = []

    for doc in docs:
        doc_id = doc.get(key_field)
        if doc_id is None:
            continue

        batch.append(ReplaceOne({key_field: doc_id}, doc, upsert=True))
        total_docs += 1

        if len(batch) >= batch_size:
            bulk = collection.bulk_write(batch, ordered=False)
            affected += getattr(bulk, "upserted_count", 0) + getattr(bulk, "modified_count", 0)
            batch = []

    if batch:
        bulk = collection.bulk_write(batch, ordered=False)
        affected += getattr(bulk, "upserted_count", 0) + getattr(bulk, "modified_count", 0)

    return {"to_process": total_docs, "inserted": affected}


def iter_parquet_records(parquet_path: str, *, data_level: str) -> Iterable[Dict[str, Any]]:
    if get_spark is None:
        raise RuntimeError("Spark helper is unavailable; cannot read processed parquet")

    spark = get_spark(f"mongo_{data_level}_export")
    try:
        df = spark.read.parquet(parquet_path)
        for row in df.toLocalIterator():
            normalized = _normalize_record(row.asDict(recursive=True))
            normalized["_id"] = build_climate_doc_id(normalized)
            normalized["dataLevel"] = data_level
            normalized["recordType"] = "climate_daily"
            yield normalized
    finally:
        spark.stop()


def write_processed_parquet_to_mongo(
    parquet_path: str,
    mongo_db=None,
    collection_name: Optional[str] = None,
    *,
    batch_size: int = 1000,
) -> Dict[str, int]:
    """Read processed parquet rows and upsert them into MongoDB."""
    db = mongo_db if mongo_db is not None else get_db()
    target_collection = (
        collection_name
        or os.getenv("MONGODB_COLLECTION")
        or os.getenv("MONGO_COLLECTION")
        or "processed_observations"
    )
    col = db[target_collection]
    return upsert_documents(
        iter_parquet_records(parquet_path, data_level="processed"),
        collection=col,
        key_field="_id",
        batch_size=batch_size,
    )


def write_curated_parquet_to_mongo(
    parquet_path: str,
    mongo_db=None,
    collection_name: Optional[str] = None,
    *,
    batch_size: int = 1000,
) -> Dict[str, int]:
    """Read curated parquet rows and upsert them into MongoDB."""
    db = mongo_db if mongo_db is not None else get_db()
    target_collection = (
        collection_name
        or os.getenv("MONGODB_COLLECTION")
        or os.getenv("MONGO_COLLECTION")
        or "climate_daily"
    )
    col = db[target_collection]
    return upsert_documents(
        iter_parquet_records(parquet_path, data_level="curated"),
        collection=col,
        key_field="_id",
        batch_size=batch_size,
    )


def infer_run_date_from_path(raw_path: str) -> Optional[str]:
    parts = Path(raw_path).parts
    for part in parts:
        if part.startswith("run_date="):
            return part.split("=", 1)[1]
    for idx, part in enumerate(parts):
        if part == "raw" and idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def build_nws_documents(
    raw_path: str,
    *,
    key_field: str = "_id",
) -> List[Dict[str, Any]]:
    """Transform a raw NWS feature collection into MongoDB-ready documents."""
    with open(raw_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    run_date = infer_run_date_from_path(raw_path)
    features = data.get("features", []) if isinstance(data, dict) else []
    docs: List[Dict[str, Any]] = []
    for feat in features:
        props = feat.get("properties", {})
        doc_id = props.get("@id") or feat.get("id")
        if doc_id is None:
            continue

        docs.append(
            {
                key_field: doc_id,
                "source": "nws",
                "dataLevel": "raw",
                "ingestRunDate": run_date,
                "timestamp": props.get("timestamp"),
                "stationId": props.get("stationId"),
                "station": props.get("station"),
                "stationName": props.get("stationName"),
                "textDescription": props.get("textDescription"),
                "properties": props,
                "geometry": feat.get("geometry"),
            }
        )
    return docs


def write_nws_to_mongo(
    raw_path: str,
    mongo_db=None,
    collection_name: Optional[str] = None,
    key_field: str = "_id",
) -> Dict[str, int]:
    """Legacy raw NWS writer. Prefer processed parquet writes for the main pipeline."""
    db = mongo_db if mongo_db is not None else get_db()
    target_collection = (
        collection_name
        or os.getenv("MONGODB_COLLECTION")
        or os.getenv("MONGO_COLLECTION")
        or "climate_daily"
    )
    col = db[target_collection]
    docs = build_nws_documents(raw_path, key_field=key_field)
    return upsert_documents(docs, collection=col, key_field=key_field)


def count_docs(db, collection: str) -> int:
    return db[collection].count_documents({})
