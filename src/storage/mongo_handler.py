import json
import os
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, ReplaceOne


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


def write_nws_to_mongo(
    raw_path: str,
    mongo_db=None,
    collection_name: Optional[str] = None,
    key_field: str = "_id",
) -> Dict[str, int]:
    """Read a raw NWS JSON file and upsert observation features into MongoDB."""
    db = mongo_db if mongo_db is not None else get_db()
    target_collection = (
        collection_name
        or os.getenv("MONGODB_COLLECTION")
        or os.getenv("MONGO_COLLECTION")
        or "observations"
    )
    col = db[target_collection]

    with open(raw_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    features = data.get("features", []) if isinstance(data, dict) else []
    ops = []
    for feat in features:
        props = feat.get("properties", {})
        doc_id = props.get("@id") or feat.get("id")
        if doc_id is None:
            continue

        doc = {
            key_field: doc_id,
            "timestamp": props.get("timestamp"),
            "stationId": props.get("stationId"),
            "station": props.get("station"),
            "properties": props,
            "geometry": feat.get("geometry"),
        }
        ops.append(ReplaceOne({key_field: doc_id}, doc, upsert=True))

    result = {"to_process": len(ops), "inserted": 0}
    if ops:
        bulk = col.bulk_write(ops, ordered=False)
        upserted = getattr(bulk, "upserted_count", 0)
        modified = getattr(bulk, "modified_count", 0)
        result["inserted"] = upserted + modified

    return result


def count_docs(db, collection: str) -> int:
    return db[collection].count_documents({})
