import os
import sys
import argparse
import logging
import importlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("main")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)  # always use this repo's .env values


def first(*vals):
    """Return the first non-empty value."""
    for v in vals:
        if v is not None and v != "":
            return v
    return None


def load_config() -> Dict[str, Any]:
    """
    Configuration priority:
      1) .env in project root (loaded with override=True)
      2) Existing environment variables (for keys missing in .env)
      3) Defaults
    """
    return {
        "MONGODB_URI": os.getenv("MONGODB_URI", ""),
        "MONGODB_DB": first(os.getenv("MONGODB_DB"), "climate"),
        "MONGODB_COLLECTION": first(os.getenv("MONGODB_COLLECTION"), "observations"),
        "STATION": first(os.getenv("STATION"), "KATL"),
        "USER_AGENT": first(
            os.getenv("USER_AGENT"),
            "BigDataProject (mailto:joshua.young96@gmail.com)",
        ),
    }


def import_module_flexible(module_a: str, module_b: str):
    """
    Try import module_a (package run), else module_b (direct run).
    """
    try:
        return importlib.import_module(module_a)
    except ModuleNotFoundError:
        return importlib.import_module(module_b)


def run_ingestion(cfg: Dict[str, Any], out_dir: str, run_date: str) -> str:
    os.makedirs(out_dir, exist_ok=True)

    ingest = import_module_flexible("src.ingestion.nws_acquire", "ingestion.nws_acquire")

    args = [
        "--station", cfg["STATION"],
        "--user-agent", cfg["USER_AGENT"],
        "--out-dir", out_dir,
        "--date", run_date,
    ]

    logger.info("Running ingestion for station %s (saving to %s)", cfg["STATION"], out_dir)
    rc = ingest.main(args)
    if rc:
        raise RuntimeError(f"Ingestion failed with exit code {rc}")

    raw_path = os.path.join(out_dir, "nws_raw.json")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Expected raw JSON at {raw_path} after ingestion")

    return raw_path


def store_to_mongo(raw_path: str, cfg: Dict[str, Any]) -> Dict[str, int]:
    if not cfg.get("MONGODB_URI"):
        raise ValueError("MONGODB_URI is missing. Put it in .env or your shell environment.")

    storage = import_module_flexible("src.storage.mongo_handler", "storage.mongo_handler")
    get_db = getattr(storage, "get_db")
    write_nws_to_mongo = getattr(storage, "write_nws_to_mongo")
    mongo_db = get_db(mongo_uri=cfg["MONGODB_URI"], db_name=cfg["MONGODB_DB"])

    return write_nws_to_mongo(
        raw_path,
        mongo_db=mongo_db,
        collection_name=cfg["MONGODB_COLLECTION"],
    )


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Project pipeline orchestrator")
    p.add_argument("--date", default=None, help="YYYY-MM-DD (default: today UTC)")
    p.add_argument("--out-dir", default=None, help="Directory to save raw JSON (default: data/raw/<date>)")
    p.add_argument("--station", default=None, help="Weather station code (default from env)")
    p.add_argument("--skip-ingest", action="store_true", help="Skip ingestion and use existing raw JSON in out-dir")
    p.add_argument("--skip-mongo", action="store_true", help="Skip MongoDB storage step")
    args = p.parse_args(argv)

    cfg = load_config()

    # CLI overrides
    if args.station:
        cfg["STATION"] = args.station

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = args.out_dir or str(DATA_DIR / "raw" / run_date)
    raw_path = os.path.join(out_dir, "nws_raw.json")

    try:
        if args.skip_ingest:
            if not os.path.exists(raw_path):
                raise FileNotFoundError(f"Skipping ingestion but expected raw JSON at {raw_path} not found")
            logger.info("Skipping ingestion; using existing raw JSON at %s", raw_path)
        else:
            raw_path = run_ingestion(cfg, out_dir=out_dir, run_date=run_date)
    except Exception:
        logger.exception("Error during ingestion")
        return 1

    try:
        if args.skip_mongo:
            logger.info("Skipping MongoDB storage step")
        else:
            summary = store_to_mongo(raw_path, cfg)
            logger.info("MongoDB storage summary: %s", summary)
    except Exception:
        logger.exception("Error during MongoDB storage")
        return 2

    logger.info("Pipeline completed successfully; raw saved to %s", raw_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

    #123
    
