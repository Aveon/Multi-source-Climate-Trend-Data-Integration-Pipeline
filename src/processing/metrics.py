import json
from pathlib import Path
from typing import Any, Dict, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"


def metrics_path(source: str, run_date: str) -> Path:
    """Return the JSON file path used for per-source transformation metrics."""
    return DATA_DIR / "processed" / f"{source}_daily" / "metrics" / f"run_date={run_date}.json"


def write_metrics(source: str, run_date: str, metrics: Dict[str, Any]) -> Path:
    """Persist one source's transformation metrics as JSON."""
    out_path = metrics_path(source, run_date)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    return out_path


def load_metrics(source: str, run_date: str) -> Optional[Dict[str, Any]]:
    """Load one source's transformation metrics when present."""
    path = metrics_path(source, run_date)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
