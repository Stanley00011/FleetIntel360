import json
import logging
from pathlib import Path
from typing import List, Dict

# Config
RAW_DRIVER_HEALTH_PATH = Path("warehouse/raw/driver_health")
STAGED_OUT_PATH = Path("warehouse/staging/driver_health_staged.jsonl")

# Identity fields only (hard requirement)
REQUIRED_FIELDS = {
    "event_id",
    "driver_id",
    "timestamp",
}

# Telemetry fields (nullable but expected)
OPTIONAL_FIELDS = {
    "shift_hours",
    "continuous_driving_hours",
    "fatigue_index",
    "breaks_taken",
    "alerts",
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Loaders
def load_raw_driver_health_files() -> List[Dict]:
    """
    Load all driver health JSONL files from raw layer.
    Raw layer is immutable and untrusted.
    """
    records: List[Dict] = []

    if not RAW_DRIVER_HEALTH_PATH.exists():
        raise FileNotFoundError("Raw driver health directory does not exist")

    files = sorted(RAW_DRIVER_HEALTH_PATH.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError("No raw driver health files found")

    logger.info("Found %s raw driver health files", len(files))

    for file in files:
        with file.open() as f:
            for line in f:
                records.append(json.loads(line))

    logger.info("Loaded %s raw driver health records", len(records))
    return records

# Validation
def validate_required_fields(record: Dict) -> None:
    missing = REQUIRED_FIELDS - record.keys()
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

def quality_checks(record: Dict) -> None:
    """
    Hard safety checks only.
    """
    if record.get("shift_hours") is not None and record["shift_hours"] < 0:
        raise ValueError("Negative shift hours")

    if record.get("continuous_driving_hours") is not None and record["continuous_driving_hours"] < 0:
        raise ValueError("Negative continuous driving hours")

    fatigue = record.get("fatigue_index")
    if fatigue is not None and not (0 <= fatigue <= 1):
        raise ValueError("Invalid fatigue index")

# Staging transform
def stage_record(record: Dict) -> Dict:
    return {
        "event_id": record["event_id"],
        "driver_id": record["driver_id"],
        "timestamp": record["timestamp"],

        # Telemetry (nullable)
        "shift_hours": record.get("shift_hours"),
        "continuous_driving_hours": record.get("continuous_driving_hours"),
        "fatigue_index": record.get("fatigue_index"),
        "breaks_taken": record.get("breaks_taken"),
        "alerts": record.get("alerts", []),
    }

# Orchestrator
def stage_driver_health() -> None:
    logger.info("Starting driver health staging")

    raw_records = load_raw_driver_health_files()
    staged_records: List[Dict] = []

    for record in raw_records:
        validate_required_fields(record)
        quality_checks(record)
        staged_records.append(stage_record(record))

    STAGED_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with STAGED_OUT_PATH.open("w") as f:
        for rec in staged_records:
            f.write(json.dumps(rec))
            f.write("\n")

    logger.info("Wrote %s staged driver health records", len(staged_records))
    logger.info("Driver health staging completed successfully")

# Entry point
if __name__ == "__main__":
    stage_driver_health()
