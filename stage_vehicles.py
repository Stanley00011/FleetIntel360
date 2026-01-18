import json
import logging
from pathlib import Path
from typing import List, Dict

# Config
RAW_VEHICLES_PATH = Path("warehouse/raw/vehicles")
STAGED_OUT_PATH = Path("warehouse/staging/vehicles_staged.jsonl")

# Identity + location only (hard requirements)
REQUIRED_FIELDS = {
    "event_id",
    "vehicle_id",
    "driver_id",
    "timestamp",
    "lat",
    "lon",
}

# Optional telemetry fields expected but do not hard-fail on
OPTIONAL_FIELDS = {
    "speed_kph",
    "engine_temp_c",
    "battery_v",
    "fuel_percent",
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# Loaders
def load_raw_vehicle_files() -> List[Dict]:
    """
    Load all vehicle JSONL files from raw layer.
    Raw layer is immutable and untrusted.
    """
    records: List[Dict] = []

    files = sorted(RAW_VEHICLES_PATH.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError("No raw vehicle files found")

    logger.info("Found %s raw vehicle files", len(files))

    for file in files:
        with file.open() as f:
            for line in f:
                records.append(json.loads(line))

    logger.info("Loaded %s raw vehicle records", len(records))
    return records


# Normalization
def normalize_record(record: Dict) -> Dict:
    """
    Normalize raw telemetry into staging contract.
    No data loss. No business logic.
    """
    normalized = record.copy()

    # Explicit schema contract
    # (raw uses fuel_percent — keep it but standardize access)
    normalized["fuel_percent"] = record.get("fuel_percent")

    return normalized


# Validation
def validate_required_fields(record: Dict) -> None:
    """
    Fail only if identity fields are missing.
    """
    missing = REQUIRED_FIELDS - record.keys()
    if missing:
        raise ValueError(f"Missing required identity fields: {missing}")


def quality_checks(record: Dict) -> None:
    """
    Hard safety checks only.
    Telemetry can be null, but not physically impossible.
    """

    if not (-90 <= record["lat"] <= 90):
        raise ValueError("Invalid latitude")

    if not (-180 <= record["lon"] <= 180):
        raise ValueError("Invalid longitude")

    speed = record.get("speed_kph")
    if speed is not None and speed < 0:
        raise ValueError("Negative speed")

    fuel = record.get("fuel_percent")
    if fuel is not None and not (0 <= fuel <= 100):
        raise ValueError("Invalid fuel percent")


# Staging Transform
def stage_record(record: Dict) -> Dict:
    """
    Minimal, query-friendly staging shape.
    """
    return {
        "event_id": record["event_id"],
        "vehicle_id": record["vehicle_id"],
        "driver_id": record["driver_id"],
        "timestamp": record["timestamp"],
        "lat": round(record["lat"], 6),
        "lon": round(record["lon"], 6),

        # Telemetry (nullable by design)
        "speed_kph": record.get("speed_kph"),
        "fuel_percent": record.get("fuel_percent"),
        "engine_temp_c": record.get("engine_temp_c"),
        "battery_v": record.get("battery_v"),

        # Metadata
        "speeding": record.get("speeding", False),
    }


# Orchestrator
def stage_vehicles() -> None:
    logger.info("Starting vehicle staging")

    raw_records = load_raw_vehicle_files()
    staged_records: List[Dict] = []

    for record in raw_records:
        record = normalize_record(record)

        validate_required_fields(record)
        quality_checks(record)

        staged_records.append(stage_record(record))

    STAGED_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with STAGED_OUT_PATH.open("w") as f:
        for rec in staged_records:
            f.write(json.dumps(rec))
            f.write("\n")

    logger.info("Wrote %s staged vehicle records", len(staged_records))
    logger.info("Vehicle staging completed successfully")


# Entry point
if __name__ == "__main__":
    stage_vehicles()
