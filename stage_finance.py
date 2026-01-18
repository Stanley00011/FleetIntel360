import json
import logging
from pathlib import Path
from typing import List, Dict

# Config

RAW_FINANCE_PATH = Path("warehouse/raw/finance")

STAGED_DAILY_PATH = Path("warehouse/staging/finance_daily_staged.jsonl")
STAGED_TRIPS_PATH = Path("warehouse/staging/finance_trips_staged.jsonl")

# Identity fields
REQUIRED_DAILY_FIELDS = {
    "event_id",
    "driver_id",
    "date",
}

REQUIRED_TRIP_FIELDS = {
    "event_id",
    "driver_id",
    "timestamp",
    "revenue",
    "total_cost",
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Loaders
def load_raw_finance_files() -> List[Dict]:
    """
    Load all finance JSONL files from raw layer.
    """
    records: List[Dict] = []

    if not RAW_FINANCE_PATH.exists():
        raise FileNotFoundError("Raw finance directory does not exist")

    files = sorted(RAW_FINANCE_PATH.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError("No raw finance files found")

    logger.info("Found %s raw finance files", len(files))

    for file in files:
        with file.open() as f:
            for line in f:
                records.append(json.loads(line))

    logger.info("Loaded %s raw finance records", len(records))
    return records

# Validation
def validate_required_fields(record: Dict, required: set) -> None:
    missing = required - record.keys()
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

def quality_checks_daily(record: Dict) -> None:
    if record["total_revenue"] < 0:
        raise ValueError("Negative total revenue")

    if record["total_cost"] < 0:
        raise ValueError("Negative total cost")

def quality_checks_trip(trip: Dict) -> None:
    if trip["revenue"] < 0:
        raise ValueError("Negative trip revenue")

    if trip["total_cost"] < 0:
        raise ValueError("Negative trip total cost")

# Staging transforms
def stage_daily_record(record: Dict) -> Dict:
    return {
        "event_id": record["event_id"],
        "driver_id": record["driver_id"],
        "date": record["date"],

        "total_revenue": record["total_revenue"],
        "total_cost": record["total_cost"],
        "net_profit": record["net_profit"],

        "fraud_alerts_count": record.get("fraud_alerts_count", 0),
        "trading_position": record.get("trading_position"),
        "end_of_day_balance": record.get("end_of_day_balance"),
    }

def stage_trip_record(daily_record: Dict, trip: Dict) -> Dict:
    return {
        "trip_event_id": trip["event_id"],
        "daily_event_id": daily_record["event_id"],
        "driver_id": trip["driver_id"],
        "timestamp": trip["timestamp"],

        "revenue": trip["revenue"],
        "fuel_cost": trip.get("fuel_cost"),
        "toll_fees": trip.get("toll_fees"),
        "maintenance_cost": trip.get("maintenance_cost"),
        "total_cost": trip["total_cost"],

        "fraud_alert": trip.get("fraud_alert", False),
    }

# Orchestrator
def stage_finance() -> None:
    logger.info("Starting finance staging")

    raw_records = load_raw_finance_files()

    staged_daily: List[Dict] = []
    staged_trips: List[Dict] = []

    for record in raw_records:
        validate_required_fields(record, REQUIRED_DAILY_FIELDS)
        quality_checks_daily(record)

        staged_daily.append(stage_daily_record(record))

        trips = record.get("trips", [])
        for trip in trips:
            validate_required_fields(trip, REQUIRED_TRIP_FIELDS)
            quality_checks_trip(trip)

            staged_trips.append(stage_trip_record(record, trip))

    STAGED_DAILY_PATH.parent.mkdir(parents=True, exist_ok=True)

    with STAGED_DAILY_PATH.open("w") as f:
        for rec in staged_daily:
            f.write(json.dumps(rec))
            f.write("\n")

    with STAGED_TRIPS_PATH.open("w") as f:
        for rec in staged_trips:
            f.write(json.dumps(rec))
            f.write("\n")

    logger.info("Wrote %s daily finance records", len(staged_daily))
    logger.info("Wrote %s finance trip records", len(staged_trips))
    logger.info("Finance staging completed successfully")

# Entry point
if __name__ == "__main__":
    stage_finance()
