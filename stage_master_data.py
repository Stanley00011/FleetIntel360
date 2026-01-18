import json
import logging
from pathlib import Path
import pandas as pd
from simulator.common import DRIVERS_MAP, VEHICLES_MAP

# Config
STAGING_ROOT = Path("warehouse/staging")
DIM_DRIVERS_PATH = STAGING_ROOT / "dim_drivers.jsonl"
DIM_VEHICLES_PATH = STAGING_ROOT / "dim_vehicles.jsonl"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def stage_dimensions():
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    
    # 1. Process EVERY Driver (Active + Retired)
    logger.info("Staging Driver Dimensions from Master Config...")
    all_drivers = []
    for d_id, status in DRIVERS_MAP.items():
        all_drivers.append({
            "driver_id": d_id,
            "status": status,
            "is_active": (status == "ACTIVE")
        })
    
    with DIM_DRIVERS_PATH.open("w") as f:
        for d in all_drivers:
            f.write(json.dumps(d) + "\n")
    
    # 2. Process EVERY Vehicle (Active + Maintenance)
    logger.info("Staging Vehicle Dimensions from Master Config...")
    all_vehicles = []
    for v_id, status in VEHICLES_MAP.items():
        all_vehicles.append({
            "vehicle_id": v_id,
            "status": status,
            "is_active": (status == "ACTIVE")
        })
        
    with DIM_VEHICLES_PATH.open("w") as f:
        for v in all_vehicles:
            f.write(json.dumps(v) + "\n")

    logger.info(f"Successfully staged {len(all_drivers)} drivers and {len(all_vehicles)} vehicles.")

if __name__ == "__main__":
    stage_dimensions()