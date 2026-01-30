"""
common.py
---------
Shared utilities and constants for all simulator modules.

This file intentionally stays lightweight. It only contains:
- Cross-module constants (drivers, vehicles, finance categories)
- Reusable ID + timestamp helpers
- Small random utility functions

Nothing in this file contains business logic or simulation loops.
"""

import uuid
import random
from datetime import datetime, timedelta, timezone


#  SHARED ENTITY MAPS (The "Master Switch")

# 60 TOTAL DRIVERS: 50 Active, 10 Retired
DRIVERS_MAP = {f"DR_{i:03}": "ACTIVE" for i in range(1, 51)}
DRIVERS_MAP.update({f"DR_{i:03}": "RETIRED" for i in range(51, 61)})

# 60 TOTAL VEHICLES: 50 Active, 10 Maintenance
# 25 Buses + 25 Cars = 50 Active
VEHICLES_MAP = {f"BUS_{i:02}": "ACTIVE" for i in range(1, 26)}
VEHICLES_MAP.update({f"CAR_{i:02}": "ACTIVE" for i in range(1, 26)})
VEHICLES_MAP.update({f"VAN_{i:02}": "MAINTENANCE" for i in range(1, 11)})

DRIVERS = [k for k, v in DRIVERS_MAP.items() if v == "ACTIVE"]
VEHICLES = [k for k, v in VEHICLES_MAP.items() if v == "ACTIVE"]

# Used by vehicle_sim, driver_health_sim, and finance_sim
# These dynamically pull only the 'ACTIVE' ones for the simulation loop
DRIVERS = [k for k, v in DRIVERS_MAP.items() if v == "ACTIVE"]
VEHICLES = [k for k, v in VEHICLES_MAP.items() if v == "ACTIVE"]


#  FINANCE CONSTANTS
# These categories are intentionally generic and non-invasive.
# They represent realistic fleet-related financial metrics,
# without touching personal salary, employment contracts, etc.

FINANCE_CATEGORIES = {
    "trip_revenue",         # What the driver earned per completed trip
    "fuel_cost",            # Fuel consumed during the trip
    "toll_fees",            # Any tolls paid
    "maintenance_cost"      # Small amortized cost per trip
}


#  UTILITY FUNCTIONS
def generate_id(prefix: str) -> str:
    """
    Generate a short unique ID with a prefix.
    Example: generate_id("evt") -> "evt_8af21bd41f"
    """
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def utc_now_iso() -> str:
    """
    Return a UTC timestamp in ISO format with a 'Z' suffix.
    Example: '2025-12-11T15:45:30.123456Z'
    """
    return datetime.now(timezone.utc).isoformat() + "Z"


def random_timestamp(base: datetime = None, max_offset_seconds: int = 60) -> str:
    """
    Create a random timestamp near a base timestamp.

    Parameters:
        base (datetime): The reference time. If None, uses current UTC.
        max_offset_seconds (int): Maximum number of seconds to randomly add.

    Returns:
        str (ISO formatted UTC timestamp)
    """
    base = base or datetime.now(timezone.utc)
    offset = timedelta(seconds=random.randint(0, max_offset_seconds))
    return (base + offset).isoformat() + "Z"


def safe_rand_uniform(a: float, b: float) -> float:
    """
    A simple wrapper for random.uniform() that keeps values clean
    and rounded for nicer JSON output.
    """
    assert a <= b, "Lower bound must be <= upper bound"
    return round(random.uniform(a, b), 2)


def safe_rand_int(a: int, b: int) -> int:
    """
    Random integer helper with safer semantics.
    """
    return random.randint(a, b)
