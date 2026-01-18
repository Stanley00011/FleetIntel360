#!/usr/bin/env python3
"""
FleetIntel360 - Batch Simulation Runner

Generates raw JSONL data for vehicles, driver health, and finance
into the warehouse/raw layer.

Design principles:
- Raw layer is append-only
- Dates are explicit and reproducible
- Staging is downstream (not automatic)
"""

import os
import json
import random
import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import List

from simulator.vehicle_sim import Vehicle
from simulator import driver_health_sim
from simulator import finance_sim
from simulator.common import DRIVERS, VEHICLES, DRIVERS_MAP, VEHICLES_MAP, utc_now_iso


# Config
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_ROOT = os.path.join(PROJECT_ROOT, "warehouse", "raw")

VEHICLES_OUT = os.path.join(RAW_ROOT, "vehicles")
HEALTH_OUT = os.path.join(RAW_ROOT, "driver_health")
FINANCE_OUT = os.path.join(RAW_ROOT, "finance")

DEFAULT_TELEMETRY_PER_DAY = 180


# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# Helpers
def ensure_dirs():
    for path in (VEHICLES_OUT, HEALTH_OUT, FINANCE_OUT):
        os.makedirs(path, exist_ok=True)


def write_jsonl(path: str, records: List[dict], overwrite: bool = False):
    mode = "w" if overwrite else "a"
    with open(path, mode, encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r))
            f.write("\n")


def make_vehicle_list():
    """
    Dynamically creates metadata for only ACTIVE vehicles.
    Pairs them with ACTIVE drivers from common.py.
    """
    active_vehicles = VEHICLES
    active_drivers = DRIVERS
    
    vehicles_meta = []
    for i, v_id in enumerate(active_vehicles):
        # Pair with an active driver (round-robin if counts differ)
        d_id = active_drivers[i % len(active_drivers)]
        
        vehicles_meta.append({
            "vehicle_id": v_id,
            "driver_id": d_id,
            "lat": 6.45 + (i * 0.005),
            "lon": 3.39 + (i * 0.005),
        })
    return vehicles_meta


# Generators
def generate_vehicle_snapshots(vehicles, samples, date):
    records = []
    for idx, meta in enumerate(vehicles):
        v = Vehicle(**meta)

        for s in range(samples):
            v.step(tick_seconds=60)
            v.inject_anomalies()
            payload = v.to_payload()

            ts = datetime(
                date.year, date.month, date.day, tzinfo=timezone.utc
            ) + timedelta(seconds=(12 * 3600 * s / samples))

            payload["timestamp"] = ts.isoformat().replace("+00:00", "Z")
            payload["_meta"] = {
                "generated_at": utc_now_iso(),
                "vehicle_index": idx,
                "sample_index": s,
            }

            records.append(payload)

    return records


def generate_health_events(drivers, date):
    """Fixed: Removed n_drivers argument to match new Simulator __init__"""
    sim = driver_health_sim.DriverHealthSimulator(
        tick=60,
        mode="stdout"
    )

    records = []
    for d in drivers:
        evt = sim.simulate_shift_event(d)
        evt["timestamp"] = (
            datetime(date.year, date.month, date.day, 10, tzinfo=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        evt["_meta"] = {"generated_at": utc_now_iso()}
        records.append(evt)

    return records


def generate_finance_events(drivers, date, trips_range):
    records = []
    for d in drivers:
        trips = random.randint(*trips_range)
        evt = finance_sim.generate_daily_finance(d, date, trips)
        evt["_meta"] = {"generated_at": utc_now_iso()}
        records.append(evt)
    return records


# Orchestrator
def run_batch(
    start_date,
    days,
    telemetry_per_day,
    overwrite
):
    ensure_dirs()

    # Uses the dynamic list (Buses + Cars)
    vehicles_meta = make_vehicle_list()

    for offset in range(days):
        date = start_date - timedelta(days=offset)
        logging.info(f"Generating data for {date}")

        # Vehicle Telemetry
        write_jsonl(
            os.path.join(VEHICLES_OUT, f"{date}.jsonl"),
            generate_vehicle_snapshots(vehicles_meta, telemetry_per_day, date),
            overwrite=overwrite,
        )

        # Health Events (Uses DRIVERS constant from common)
        write_jsonl(
            os.path.join(HEALTH_OUT, f"{date}.jsonl"),
            generate_health_events(DRIVERS, date),
            overwrite=overwrite,
        )

        # Finance Summaries
        write_jsonl(
            os.path.join(FINANCE_OUT, f"{date}.jsonl"),
            generate_finance_events(DRIVERS, date, (5, 15)),
            overwrite=overwrite,
        )

    logging.info(f"Batch run complete. Processed {len(vehicles_meta)} active vehicles.")

# CLI
def parse_args():
    p = argparse.ArgumentParser(description="FleetIntel360 batch simulator")
    p.add_argument(
        "--start-date", 
        default=str(datetime.now().date()), 
        help="YYYY-MM-DD (defaults to today)"
    )
    p.add_argument("--days", type=int, default=1)
    p.add_argument("--telemetry-per-day", type=int, default=180)
    p.add_argument("--overwrite", action="store_true")

    return p.parse_args()


def main():
    args = parse_args()

    # Convert the string date from CLI into a date object
    start_dt = datetime.fromisoformat(args.start_date).date()

    run_batch(
        start_date=start_dt,
        days=args.days,
        telemetry_per_day=args.telemetry_per_day,
        overwrite=args.overwrite,
    )

if __name__ == "__main__":
    main()