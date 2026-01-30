"""
finance_sim.py
---------------
Simulates financial activity for each driver in the fleet.

Generates:
1. Per-trip financial events
2. Daily financial summaries
3. Optional fraud signals
4. Basic trading/position metadata (non-invasive)
5. Time-series suitable for a data warehouse

This module intentionally uses shared utilities from common.py
to keep IDs, timestamps, and driver lists consistent across the
entire FleetIntel360 simulator.
"""

import json
import random
from datetime import datetime, timedelta

from simulator.common import (
    DRIVERS,
    FINANCE_CATEGORIES,
    generate_id,
    utc_now_iso,
    safe_rand_uniform,
    safe_rand_int,
)


#   TRIP GENERATION
def generate_trip(driver_id: str, event_time: datetime) -> dict:
    """
    Simulate one trip/job for a driver.

    Returns a dictionary containing:
    - revenue (per trip)
    - fuel/toll/maintenance costs
    - fraud signal
    """

    # Core revenue + cost simulation
    revenue = safe_rand_uniform(10, 120)           # what the driver earns
    fuel_cost = safe_rand_uniform(2, 15)
    toll_fees = safe_rand_uniform(0, 7)
    maintenance_cost = safe_rand_uniform(0.5, 5)

    total_cost = round(fuel_cost + toll_fees + maintenance_cost, 2)

    # Slight chance of fraud/risk behavior
    fraud_alert = random.random() < 0.12           # ~12% probability

    return {
        "event_id": generate_id("trip"),
        "driver_id": driver_id,
        "timestamp": event_time.isoformat() + "Z",

        # Finance fields
        "revenue": revenue,
        "fuel_cost": fuel_cost,
        "toll_fees": toll_fees,
        "maintenance_cost": maintenance_cost,
        "total_cost": total_cost,

        # Risk signals
        "fraud_alert": fraud_alert,
    }

#   DAILY SUMMARY GENERATION
def generate_daily_finance(driver_id: str, date: datetime, num_trips: int) -> dict:
    """
    Generate all trips for a driver for a specific day and produce
    a financial summary.
    """

    trips = []
    base_time = datetime(date.year, date.month, date.day)

    # Generate each trip with realistic timestamps across the workday
    for i in range(num_trips):
        # Spread timestamps across 8–12 hour window
        seconds_into_day = random.randint(0, 12 * 3600)
        trip_time = base_time + timedelta(seconds=seconds_into_day)

        trips.append(generate_trip(driver_id, trip_time))

    # Aggregate metrics
    total_revenue = round(sum(t["revenue"] for t in trips), 2)
    total_cost = round(sum(t["total_cost"] for t in trips), 2)
    net_profit = round(total_revenue - total_cost, 2)
    fraud_alerts_count = sum(1 for t in trips if t["fraud_alert"])

    # Slightly financial-flavored but non-invasive
    trading_position = random.choice(["hold", "long_bias", "short_bias", "risk_off"])

    # End-of-day account balance simulation (for dashboarding)
    end_of_day_balance = safe_rand_uniform(200, 1500) + net_profit

    return {
        "event_id": generate_id("daily_finance"),
        "driver_id": driver_id,
        "date": date.isoformat(),

        # Daily aggregates
        "total_revenue": total_revenue,
        "total_cost": total_cost,
        "net_profit": net_profit,
        "fraud_alerts_count": fraud_alerts_count,
        "trading_position": trading_position,
        "end_of_day_balance": round(end_of_day_balance, 2),

        # Nested per-trip events
        "trips": trips,
    }


#   MAIN SIMULATION LOOP
def run_simulation(
    drivers=DRIVERS,
    days=7,
    trips_per_driver_range=(5, 15),
    output_mode="stdout"
):
    """
    Simulates N drivers over a number of days.

    Produces a list of all events suitable for:
    - Data warehouse ingestion
    - Kafka/MQ streaming alternative
    - Batch upload into analytics systems
    """

    start_date = datetime.utcnow().date()
    all_events = []

    for day_offset in range(days):
        current_date = start_date - timedelta(days=day_offset)

        for driver in drivers:
            num_trips = safe_rand_int(*trips_per_driver_range)
            daily_event = generate_daily_finance(driver, current_date, num_trips)

            all_events.append(daily_event)

            if output_mode == "stdout":
                print(json.dumps(daily_event, indent=2))

    return all_events

#   SCRIPT ENTRY POINT
if __name__ == "__main__":
    run_simulation(DRIVERS, days=1)
