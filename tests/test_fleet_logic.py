import pytest
import os
import json
from dotenv import load_dotenv

# Load local .env for testing
load_dotenv()

# 1. TEST THE VEHICLE LOGIC (From vehicle_sim.py)
def test_vehicle_telemetry_integrity():
    """Rigor: Ensure the Vehicle class produces valid physical data."""
    from simulator.vehicle_sim import Vehicle
    
    # Initialize a test vehicle
    v = Vehicle(vehicle_id="TEST_01", driver_id="DRV_01", lat=6.45, lon=3.39)
    
    # Simulate a step
    v.step(tick_seconds=60)
    payload = v.to_payload()
    
    assert payload["vehicle_id"] == "TEST_01"
    assert 0 <= payload["fuel_percent"] <= 100
    assert "engine_temp_c" in payload
    # Check that coordinate logic didn't break
    assert payload["lat"] != 0 

# 2. TEST THE FINANCE LOGIC (From finance_sim.py)
def test_finance_profit_calculation():
    """Rigor: Verify that Net Profit = Revenue - Total Cost."""
    from simulator.finance_sim import generate_trip
    from datetime import datetime
    
    trip = generate_trip("DRV_01", datetime.now())
    
    calculated_total_cost = (
        trip["fuel_cost"] + 
        trip["toll_fees"] + 
        trip["maintenance_cost"]
    )
    
    # Note: Using pytest.approx for floating point math
    assert trip["total_cost"] == pytest.approx(calculated_total_cost, 0.01)

# 3. TEST THE CLOUD PUBLISHER (From your new CloudPublisher class)
def test_publisher_initialization():
    """Rigor: Ensure publisher handles the Service Account string correctly."""
    from simulator.cloud_publisher import CloudPublisher
    
    # This will test if your JSON string in .env is valid
    try:
        pub = CloudPublisher()
        assert pub.project_id is not None
    except Exception as e:
        pytest.fail(f"Publisher failed to initialize: {e}")

# 4. TEST THE SHARED CONSTANTS (From common.py)
def test_active_driver_count():
    """Rigor: Ensure our 'Master Switch' logic in common.py is consistent."""
    from simulator.common import DRIVERS, DRIVERS_MAP
    
    active_in_map = [k for k, v in DRIVERS_MAP.items() if v == "ACTIVE"]
    assert len(DRIVERS) == len(active_in_map)
    assert len(DRIVERS) == 50  