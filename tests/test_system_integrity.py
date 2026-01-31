import pytest
import json
from simulator.vehicle_sim import Vehicle
from simulator.common import DRIVERS, VEHICLES

def test_driver_vehicle_alignment():
    """ Ensure every vehicle is assigned a valid, active driver."""
    from simulator.run_simulation import make_vehicle_list
    fleet = make_vehicle_list()
    
    for entry in fleet:
        assert entry['driver_id'] in DRIVERS, f"Invalid driver {entry['driver_id']} assigned!"
        assert entry['vehicle_id'] in VEHICLES, f"Invalid vehicle {entry['vehicle_id']} assigned!"

def test_json_serialization_safety():
    """ Ensure payloads contain NO non-serializable objects (like raw datetimes)."""
    v = Vehicle("BUS_01", "DR_001", 6.45, 3.39)
    payload = v.to_payload()
    
    try:
        # If this passes, the data is safe for PubSub/BigQuery ingestion
        json_dumps = json.dumps(payload)
        assert isinstance(json_dumps, str)
    except TypeError as e:
        pytest.fail(f"Payload contains non-serializable data: {e}")

def test_slack_block_structure():
    """ Verify Slack Alert formatting follows the Block Kit schema."""
    # import the LOGIC function, bypassing the BigQuery connection error
    from monitor_alerts import build_slack_payload
    
    alert_sample = {
        'severity': 'HIGH',
        'vehicle_id': 'BUS_01',
        'driver_id': 'DR_001',
        'alert_message': 'Engine Overheat',
        'alert_time': '2026-01-31'
    }
    
    payload = build_slack_payload(alert_sample)
    
    # Verify Slack Block Kit requirements
    assert "blocks" in payload
    block_types = [block["type"] for block in payload["blocks"]]
    assert "header" in block_types
    assert "section" in block_types
    assert "divider" in block_types