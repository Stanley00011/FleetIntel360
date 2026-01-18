# simulator/driver_health_sim.py

import json
import random
import uuid
from datetime import datetime, timedelta
from simulator.common import generate_id, utc_now_iso, DRIVERS
import time
import argparse

class DriverHealthSimulator:
    def __init__(self, tick=5, mode="stdout", broker=None, topic=None):
        # Use the list from common.py
        self.tick = tick
        self.mode = mode
        self.broker = broker
        self.topic = topic
        
        self.drivers = DRIVERS 

        if self.mode == "mqtt":
            try:
                import paho.mqtt.client as mqtt  # type: ignore
            except Exception:
                # if paho-mqtt is not available; fall back to stdout and warn
                print("Warning: paho-mqtt package not found, falling back to stdout mode.")
                self.mode = "stdout"
                self.mqtt_client = None
            else:
                self.mqtt_client = mqtt.Client()
                try:
                    self.mqtt_client.connect(self.broker, 1883, 60)
                except Exception:
                    # connection failed; keep client but warn
                    print("Warning: could not connect to MQTT broker; continuing with mqtt client object.")

    def simulate_shift_event(self, driver_id):
        # Simulate shift hours: 6–10
        shift_hours = round(random.uniform(6, 10), 1)
        # Continuous driving: 2–6 hours, cannot exceed shift
        continuous_hours = round(random.uniform(2, min(6, shift_hours)), 1)
        # Fatigue index: derived from continuous hours
        fatigue_index = round(min(1.0, continuous_hours / 6 + random.uniform(-0.1, 0.1)), 2)
        # Breaks taken if fatigue index is below threshold
        breaks_taken = fatigue_index < 0.6
        # Alerts
        alerts = []
        if fatigue_index >= 0.6:
            alerts.append("fatigue_risk")

        event = {
            "event_id": f"health_{uuid.uuid4().hex[:8]}",
            "driver_id": driver_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "shift_hours": shift_hours,
            "continuous_driving_hours": continuous_hours,
            "fatigue_index": fatigue_index,
            "breaks_taken": breaks_taken,
            "alerts": alerts
        }
        return event

    def run(self):
        while True:
            for driver in self.drivers:
                event = self.simulate_shift_event(driver)
                if self.mode == "stdout":
                    print(json.dumps(event))
                elif self.mode == "mqtt":
                    self.mqtt_client.publish(self.topic, json.dumps(event))
            time.sleep(self.tick)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--drivers", type=int, default=6, help="Number of drivers to simulate")
    parser.add_argument("--tick", type=int, default=5, help="Tick interval in seconds")
    parser.add_argument("--mode", type=str, default="stdout", help="Output mode: stdout or mqtt")
    parser.add_argument("--broker", type=str, default="localhost", help="MQTT broker address")
    parser.add_argument("--topic", type=str, default="fleet/health", help="MQTT topic")
    args = parser.parse_args()

    sim = DriverHealthSimulator(
        n_drivers=args.drivers,
        tick=args.tick,
        mode=args.mode,
        broker=args.broker,
        topic=args.topic
    )
    sim.run()
