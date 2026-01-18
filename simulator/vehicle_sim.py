#!/usr/bin/env python3
#simulator/vehicle_sim.py
"""
simulator/vehicle_sim.py

Multi-vehicle telematics simulator.

Features:
- Simulates N vehicles producing telemetry every `tick` seconds.
- Movement: simple route-following (if route provided) or random walk.
- Injects realistic anomalies with configurable probabilities:
    - engine overheat spike
    - fuel siphon (sudden drop)
    - tyre leak (per wheel)
    - harsh braking
- Replay buffer per vehicle (last N samples) kept in memory for use by processors.
- Publishes JSON payloads to:
    - MQTT broker (default)
    - or STDOUT (mode=stdout) for demo without broker
- Configurable via command-line args or environment variables.
"""

import argparse
import json
import math
import random
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional
from simulator.common import safe_rand_int


try:
    import paho.mqtt.client as mqtt  # type: ignore
except Exception:
    mqtt = None  

# Configurable defaults
DEFAULT_BROKER = "localhost"
DEFAULT_TOPIC = "fleet/telemetry"
DEFAULT_VEHICLE_COUNT = 6
DEFAULT_TICK = 1.0  # seconds
REPLAY_BUFFER_SIZE = 30  # last N telemetry records to keep in memory

# anomaly probabilities per tick (tweakable)
ANOMALY_PROBS = {
    "overheat_spike": 0.001,     # 0.1% per tick
    "fuel_siphon": 0.0008,       # 0.08% per tick
    "tyre_leak": 0.0005,         # 0.05% per tick
    "harsh_brake": 0.002,        # 0.2% per tick
}

# realistic bounds
SPEED_MIN = 0
SPEED_MAX = 120  # kph

# Helper / Utility functions
def utc_iso_ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

# Vehicle model
class Vehicle:
    def __init__(self, vehicle_id: str, driver_id: str, lat: float, lon: float):
        self.vehicle_id = vehicle_id
        self.driver_id = driver_id
        self.lat = lat
        self.lon = lon
        self.speed_kph = random.uniform(20, 60)
        self.heading_deg = random.uniform(0, 360)
        self.engine_temp_c = random.uniform(75, 95)
        self.battery_v = random.uniform(12.0, 12.8)
        self.tire_psi = {"FL": random.uniform(30, 34),
                         "FR": random.uniform(30, 34),
                         "RL": random.uniform(30, 34),
                         "RR": random.uniform(30, 34)}
        self.fuel_percent = random.uniform(50, 100)
        self.replay = deque(maxlen=REPLAY_BUFFER_SIZE)
        # state flags to make anomalies persist a bit
        self._overheat_until = 0.0
        self._tyre_leak_until = {k: 0.0 for k in self.tire_psi.keys()}

    def step(self, tick_seconds: float = 1.0):
        """Advance the vehicle state by one tick."""
        # random small heading change
        self.heading_deg = (self.heading_deg + random.uniform(-3, 3)) % 360

        # movement distance (approx): kph -> degrees delta (very rough)
        # NOTE: this is just for visualization; it's not high-precision geo sim.
        distance_km = (self.speed_kph * tick_seconds) / 3600.0  # km moved this tick
        # convert to lat/lon degree deltas (approx): 1 deg lat ~ 111 km
        dlat = (distance_km / 111.0) * math.cos(math.radians(self.heading_deg))
        dlon = (distance_km / (111.0 * math.cos(math.radians(self.lat)) + 1e-6)) * math.sin(math.radians(self.heading_deg))
        self.lat += dlat
        self.lon += dlon

        # small speed drift
        self.speed_kph += random.uniform(-3, 3)
        self.speed_kph = clamp(self.speed_kph, SPEED_MIN, SPEED_MAX)

        # engine temp random walk (cooling if stopped)
        if self.speed_kph > 5:
            self.engine_temp_c += random.uniform(-0.2, 0.7)
        else:
            self.engine_temp_c += random.uniform(-0.5, 0.2)
        self.engine_temp_c = clamp(self.engine_temp_c, 60.0, 140.0)

        # battery slow drift
        self.battery_v += random.uniform(-0.01, 0.01)
        self.battery_v = clamp(self.battery_v, 11.0, 13.0)

        # fuel consumption depends on speed
        self.fuel_percent -= (self.speed_kph / 10000.0) * tick_seconds  # slow drain
        self.fuel_percent = clamp(self.fuel_percent, 0.0, 100.0)

        # tire slow leakage
        for k in self.tire_psi.keys():
            self.tire_psi[k] += random.uniform(-0.02, 0.02)
            self.tire_psi[k] = clamp(self.tire_psi[k], 18.0, 40.0)

    def inject_anomalies(self):
        """Randomly inject anomalies based on configured probabilities."""
        now = time.time()
        # Overheat spike (temporary high temp)
        if random.random() < ANOMALY_PROBS["overheat_spike"]:
            spike = random.uniform(12.0, 28.0)
            self.engine_temp_c += spike
            self._overheat_until = now + random.uniform(10, 60)  # lasts a bit
            # print debug
            # print(f"[ANOMALY] {self.vehicle_id} overheat spike +{spike:.1f}C")

        # Fuel siphon: sudden large drop
        if random.random() < ANOMALY_PROBS["fuel_siphon"]:
            drop = random.uniform(6.0, 22.0)
            self.fuel_percent = clamp(self.fuel_percent - drop, 0.0, 100.0)

        # Tyre leak on a random wheel
        if random.random() < ANOMALY_PROBS["tyre_leak"]:
            wheel = random.choice(list(self.tire_psi.keys()))
            leak_drop = random.uniform(3.0, 8.0)
            self.tire_psi[wheel] = clamp(self.tire_psi[wheel] - leak_drop, 10.0, 40.0)
            self._tyre_leak_until[wheel] = now + random.uniform(60, 3600)  # leak persists
            # print(f"[ANOMALY] {self.vehicle_id} tyre leak {wheel} -{leak_drop:.1f}psi")

        # Harsh braking event: sudden speed drop
        if random.random() < ANOMALY_PROBS["harsh_brake"]:
            drop_pct = random.uniform(0.2, 0.7)  # fraction of speed lost instantly
            prev_speed = self.speed_kph
            self.speed_kph = max(0.0, self.speed_kph * (1 - drop_pct))
            # embed a temporary "brake_force" marker in the next payload by manipulating heading a bit
            # print(f"[ANOMALY] {self.vehicle_id} harsh brake {prev_speed:.1f} -> {self.speed_kph:.1f} kph")

    def to_payload(self) -> Dict:
        """Construct JSON payload for this vehicle at current state."""
        payload = {
            "event_id": f"evt_{uuid.uuid4().hex[:10]}",
            "vehicle_id": self.vehicle_id,
            "driver_id": self.driver_id,
            "timestamp": utc_iso_ts(),
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed_kph": round(self.speed_kph, 2),
            "heading": round(self.heading_deg, 2),
            "engine_temp_c": round(self.engine_temp_c, 2),
            "battery_v": round(self.battery_v, 2),
            "tire_psi": {k: round(v, 2) for k, v in self.tire_psi.items()},
            "fuel_percent": round(self.fuel_percent, 2),
            # a simple derived field (speed zone not implemented here; consumer can enrich with polygons)
            "speed_zone_kph": 50,
            "speeding": self.speed_kph > 50,
            "obd_codes": []  # simulated OBD codes can be added by processor or more complex sim
        }
        return payload

# Simulator Engine
class Simulator:
    def __init__(self, n_vehicles: int, broker: str, topic: str, mode: str, tick: float, start_lat: float = 6.45, start_lon: float = 3.39):
        self.n = n_vehicles
        self.broker = broker
        self.topic = topic
        self.mode = mode  # 'mqtt' or 'stdout'
        self.tick = tick
        self.vehicles: List[Vehicle] = []
        self.threads: List[threading.Thread] = []
        self.stop_event = threading.Event()
        self._init_vehicles(start_lat, start_lon)
        self.mqtt_client = None
        if self.mode == "mqtt":
            if mqtt is None:
                raise RuntimeError("paho-mqtt is not installed but mode=mqtt selected. pip install paho-mqtt")
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.connect(self.broker, 1883, 60)
            # run network loop in a background thread
            self.mqtt_client.loop_start()

def _init_vehicles(self, start_lat: float, start_lon: float):
    from simulator.common import VEHICLES, DRIVERS
    
    # Pair active vehicles with active drivers
    for i, v_id in enumerate(VEHICLES):
        # Safety: Does not go out of index if more vehicles than drivers
        d_id = DRIVERS[i % len(DRIVERS)] 
        
        lat = start_lat + (i * 0.0012)
        lon = start_lon + (i * 0.0015)
        v = Vehicle(vehicle_id=v_id, driver_id=d_id, lat=lat, lon=lon)
        self.vehicles.append(v)

    def _publish(self, payload: Dict):
        if self.mode == "mqtt":
            self.mqtt_client.publish(self.topic, json.dumps(payload))
        elif self.mode == "stdout":
            print(json.dumps(payload, default=str), flush=True)
        else:
            # future: implement pubsub/http
            print(json.dumps(payload, default=str), flush=True)

    def _vehicle_loop(self, vehicle: Vehicle):
        """Loop for single vehicle."""
        while not self.stop_event.is_set():
            # step state
            vehicle.step(tick_seconds=self.tick)
            # maybe inject anomalies
            vehicle.inject_anomalies()

            # create payload
            payload = vehicle.to_payload()

            # attach a "brake_force" synthetic field when immediate large decel from previous replay item
            # compare last replay if exists
            if vehicle.replay:
                last = vehicle.replay[-1]
                prev_speed = last.get("speed_kph", vehicle.speed_kph)
                # compute decel
                if prev_speed - payload["speed_kph"] > max(8.0, prev_speed * 0.25):
                    payload["harsh_brake"] = True
                else:
                    payload["harsh_brake"] = False
            else:
                payload["harsh_brake"] = False

            # append to replay buffer
            vehicle.replay.append(payload)

            # publish
            self._publish(payload)

            # tiny random jitter so not all vehicles align perfectly
            time.sleep(self.tick + random.uniform(-0.2 * self.tick, 0.2 * self.tick))

    def start(self):
        for v in self.vehicles:
            t = threading.Thread(target=self._vehicle_loop, args=(v,), daemon=True)
            self.threads.append(t)
            t.start()
        print(f"[SIM] started {len(self.vehicles)} vehicle threads. mode={self.mode}, tick={self.tick}s, topic={self.topic}")

    def stop(self):
        self.stop_event.set()
        # allow threads to exit
        for t in self.threads:
            t.join(timeout=1.0)
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        print("[SIM] stopped simulator.")

# CLI
def parse_args():
    p = argparse.ArgumentParser(description="Multi-vehicle telematics simulator")
    p.add_argument("--vehicles", "-n", type=int, default=DEFAULT_VEHICLE_COUNT, help="number of vehicles to simulate")
    p.add_argument("--broker", type=str, default=DEFAULT_BROKER, help="MQTT broker host (ignored in stdout mode)")
    p.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="MQTT topic to publish telemetry")
    p.add_argument("--mode", type=str, default="mqtt", choices=["mqtt", "stdout"], help="publish mode: mqtt or stdout")
    p.add_argument("--tick", type=float, default=DEFAULT_TICK, help="seconds between telemetry ticks (per vehicle)")
    p.add_argument("--start-lat", type=float, default=6.45, help="starting latitude for first vehicle")
    p.add_argument("--start-lon", type=float, default=3.39, help="starting longitude for first vehicle")
    return p.parse_args()

def main():
    args = parse_args()
    sim = Simulator(n_vehicles=args.vehicles, broker=args.broker, topic=args.topic, mode=args.mode, tick=args.tick,
                    start_lat=args.start_lat, start_lon=args.start_lon)
    try:
        sim.start()
        # run until interrupted
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[SIM] KeyboardInterrupt received. Shutting down...")
    finally:
        sim.stop()

if __name__ == "__main__":
    main()
