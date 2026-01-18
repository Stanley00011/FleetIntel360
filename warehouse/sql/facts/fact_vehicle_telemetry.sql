-- FACT: Vehicle Telemetry
-- Grain: 1 row per telemetry event
-- Source: warehouse/staging/vehicles_staged.jsonl
-- Target: mart.fact_vehicle_telemetry

INSERT INTO mart.fact_vehicle_telemetry (
    event_id,
    vehicle_id,
    driver_id,
    event_timestamp,
    lat,
    lon,
    speed_kph,
    fuel_percent,
    engine_temp_c,
    battery_v,
    speeding,
    date_key
)
SELECT
    event_id,
    vehicle_id,
    driver_id,
    CAST(timestamp AS TIMESTAMP)          AS event_timestamp,
    lat,
    lon,
    speed_kph,
    fuel_percent,
    engine_temp_c,
    battery_v,
    speeding,
    CAST(timestamp AS DATE)               AS date_key
FROM read_json_auto(
    'warehouse/staging/vehicles_staged.jsonl'
)
WHERE event_id IS NOT NULL
  AND vehicle_id IS NOT NULL
  AND timestamp IS NOT NULL
ON CONFLICT (event_id) DO NOTHING;
