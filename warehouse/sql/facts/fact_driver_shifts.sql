-- FACT: Driver Shifts / Health
-- Grain: 1 row per driver health event
-- Source: warehouse/staging/driver_health.jsonl
-- Target: mart.fact_driver_shifts

INSERT INTO mart.fact_driver_shifts (
    event_id,
    driver_id,
    event_timestamp,
    shift_hours,
    continuous_driving_hours,
    fatigue_index,
    breaks_taken,
    alerts,
    date_key
)
SELECT
    event_id,
    driver_id,
    CAST(timestamp AS TIMESTAMP)                 AS event_timestamp,
    shift_hours,
    continuous_driving_hours,
    fatigue_index,
    breaks_taken,
    alerts,
    CAST(timestamp AS DATE)                      AS date_key
FROM read_json_auto(
    'warehouse/staging/driver_health_staged.jsonl' 
)
WHERE event_id IS NOT NULL
  AND driver_id IS NOT NULL
  AND timestamp IS NOT NULL
ON CONFLICT (event_id) DO NOTHING;
