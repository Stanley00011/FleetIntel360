-- FACT: Vehicle Daily Metrics (Incremental + Alerts)
-- Grain: 1 row per vehicle per day
-- Source: mart.fact_vehicle_telemetry
-- Purpose: Operational KPIs + risk signals per vehicle

CREATE OR REPLACE TEMP TABLE tmp_vehicle_dates AS
SELECT DISTINCT
    vehicle_id,
    date_key
FROM mart.fact_vehicle_telemetry
WHERE date_key > COALESCE(
    (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics),
    DATE '1900-01-01'
);

-- Remove existing rows for recomputed days
DELETE FROM mart.fact_vehicle_daily_metrics
WHERE (vehicle_id, date_key) IN (
    SELECT vehicle_id, date_key FROM tmp_vehicle_dates
);

-- Insert recomputed metrics + dynamic alerts
INSERT INTO mart.fact_vehicle_daily_metrics
WITH thresholds AS (
    -- Pivot thresholds into columns for easy calculation in the SELECT
    SELECT 
        MAX(CASE WHEN metric_name = 'engine_temp_c' THEN warning_threshold END) as temp_warn,
        MAX(CASE WHEN metric_name = 'battery_voltage' THEN warning_threshold END) as batt_warn,
        MAX(CASE WHEN metric_name = 'speeding_rate' THEN warning_threshold END) as speed_warn
    FROM mart.alert_thresholds
)
SELECT
    v.vehicle_id,
    v.date_key,

    -- Activity
    COUNT(t.event_id)                                           AS telemetry_events,

    -- Speed
    AVG(t.speed_kph)                                            AS avg_speed_kph,
    MAX(t.speed_kph)                                            AS max_speed_kph,

    -- Fuel & engine
    AVG(t.fuel_percent)                                         AS avg_fuel_percent,
    AVG(t.engine_temp_c)                                        AS avg_engine_temp_c,

    -- Electrical
    AVG(t.battery_v)                                            AS avg_battery_voltage,

    -- Risk metrics
    SUM(CASE WHEN t.speeding THEN 1 ELSE 0 END)                 AS speeding_events,

    ROUND(
        SUM(CASE WHEN t.speeding THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(t.event_id), 0),
        3
    )                                                           AS speeding_rate,

    -- DYNAMIC Alert logic (using values from the 'thresholds' CTE)
    (
        SUM(CASE WHEN t.speeding THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(t.event_id), 0)
    ) > (SELECT speed_warn FROM thresholds)                     AS speeding_alert,

    AVG(t.engine_temp_c) > (SELECT temp_warn FROM thresholds)   AS engine_temp_alert,
    AVG(t.battery_v) < (SELECT batt_warn FROM thresholds)       AS battery_alert

FROM tmp_vehicle_dates v
LEFT JOIN mart.fact_vehicle_telemetry t
    ON v.vehicle_id = t.vehicle_id
   AND v.date_key   = t.date_key
GROUP BY
    v.vehicle_id,
    v.date_key;