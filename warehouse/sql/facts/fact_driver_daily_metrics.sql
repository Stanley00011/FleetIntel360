-- FACT: Driver Daily Metrics (Incremental + Alerts)
-- Grain: 1 row per driver per day
-- Sources:
--   mart.fact_vehicle_telemetry
--   mart.fact_driver_shifts
--   mart.fact_daily_finance
-- 1. Ensure the table exists first
CREATE TABLE IF NOT EXISTS mart.fact_driver_daily_metrics (
    driver_id                   VARCHAR,
    date_key                    DATE,
    total_events                INTEGER,
    avg_speed_kph               DOUBLE,
    speeding_events             INTEGER,
    total_shift_hours           DOUBLE,
    max_continuous_hours        DOUBLE,
    avg_fatigue_index           DOUBLE,
    alerts_count                INTEGER,
    total_revenue               DOUBLE,
    total_cost                  DOUBLE,
    net_profit                  DOUBLE,
    fraud_alerts_count          INTEGER,
    speeding_alert              BOOLEAN,
    fatigue_alert               BOOLEAN,
    fraud_alert                 BOOLEAN,
    PRIMARY KEY (driver_id, date_key)
);

-- 2. Identify incremental driver-date keys 
-- Subquery to check if the table is empty to avoid the Catalog Error
CREATE OR REPLACE TEMP TABLE tmp_driver_dates AS
WITH last_date AS (
    SELECT COALESCE(MAX(date_key), DATE '1900-01-01') as val 
    FROM mart.fact_driver_daily_metrics
)
SELECT DISTINCT driver_id, date_key FROM mart.fact_vehicle_telemetry WHERE date_key > (SELECT val FROM last_date)
UNION
SELECT DISTINCT driver_id, date_key FROM mart.fact_driver_shifts WHERE date_key > (SELECT val FROM last_date)
UNION
SELECT DISTINCT driver_id, date_key FROM mart.fact_daily_finance WHERE date_key > (SELECT val FROM last_date);

-- 3. Only attempt DELETE if there are actually dates to refresh
DELETE FROM mart.fact_driver_daily_metrics
WHERE (driver_id, date_key) IN (SELECT driver_id, date_key FROM tmp_driver_dates);

-- 4. Insert recomputed metrics using CTE approach
INSERT INTO mart.fact_driver_daily_metrics
WITH telemetry_agg AS (
    SELECT 
        driver_id, date_key,
        COUNT(event_id) AS total_events,
        AVG(speed_kph) AS avg_speed_kph,
        SUM(CASE WHEN speeding THEN 1 ELSE 0 END) AS speeding_events
    FROM mart.fact_vehicle_telemetry
    WHERE (driver_id, date_key) IN (SELECT driver_id, date_key FROM tmp_driver_dates)
    GROUP BY 1, 2
),
shift_agg AS (
    SELECT 
        driver_id, date_key,
        SUM(shift_hours) AS total_shift_hours,
        MAX(continuous_driving_hours) AS max_continuous_hours,
        AVG(fatigue_index) AS avg_fatigue_index,
        SUM(json_array_length(alerts)) AS alerts_count
    FROM mart.fact_driver_shifts
    WHERE (driver_id, date_key) IN (SELECT driver_id, date_key FROM tmp_driver_dates)
    GROUP BY 1, 2
),
finance_agg AS (
    SELECT 
        driver_id, date_key,
        SUM(total_revenue) AS total_revenue,
        SUM(total_cost) AS total_cost,
        SUM(net_profit) AS net_profit,
        SUM(fraud_alerts_count) AS fraud_alerts_count
    FROM mart.fact_daily_finance
    WHERE (driver_id, date_key) IN (SELECT driver_id, date_key FROM tmp_driver_dates)
    GROUP BY 1, 2
)
SELECT
    d.driver_id,
    d.date_key,
    COALESCE(t.total_events, 0),
    COALESCE(t.avg_speed_kph, 0),
    COALESCE(t.speeding_events, 0),
    COALESCE(s.total_shift_hours, 0),
    COALESCE(s.max_continuous_hours, 0),
    COALESCE(s.avg_fatigue_index, 0),
    COALESCE(s.alerts_count, 0),
    COALESCE(f.total_revenue, 0),
    COALESCE(f.total_cost, 0),
    COALESCE(f.net_profit, 0),
    COALESCE(f.fraud_alerts_count, 0),
    -- Speeding Alert
    (CAST(COALESCE(t.speeding_events, 0) AS DOUBLE) / NULLIF(t.total_events, 0)) > 0.10,
    -- Fatigue Alert 
    CASE 
        WHEN dim.status = 'RETIRED' THEN FALSE 
        ELSE (COALESCE(s.avg_fatigue_index, 0) > 0.7 OR COALESCE(s.max_continuous_hours, 0) > 8)
    END,
    -- Fraud Alert
    COALESCE(f.fraud_alerts_count, 0) > 0
FROM tmp_driver_dates d
-- ADD THIS JOIN 
JOIN mart.dim_driver dim ON d.driver_id = dim.driver_id 
LEFT JOIN telemetry_agg t ON d.driver_id = t.driver_id AND d.date_key = t.date_key
LEFT JOIN shift_agg s ON d.driver_id = s.driver_id AND d.date_key = s.date_key
LEFT JOIN finance_agg f ON d.driver_id = f.driver_id AND d.date_key = f.date_key;