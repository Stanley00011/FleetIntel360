
-- Analytics Warehouse Schema
-- Engine: DuckDB (Local → Cloud portable)

PRAGMA enable_object_cache;

-- SCHEMAS

-- CREATE TABLE statements 

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- DIMENSIONS

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key        DATE PRIMARY KEY,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    week_of_year    INTEGER,
    day_of_week     VARCHAR,
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS mart.dim_driver (
    driver_id       VARCHAR PRIMARY KEY,
    status          VARCHAR DEFAULT 'ACTIVE', 
    first_seen_at   TIMESTAMP,
    last_seen_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mart.dim_vehicle (
    vehicle_id      VARCHAR PRIMARY KEY,
    vehicle_type    VARCHAR,                  
    status          VARCHAR DEFAULT 'ACTIVE', 
    first_seen_at   TIMESTAMP,
    last_seen_at    TIMESTAMP
);

-- FACT TABLES

CREATE TABLE IF NOT EXISTS mart.fact_vehicle_telemetry (
    event_id        VARCHAR PRIMARY KEY,
    vehicle_id      VARCHAR,
    driver_id       VARCHAR,
    event_timestamp TIMESTAMP,
    lat             DOUBLE,
    lon             DOUBLE,
    speed_kph       DOUBLE,
    fuel_percent    DOUBLE,
    engine_temp_c   DOUBLE,
    battery_v       DOUBLE,
    speeding        BOOLEAN,
    date_key        DATE
);

CREATE TABLE IF NOT EXISTS mart.fact_driver_shifts (
    event_id                    VARCHAR PRIMARY KEY,
    driver_id                   VARCHAR,
    event_timestamp             TIMESTAMP,
    shift_hours                 DOUBLE,
    continuous_driving_hours    DOUBLE,
    fatigue_index               DOUBLE,
    breaks_taken                BOOLEAN,
    alerts                      JSON,
    date_key                    DATE
);

CREATE TABLE IF NOT EXISTS mart.fact_daily_finance (
    event_id                VARCHAR PRIMARY KEY,
    driver_id               VARCHAR,
    date_key                DATE,
    total_revenue           DOUBLE,
    total_cost              DOUBLE,
    net_profit              DOUBLE,
    fraud_alerts_count      INTEGER,
    trading_position        VARCHAR,
    end_of_day_balance      DOUBLE
);

CREATE TABLE IF NOT EXISTS mart.alert_thresholds (
    metric_name        VARCHAR PRIMARY KEY,
    warning_threshold  DOUBLE,
    critical_threshold DOUBLE,
    comparison_op      VARCHAR, -- '>', '<'
    description        VARCHAR,
    is_active          BOOLEAN
);

CREATE TABLE IF NOT EXISTS mart.alerts (
    alert_name TEXT,
    entity_id TEXT,
    entity_type TEXT,
    metric_name TEXT,
    metric_value DOUBLE,
    severity TEXT,
    alert_time TIMESTAMP,
    description TEXT
);
