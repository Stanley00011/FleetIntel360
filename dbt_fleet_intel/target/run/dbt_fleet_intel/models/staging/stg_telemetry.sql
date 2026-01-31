

  create or replace view `fleetintel360`.`fleet_intel_staging`.`stg_telemetry`
  OPTIONS()
  as -- dbt_fleet_intel/models/staging/stg_telemetry.sql
WITH source AS (
    SELECT * FROM `fleetintel360`.`fleet_intel_raw`.`raw_telemetry`
),
renamed AS (
    SELECT
        publish_time AS ingested_at,
        message_id,
        JSON_VALUE(data, '$.vehicle_id') AS vehicle_id,
        JSON_VALUE(data, '$.driver_id') AS driver_id,
        CAST(JSON_VALUE(data, '$.lat') AS FLOAT64) AS latitude,
        CAST(JSON_VALUE(data, '$.lon') AS FLOAT64) AS longitude,
        CAST(JSON_VALUE(data, '$.speed_kph') AS FLOAT64) AS speed_kph,
        CAST(JSON_VALUE(data, '$.fuel_percent') AS FLOAT64) AS fuel_level,
        CAST(JSON_VALUE(data, '$.engine_temp_c') AS FLOAT64) AS engine_temp_c,
        CAST(JSON_VALUE(data, '$.speeding') AS BOOL) AS is_speeding,
        TIMESTAMP(JSON_VALUE(data, '$.timestamp')) AS event_timestamp,
        CAST(JSON_VALUE(data, '$.tire_psi.FL') AS FLOAT64) AS tire_psi_fl,
        CAST(JSON_VALUE(data, '$.tire_psi.FR') AS FLOAT64) AS tire_psi_fr,
        CAST(JSON_VALUE(data, '$.tire_psi.RL') AS FLOAT64) AS tire_psi_rl,
        CAST(JSON_VALUE(data, '$.tire_psi.RR') AS FLOAT64) AS tire_psi_rr

    FROM source
)
SELECT * FROM renamed;

