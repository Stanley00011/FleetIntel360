-- dbt_fleet_intel/models/staging/stg_telemetry.sql

WITH source AS (
    SELECT * FROM `fleetintel360`.`fleet_intel_raw`.`raw_telemetry`
),

renamed AS (
    SELECT
        -- Extracting metadata
        publish_time AS ingested_at,
        message_id,

        -- Extracting from the JSON 'data' column
        JSON_VALUE(data, '$.event_id') AS event_id,
        JSON_VALUE(data, '$.vehicle_id') AS vehicle_id,
        JSON_VALUE(data, '$.driver_id') AS driver_id,
        
        -- Casting to correct data types
        CAST(JSON_VALUE(data, '$.lat') AS FLOAT64) AS latitude,
        CAST(JSON_VALUE(data, '$.lon') AS FLOAT64) AS longitude,
        CAST(JSON_VALUE(data, '$.speed_kph') AS FLOAT64) AS speed_kph,
        CAST(JSON_VALUE(data, '$.fuel_percent') AS FLOAT64) AS fuel_level,
        CAST(JSON_VALUE(data, '$.engine_temp_c') AS FLOAT64) AS engine_temp_c,
        
        -- Handling Booleans
        CAST(JSON_VALUE(data, '$.speeding') AS BOOL) AS is_speeding,
        
        -- Event Timestamp from the simulator
        TIMESTAMP(JSON_VALUE(data, '$.timestamp')) AS event_timestamp

    FROM source
)

SELECT * FROM renamed