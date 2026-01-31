

  create or replace view `fleetintel360`.`fleet_intel_staging`.`stg_health`
  OPTIONS()
  as WITH source AS (
    SELECT * FROM `fleetintel360`.`fleet_intel_raw`.`raw_health`
),
renamed AS (
    SELECT
        JSON_VALUE(data, '$.event_id') AS event_id,
        JSON_VALUE(data, '$.driver_id') AS driver_id,
        CAST(JSON_VALUE(data, '$.shift_hours') AS FLOAT64) AS shift_hours,
        CAST(JSON_VALUE(data, '$.continuous_driving_hours') AS FLOAT64) AS continuous_hours,
        CAST(JSON_VALUE(data, '$.fatigue_index') AS FLOAT64) AS fatigue_index,
        CAST(JSON_VALUE(data, '$.breaks_taken') AS BOOL) AS took_breaks,
        -- Alerts is an array, so it's kept as JSON for now or flattened later
        JSON_QUERY(data, '$.alerts') AS alerts,
        TIMESTAMP(JSON_VALUE(data, '$.timestamp')) AS event_timestamp,
        publish_time AS ingested_at
    FROM source
)
SELECT * FROM renamed;

