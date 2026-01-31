

  create or replace view `fleetintel360`.`fleet_intel_staging`.`stg_finance`
  OPTIONS()
  as -- dbt_fleet_intel/models/staging/stg_finance.sql
WITH source AS (
    SELECT * FROM `fleetintel360`.`fleet_intel_raw`.`raw_finance`
),
renamed AS (
    SELECT
        JSON_VALUE(data, '$.driver_id') AS driver_id,
        CAST(JSON_VALUE(data, '$.total_revenue') AS FLOAT64) AS revenue,
        CAST(JSON_VALUE(data, '$.total_cost') AS FLOAT64) AS total_cost,
        CAST(JSON_VALUE(data, '$.net_profit') AS FLOAT64) AS net_profit,
        CAST(JSON_VALUE(data, '$.date') AS DATE) AS business_date,
        publish_time AS ingested_at,
        
        -- Pull the fraud count from the simulator's JSON
        CAST(JSON_VALUE(data, '$.fraud_alerts_count') AS INT64) AS fraud_alerts_count
    FROM source
)
SELECT * FROM renamed;

