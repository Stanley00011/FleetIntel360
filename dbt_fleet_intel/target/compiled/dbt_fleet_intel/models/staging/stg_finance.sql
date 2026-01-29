WITH source AS (
    SELECT * FROM `fleetintel360`.`fleet_intel_raw`.`raw_finance`
),
renamed AS (
    SELECT
        JSON_VALUE(data, '$.event_id') AS event_id,
        JSON_VALUE(data, '$.driver_id') AS driver_id,
        CAST(JSON_VALUE(data, '$.total_revenue') AS FLOAT64) AS revenue,
        CAST(JSON_VALUE(data, '$.total_cost') AS FLOAT64) AS total_cost,
        CAST(JSON_VALUE(data, '$.net_profit') AS FLOAT64) AS net_profit,
        JSON_VALUE(data, '$.trading_position') AS trading_position,
        CAST(JSON_VALUE(data, '$.end_of_day_balance') AS FLOAT64) AS account_balance,
        -- Extracting date string and casting to DATE type
        CAST(JSON_VALUE(data, '$.date') AS DATE) AS business_date,
        publish_time AS ingested_at
    FROM source
)
SELECT * FROM renamed