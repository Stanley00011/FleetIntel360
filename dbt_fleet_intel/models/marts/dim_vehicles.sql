SELECT 
    vehicle_id,
    vehicle_type,
    status as operational_status,
    CAST(last_service_date AS DATE) as last_service_date,
    -- Business Logic: Days since last service
    DATE_DIFF(CURRENT_DATE(), CAST(last_service_date AS DATE), DAY) as days_since_service
FROM {{ ref('seed_vehicles') }}