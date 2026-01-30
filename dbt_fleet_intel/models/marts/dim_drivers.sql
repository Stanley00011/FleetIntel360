SELECT 
    driver_id,
    driver_name,
    status,
    location_base, 
    CAST(joined_date AS DATE) as joined_date,
    -- Years of experience
    DATE_DIFF(CURRENT_DATE(), CAST(joined_date AS DATE), YEAR) as tenure_years,
    -- Added Months for better dashboard visuals
    DATE_DIFF(CURRENT_DATE(), CAST(joined_date AS DATE), MONTH) as tenure_months
FROM {{ ref('seed_drivers') }}

