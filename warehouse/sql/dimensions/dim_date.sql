
-- Dimension: Date
-- Grain: 1 row per calendar date

INSERT INTO mart.dim_date (
    date_key,
    year,
    month,
    day,
    week_of_year,
    day_of_week,
    is_weekend
)
SELECT
    d::DATE                                AS date_key,
    EXTRACT(YEAR FROM d)                   AS year,
    EXTRACT(MONTH FROM d)                  AS month,
    EXTRACT(DAY FROM d)                    AS day,
    EXTRACT(WEEK FROM d)                   AS week_of_year,
    STRFTIME(d, '%A')                      AS day_of_week,
    CASE
        WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                    AS is_weekend
FROM generate_series(
    DATE '2025-01-01',
    DATE '2027-12-31',
    INTERVAL 1 DAY
) t(d)
ON CONFLICT (date_key) DO NOTHING;
