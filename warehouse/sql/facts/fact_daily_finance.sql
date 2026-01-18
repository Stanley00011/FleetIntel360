-- FACT: Daily Finance
-- Grain: 1 row per driver per day
-- Source: staging.finance_daily_staged.jsonl
-- Target: mart.fact_daily_finance

INSERT INTO mart.fact_daily_finance (
    event_id,
    driver_id,
    date_key,
    total_revenue,
    total_cost,
    net_profit,
    fraud_alerts_count,
    trading_position,
    end_of_day_balance
)
SELECT
    event_id,
    driver_id,
    CAST(date AS DATE) AS date_key,
    total_revenue,
    total_cost,
    net_profit,                     -- already provided in source
    fraud_alerts_count,
    trading_position,
    end_of_day_balance
FROM read_json_auto(
    'warehouse/staging/finance_daily_staged.jsonl'
)
WHERE event_id IS NOT NULL
  AND driver_id IS NOT NULL
  AND date IS NOT NULL
ON CONFLICT (event_id) DO NOTHING;
