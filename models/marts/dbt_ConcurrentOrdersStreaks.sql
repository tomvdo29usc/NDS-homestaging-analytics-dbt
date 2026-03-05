WITH calendar AS (
  SELECT day
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE '2025-01-01',
    DATE '2025-12-31',
    INTERVAL 1 DAY
  )) AS day
),

orders AS (
  SELECT
    Order_ID,
    DATE(Schedule_Staging_Date) AS staging_date,
    DATE(Schedule_Pickup_Date) AS pickup_date
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE Schedule_Staging_Date IS NOT NULL
),

active_orders AS (
  SELECT
    c.day,
    COUNT(o.Order_ID) AS concurrent_orders
  FROM calendar c
  LEFT JOIN orders o
    ON o.staging_date <= c.day
   AND (o.pickup_date IS NULL OR c.day < o.pickup_date)
  GROUP BY c.day
), 

streaks AS (
  SELECT
    day,
    concurrent_orders,
    DATE_SUB(
      day,
      INTERVAL ROW_NUMBER() OVER (
        PARTITION BY concurrent_orders
        ORDER BY day
      ) DAY
    ) AS streak_group
  FROM active_orders
)

SELECT
  concurrent_orders AS number_of_concurrent_orders,
  MIN(day) AS start_date,
  MAX(day) AS end_date,
  COUNT(*) AS consecutive_duration_days
FROM streaks
GROUP BY concurrent_orders, streak_group
ORDER BY start_date