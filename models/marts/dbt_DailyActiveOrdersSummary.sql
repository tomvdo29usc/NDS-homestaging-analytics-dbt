WITH 
orders AS (
  SELECT
    Order_ID,
    DATE(Schedule_Staging_Date) AS staging_date,
    DATE(Schedule_Pickup_Date) AS pickup_date,
    Pickup_Complete,
    PARSE_DATE('%m/%d/%Y', Updated_End_Date) AS Updated_End_Date
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE Schedule_Staging_Date IS NOT NULL
),
calendar AS (
  SELECT day
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE '2025-01-01',# (SELECT MIN(Schedule_Staging_Date) FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}),  -- earliest staging date
    GREATEST(DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY), (SELECT MAX(staging_date) FROM orders), (SELECT MAX(pickup_date) FROM orders)),  -- until 7 days after today                                                    
    INTERVAL 1 DAY
  )) AS day
),

active_orders AS (
  SELECT
    c.day as day_mark,
    COUNT(o.Order_ID) AS active_orders_count
  FROM calendar c
  LEFT JOIN orders o
    ON o.staging_date <= c.day AND (
      (o.pickup_date IS NULL AND Pickup_Complete = TRUE AND Updated_End_Date > c.day) OR
      (o.pickup_date IS NOT NULL AND c.day < o.pickup_date) OR 
      (o.pickup_date IS NULL AND Pickup_Complete = FALSE))
    
  GROUP BY c.day
  ORDER BY c.day
),
unconverted AS (
  SELECT
    Order_ID,
    DATE(Request_Submitted) AS Request_Submitted
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE Status IN ("Inquiry", "Archived")
),

potential AS (
  SELECT
    day as day_mark,
    COUNT(Order_ID) AS count_potential_within_30days
  FROM calendar
  LEFT JOIN unconverted
  ON DATE_DIFF(day, Request_Submitted, DAY) BETWEEN 1 AND 30 #It takes on avg 14 days from inquiry to staging

    
  GROUP BY day
  ORDER BY day
)

SELECT 
  ao.day_mark,
  active_orders_count,
  count_potential_within_30days,
  active_orders_count+count_potential_within_30days as active_plus_potential
FROM potential
INNER JOIN active_orders ao
ON ao.day_mark = potential.day_mark
ORDER BY ao.day_mark