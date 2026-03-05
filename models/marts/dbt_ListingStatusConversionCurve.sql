WITH duration_data AS (
  SELECT
    Order_ID,
    DATE_DIFF(Listing_First_Active, Schedule_Staging_Date, DAY) AS Duration_Staged_to_Listed,
    DATE_DIFF(Listing_First_Contingent, GREATEST(Listing_First_Active, Schedule_Staging_Date), DAY) AS Duration_to_Contingent,
    DATE_DIFF(Listing_First_Pending, GREATEST(Listing_First_Active, Schedule_Staging_Date), DAY) AS Duration_to_Pending,
    DATE_DIFF(Listing_Sold, GREATEST(Listing_First_Active, Schedule_Staging_Date), DAY) AS Duration_to_Closed

  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE Request_Submitted >= '2024-01-01' AND 
    DATE_DIFF(Listing_First_Active, Schedule_Staging_Date, DAY) <= 45 AND
    Listing_First_Active IS NOT NULL AND (Unknown_Outcome IS NULL OR (Unknown_Outcome IS NOT NULL AND Listing_First_Contingent IS NOT NULL))
),

num_days AS (
  SELECT day
  FROM UNNEST(GENERATE_ARRAY(0, 59, 1)) AS day   -- 0 to 21 by 1
  UNION ALL
  SELECT day
  FROM UNNEST(GENERATE_ARRAY(60, 119, 5)) AS day -- 22 to 180 by 7
  UNION ALL
  SELECT day
  FROM UNNEST(GENERATE_ARRAY(120, 360, 10)) AS day -- 22 to 180 by 7
),

bucketed AS (
  SELECT
    n.day,
    COUNTIF(d.Duration_to_Contingent <= n.day) AS num_orders_contingent,
    COUNTIF(d.Duration_to_Pending <= n.day) AS num_orders_pending,
    COUNTIF(d.Duration_to_Closed <= n.day) AS num_orders_closed
  FROM num_days n
  CROSS JOIN duration_data d
  GROUP BY n.day
  ORDER BY n.day
),

num_orders AS (
  SELECT
    countif(Listing_First_Contingent IS NOT NULL OR 
      (Listing_First_Contingent IS NULL AND
        DATE_DIFF(CURRENT_DATE("America/New_York"), SAFE.PARSE_DATE('%m/%d/%Y', Updated_End_Date), DAY) > 0)) AS num_contingent


  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE Request_Submitted >= '2024-01-01' AND 
    DATE_DIFF(Listing_First_Active, Schedule_Staging_Date, DAY) <= 45 AND
    Listing_First_Active IS NOT NULL AND (Unknown_Outcome IS NULL OR (Unknown_Outcome IS NOT NULL AND Listing_First_Contingent IS NOT NULL))

)

SELECT
  day,
  num_orders_contingent / (SELECT num_contingent FROM num_orders) AS pct_contingent,
  num_orders_pending / (SELECT num_contingent FROM num_orders) AS pct_pending,
  num_orders_closed / (SELECT num_contingent FROM num_orders) AS pct_closed,
  (SELECT num_contingent FROM num_orders) AS num_orders
FROM bucketed
ORDER BY day