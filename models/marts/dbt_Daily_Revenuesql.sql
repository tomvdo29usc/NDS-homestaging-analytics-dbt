WITH expanded AS (
  SELECT
    Order_ID,
    date,
    CASE 
      WHEN date <= PARSE_DATE('%m/%d/%Y', End_Date)
      THEN SAFE_DIVIDE(
             Payment_Amount,
             DATE_DIFF(PARSE_DATE('%m/%d/%Y', End_Date), DATE(Schedule_Staging_Date), DAY) + 1
           )
      ELSE NULL
    END AS daily_revenue
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }},
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(Schedule_Staging_Date),
      DATE('2026-12-31')
    )
  ) AS date
  WHERE Schedule_Staging_Date IS NOT NULL
    AND End_Date != "TBD"
)
SELECT
  'Earned' AS period,
  date,
  SUM(daily_revenue) AS total_daily_revenue
FROM expanded
WHERE date BETWEEN DATE_TRUNC(CURRENT_DATE(), YEAR) AND DATE_SUB(CURRENT_DATE() + 1, INTERVAL 1 DAY)
GROUP BY date

UNION ALL

SELECT
  'Future' AS period,
  date,
  SUM(daily_revenue) AS total_daily_revenue
FROM expanded
WHERE date BETWEEN CURRENT_DATE() + 1 AND DATE_ADD(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY date
ORDER BY date