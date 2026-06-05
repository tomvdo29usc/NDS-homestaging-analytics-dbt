WITH expanded AS (
  SELECT
    Order_ID,
    date,
    CASE 
      WHEN date <= PARSE_DATE('%m/%d/%Y', End_Date) AND date > Schedule_Staging_Date
      THEN SAFE_DIVIDE(
             Payment_Amount,
             DATE_DIFF(PARSE_DATE('%m/%d/%Y', End_Date), DATE(Schedule_Staging_Date), DAY) 
           )
      ELSE null
    END AS daily_revenue,

    CASE 
      WHEN date <= PARSE_DATE('%m/%d/%Y', Updated_End_Date) AND date > Schedule_Staging_Date
      THEN SAFE_DIVIDE(
             Payment_Amount,
             DATE_DIFF(PARSE_DATE('%m/%d/%Y', Updated_End_Date), DATE(Schedule_Staging_Date), DAY) 
           )
      ELSE null
    END AS daily_revenue_actual

  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}, 
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE('2026-01-01'),
      DATE('2026-12-31')
    )
  ) AS date
  WHERE Schedule_Staging_Date >= "2026-01-01"
    AND End_Date != "TBD"
)

,proposal_conversion_stats AS (
  SELECT
    CEIL(AVG(DATE_DIFF(DATE(Schedule_Staging_Date), DATE(Proposal_SentDate), DAY))) AS AvgDuration_Proposal_to_Staging,
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE Proposal_SentDate IS NOT NULL AND Schedule_Staging_Date IS NOT NULL
)

,prospective AS (
  SELECT
    Order_ID,
    Payment_Amount,
    Proposal_Duration,
    DATE_ADD(CURRENT_DATE(), INTERVAL CAST(CEIL((SELECT AvgDuration_Proposal_to_Staging FROM proposal_conversion_stats)) AS INT64) DAY) AS StagingDate,
    RANK() OVER (ORDER BY Payment_Amount DESC) AS rank_priority
  
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE Proposal_SentDate IS NOT NULL AND Proposal_ClosedDate IS NULL
  ORDER BY Payment_Amount DESC
)

,prospective_staging AS (
  SELECT
    Order_ID,
    Payment_Amount,
    DATE_ADD(StagingDate, INTERVAL rank_priority DAY) AS ProspectiveStaging_Date,
    DATE_ADD(DATE_ADD(StagingDate, INTERVAL Proposal_Duration DAY), INTERVAL rank_priority DAY) AS ProspectiveEnd_Date
  FROM prospective
)

,prospective_expanded AS (
  SELECT
    Order_ID,
    date,
    CASE 
      WHEN date <= ProspectiveEnd_Date AND date > ProspectiveStaging_Date
      THEN SAFE_DIVIDE(
             Payment_Amount,
             DATE_DIFF(ProspectiveEnd_Date, ProspectiveStaging_Date, DAY) 
           )
      ELSE null
    END AS daily_revenue
  FROM prospective_staging,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE('2026-01-01'),
      DATE('2026-12-31')
    )
  ) AS date
)
,daily_prospective AS (
  SELECT 
    date AS Full_Date,
    SUM(daily_revenue) AS total_prospective_daily_revenue
  FROM prospective_expanded
  WHERE daily_revenue IS NOT NULL
  GROUP BY date
)

,daily AS (
  SELECT 
    CASE WHEN date <= CURRENT_DATE() THEN "Earned" ELSE "Future" END AS Period,
    date AS Full_Date,
    SUM(daily_revenue) AS total_daily_revenue,
    SUM(daily_revenue_actual) AS total_daily_revenue_actual
  FROM expanded
     GROUP BY date
),

base AS (

SELECT
  period AS Period,
  daily.Full_Date,
  total_daily_revenue AS Total_Daily_Revenue,
  total_daily_revenue_actual AS Total_Daily_Revenue_Actual,
  coalesce(total_daily_revenue,0) + total_prospective_daily_revenue AS Total_Daily_Revenue_Ideal,
  CASE WHEN daily.Full_Date <= CURRENT_DATE() THEN Total_Daily_Revenue END AS Earned_Daily_Revenue,
  CASE WHEN daily.Full_Date > CURRENT_DATE() THEN Total_Daily_Revenue END AS Future_Daily_Revenue,
  total_prospective_daily_revenue AS Prospective_Daily_Revenue
FROM daily 
LEFT JOIN daily_prospective
ON daily.Full_Date = daily_prospective.Full_Date
ORDER BY period, Full_Date),

agg AS (
  SELECT *,
    SUM(Earned_Daily_Revenue) OVER () AS Total_Earned_Final,
    COALESCE(Total_Daily_Revenue_Ideal,Future_Daily_Revenue)AS total_daily_revenue_incl_prospective
  FROM base
),

final_output AS (
SELECT
  Period,
  Full_Date,
  Total_Daily_Revenue,
  Total_Daily_Revenue_Ideal,
  Earned_Daily_Revenue,
  Future_Daily_Revenue,
  Total_Daily_Revenue_Actual,
  Prospective_Daily_Revenue,

  -- Earned cumulative
  SUM(Earned_Daily_Revenue) OVER (
    ORDER BY Full_Date
  ) AS Cum_Earned,

  -- Future cumulative (continues from earned)
  Total_Earned_Final +
  SUM(Future_Daily_Revenue) OVER (
    ORDER BY Full_Date
  ) AS Cum_Future,

  -- Ideal cumulative
  Total_Earned_Final + SUM(total_daily_revenue_incl_prospective) OVER (
    ORDER BY Full_Date
  ) AS Cum_Ideal

FROM agg)

SELECT 
  Period,
  Full_Date,
  Total_Daily_Revenue,
  Total_Daily_Revenue_Actual,
  Total_Daily_Revenue_Ideal,
  Earned_Daily_Revenue,
  Future_Daily_Revenue,
  Prospective_Daily_Revenue,
  CASE WHEN Full_Date <= CURRENT_DATE() THEN Cum_Earned END AS Cum_Earned,
  Cum_Future,
  CASE WHEN Full_Date > DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY) THEN Cum_Ideal END AS Cum_Ideal
FROM final_output ORDER BY Full_Date
