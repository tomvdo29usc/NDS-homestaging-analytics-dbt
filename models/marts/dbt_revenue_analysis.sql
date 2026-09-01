SELECT
  Order_ID,
  Status,
  Client_Name,
  Property_Address,
  Payment_Amount,
  Schedule_Staging_Date,
  -- full_date,
  CASE WHEN Schedule_Staging_Date IS NOT NULL THEN Schedule_Staging_Date 
       WHEN Schedule_Staging_Date IS NULL AND Status = "Archived" THEN DATE(Proposal_ClosedDate) ELSE CURRENT_DATE() END AS date_revenue,
  CASE WHEN Schedule_Staging_Date IS NOT NULL THEN Payment_Amount END AS Earning_Amount, 
  CASE WHEN Schedule_Staging_Date IS NULL AND Status = "Inquiry" THEN Payment_Amount END AS OutstandingQuote_Amount,
  CASE WHEN Schedule_Staging_Date IS NULL AND Status = "Staging Scheduling" THEN Payment_Amount END AS PendingEarning_Amount,
  CASE WHEN Schedule_Staging_Date IS NULL AND Status = "Archived" THEN -Payment_Amount END AS Lost_Revenue 

FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
-- UNNEST(GENERATE_DATE_ARRAY(DATE('2026-01-01'),DATE('2026-12-31'))) AS full_date
WHERE Payment_Amount IS NOT NULL
ORDER BY date_revenue