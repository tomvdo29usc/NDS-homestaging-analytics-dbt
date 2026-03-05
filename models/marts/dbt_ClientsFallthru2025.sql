WITH filtered_paying_clients_2024 AS (
  SELECT 
    Client_Name
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE EXTRACT(YEAR FROM (Request_Submitted)) = 2024 AND Client_Name <> "Tom Do" AND Status  IN ('Active', 'Completed') 
),
filtered_paying_clients_2025 AS (
  SELECT 
    Client_Name
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE EXTRACT(YEAR FROM (Request_Submitted)) = 2025 AND Client_Name <> "Tom Do" AND Status  IN ('Active', 'Completed') 
),

filtered_fallthru_clients_2024 AS (
  SELECT 
    Client_Name
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE EXTRACT(YEAR FROM (Request_Submitted)) = 2024 AND Client_Name <> "Tom Do" AND Status  = 'Archived' AND Client_Name not in (SELECT Client_Name FROM filtered_paying_clients_2024)
),
filtered_fallthru_clients_2025 AS (
  SELECT 
    Client_Name
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE EXTRACT(YEAR FROM (Request_Submitted)) = 2025 AND Client_Name <> "Tom Do" AND Status  = 'Archived' AND Client_Name not in (SELECT Client_Name FROM filtered_paying_clients_2025)
)

SELECT 
  DISTINCT(Client_Name) AS client_fallthru_2025,
  CASE WHEN Client_Name IN (SELECT Client_Name FROM filtered_fallthru_clients_2024) THEN 'Has Fallthru 2024'
       WHEN Client_Name IN (SELECT Client_Name FROM filtered_paying_clients_2024) THEN 'Has Paid 2024' ELSE 'New Fallthru Client' END AS client_type,
  COUNT(DISTINCT Client_Name) OVER (PARTITION BY Client_Name) AS inquiry_count_2025,
  (SELECT COUNT(DISTINCT Client_Name) FROM filtered_fallthru_clients_2024) AS count_2024_fallthru_clients
FROM filtered_fallthru_clients_2025