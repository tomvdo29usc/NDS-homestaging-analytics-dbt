WITH filtered_clients_2024 AS (
  SELECT 
    Client_Name
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE EXTRACT(YEAR FROM (Request_Submitted)) = 2024 AND Client_Name <> "Tom Do" AND Paid <> "Unpaid"
),
filtered_clients_2025 AS (
  SELECT 
    Client_Name
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE EXTRACT(YEAR FROM (Request_Submitted)) = 2025 AND Client_Name <> "Tom Do" AND Paid <> "Unpaid"
)

SELECT 
  DISTINCT(Client_Name) AS client_2025,
  CASE WHEN Client_Name IN (SELECT Client_Name FROM filtered_clients_2024) THEN 'Return Client' ELSE 'New Client' END AS client_type,
  (SELECT COUNT(DISTINCT Client_Name) FROM filtered_clients_2024) AS count_2024_clients
FROM filtered_clients_2025