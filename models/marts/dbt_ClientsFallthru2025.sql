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

fallthru_2024_count AS (
    SELECT COUNT(DISTINCT Client_Name) AS count_2024_fallthru_clients
    FROM filtered_fallthru_clients_2024
),
filtered_fallthru_clients_2025 AS (
  SELECT 
    Client_Name
  FROM {{ ref('dbt_StagingOrderswithListingStatuses') }}
  WHERE EXTRACT(YEAR FROM (Request_Submitted)) = 2025 AND Client_Name <> "Tom Do" AND Status  = 'Archived' AND Client_Name not in (SELECT Client_Name FROM filtered_paying_clients_2025)
)

SELECT 
        f2025.Client_Name AS client_fallthru_2025,
        CASE 
            WHEN f2024.Client_Name IS NOT NULL THEN 'Has Fallthru 2024'
            WHEN paid2024.Client_Name IS NOT NULL THEN 'Has Paid 2024'
            ELSE 'New Fallthru Client'
        END AS client_type,
        COUNT(DISTINCT f2025.Client_Name) OVER (PARTITION BY f2025.Client_Name) AS inquiry_count_2025,
        c.count_2024_fallthru_clients AS count_2024_fallthru_clients
    FROM filtered_fallthru_clients_2025 f2025
    LEFT JOIN filtered_fallthru_clients_2024 f2024
        ON f2025.Client_Name = f2024.Client_Name
    LEFT JOIN filtered_paying_clients_2024 paid2024
        ON f2025.Client_Name = paid2024.Client_Name
    CROSS JOIN fallthru_2024_count c
