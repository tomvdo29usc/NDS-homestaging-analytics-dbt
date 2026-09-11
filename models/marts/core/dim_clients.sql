{{ config(
    materialized='table',
    indexes=[
        {'columns': ['Client_ID'], 'type': 'hash'},
        {'columns': ['Client_Phone'], 'type': 'hash'}
    ]
) }}

WITH distinct_clients AS (
  SELECT DISTINCT
    Client_ID,
    TRIM(Client_Name) AS Client_Name,
    TRIM(Client_Email) AS Client_Email,
    TRIM(Client_Phone) AS Client_Phone
  FROM {{ ref('stg_orders') }})

SELECT 
  Client_ID,
  Client_Phone,

  ARRAY_AGG(Client_Name 
            ORDER BY LENGTH(Client_Name), Client_Name
            LIMIT 1)[OFFSET(0)] AS Client_Name,

  STRING_AGG(Client_Email, ', ') AS Client_Emails,
  STRING_AGG(Client_Name, ', ') AS Client_Names

FROM distinct_clients
GROUP BY Client_ID, Client_Phone
ORDER BY Client_Name;