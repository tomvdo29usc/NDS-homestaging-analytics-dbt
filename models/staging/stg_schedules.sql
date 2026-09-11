{{ config(materialized='view') }}

SELECT 
  Order_ID,
  Type,
  Schedule_Date,
  Schedule_Time,
  PARSE_DATETIME('%Y-%m-%d %H:%M:%S', CONCAT(Schedule_Date, ' ', Schedule_Time)) AS Schedule_Datetime,
  CAST(REGEXP_EXTRACT(Duration, r'(\d+)h') AS INT64) + CAST(REGEXP_EXTRACT(Duration, r'(\d+)m') AS INT64)/60 AS Duration_Hours
FROM {{ source('StagingOrders', 'All_Schedules') }}
WHERE Order_ID IS NOT NULL AND Confirm = True