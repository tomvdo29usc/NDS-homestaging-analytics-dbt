{{ config(materialized='ephemeral') }}

SELECT
    Order_ID,
    MAX(CASE WHEN LOWER(TRIM(Type)) = 'walkthru' THEN Schedule_Datetime END) AS Walkthru_DateTime,
    MAX(CASE WHEN LOWER(TRIM(Type)) = 'pick & load' THEN Schedule_Datetime END) AS PicknLoad_Datetime,
    MAX(CASE WHEN LOWER(TRIM(Type)) = 'staging' THEN Schedule_Datetime END) AS Staging_Datetime,
    MAX(CASE WHEN LOWER(TRIM(Type)) = 'supplement staging' THEN Schedule_Datetime END) AS SupplStaging_Datetime,
    MAX(CASE WHEN LOWER(TRIM(Type)) = 'destaging' THEN Schedule_Datetime END) AS Pickup_Datetime
FROM {{ ref('stg_schedules') }}
GROUP BY Order_ID