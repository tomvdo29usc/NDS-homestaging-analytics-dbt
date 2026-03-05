WITH 
all_schedules_clean_stage1 AS (
    SELECT
        Order_ID,
        CASE WHEN Type = "Destaging" THEN "Pickup" ELSE Type END AS Type,
        PARSE_DATE('%m/%d/%Y', Schedule_Date) AS Schedule_Date,
        Schedule_Time,
        Duration,
        Staffs
    FROM {{ source('StagingOrders', 'All_Schedules') }}
),
all_schedules_clean_stage2 AS (
    SELECT
    Order_ID,
    Type,
    CONCAT(
      CAST(
        EXTRACT(YEAR FROM Schedule_Date) AS STRING),
        '-',
        LPAD(
          CAST(
            EXTRACT(MONTH FROM Schedule_Date) AS STRING), 2, '0')
            ) AS YearMonth
FROM all_schedules_clean_stage1
)

SELECT
    YearMonth,
    CONCAT(YearMonth, '-01') AS YearMonthDay,
    Type,
    COUNT(Order_ID) AS Total_Schedules
    
FROM all_schedules_clean_stage2
GROUP BY YearMonth, Type
HAVING YearMonth IS NOT NULL
ORDER BY YearMonth