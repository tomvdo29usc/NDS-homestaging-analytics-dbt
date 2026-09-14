{{ config(materialized='ephemeral') }}

WITH statuses AS (
    SELECT
        MLS,

        MAX(CASE WHEN Event = 'Listed' THEN Date END) AS Listing_First_Active,
        MIN(CASE WHEN Event = 'Contingent' THEN Date END) AS Listing_First_Contingent,
        MIN(CASE WHEN Event = 'Pending' THEN Date END) AS Listing_First_Pending,
        MAX(CASE WHEN Event = 'Relisted' THEN Date END) AS Listing_Relisted,
        MAX(CASE WHEN Event = 'Contingent' THEN Date END) AS Listing_Last_Contingent,
        MAX(CASE WHEN Event = 'Pending' THEN Date END) AS Listing_Last_Pending,
        MAX(CASE WHEN Event = 'Price Changed' THEN Date END) AS Listing_Last_PriceChanged,
        MIN(CASE WHEN Event = 'Sold' THEN Date END) AS Listing_Sold,

        CAST(
            (
                SELECT Price
                FROM UNNEST(events)
                WHERE Event = 'Listed'
                ORDER BY Date ASC
                LIMIT 1
            ) AS INT64
        ) AS First_Listed_Price,

        CAST(
            (
                SELECT Price
                FROM UNNEST(events)
                WHERE Event IN ('Listed', 'Price Changed')
                  AND Date < (
                      SELECT MAX(Date)
                      FROM UNNEST(events)
                      WHERE Event = 'Relisted'
                  )
                ORDER BY Date DESC
                LIMIT 1
            ) AS INT64
        ) AS Pre_Relisted_Price,

        CAST(
            (
                SELECT Price
                FROM UNNEST(events)
                WHERE Event IN ('Price Changed', 'Listed')
                ORDER BY Date DESC
                LIMIT 1
            ) AS INT64
        ) AS Last_Asked_Price,

        (
            SELECT Price
            FROM UNNEST(events)
            WHERE Event = 'Sold'
            ORDER BY Date ASC
            LIMIT 1
        ) AS Sold_Price

    FROM (
        SELECT
            MLS,
            ARRAY_AGG(STRUCT(Date, Event, Price) ORDER BY Date ASC) AS events
        FROM {{ source('StagingOrders', 'Listing_History') }}
        GROUP BY MLS
    )
)

SELECT
    MLS,
    Listing_First_Active,
    Listing_First_Contingent,
    Listing_First_Pending,
    Listing_Relisted,
    Listing_Last_Contingent,
    Listing_Last_Pending,
    Listing_Last_PriceChanged,
    Listing_Sold,
    First_Listed_Price,
    COALESCE(Pre_Relisted_Price, First_Listed_Price) AS Pre_Relisted_Price,
    Last_Asked_Price,
    Sold_Price
FROM statuses