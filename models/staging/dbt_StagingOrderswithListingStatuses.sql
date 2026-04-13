{{ config(materialized='view') }}

WITH statuses AS (
  SELECT
    MLS,

    (SELECT MAX(Date) FROM UNNEST(events) WHERE Event = 'Listed') AS Listing_First_Active,

    (SELECT MIN(Date) FROM UNNEST(events) WHERE Event = 'Contingent') AS Listing_First_Contingent,

    (SELECT MIN(Date) FROM UNNEST(events) WHERE Event = 'Pending') AS Listing_First_Pending,

    (SELECT MAX(Date) FROM UNNEST(events) WHERE Event = 'Relisted') AS Listing_Relisted,

    (SELECT MAX(Date) FROM UNNEST(events) WHERE Event = 'Contingent') AS Listing_Last_Contingent,

    (SELECT MAX(Date) FROM UNNEST(events) WHERE Event = 'Pending') AS Listing_Last_Pending,

    (SELECT MAX(Date) FROM UNNEST(events) WHERE Event = 'Price Changed') AS Listing_Last_PriceChanged,

    (SELECT MIN(Date) FROM UNNEST(events) WHERE Event = 'Sold') AS Listing_Sold,

    CAST((SELECT Price FROM UNNEST(events)
     WHERE Event = 'Listed'
     ORDER BY Date ASC
     LIMIT 1) AS INT64) AS First_Listed_Price,

     CAST((
      SELECT Price
      FROM UNNEST(events)
      WHERE Event IN ('Listed', 'Price Changed')
        AND Date < (
          SELECT MIN(Date)
          FROM UNNEST(events)
          WHERE Event = 'Relisted'
        )
      ORDER BY Date DESC
      LIMIT 1
    ) AS INT64) AS Pre_Relisted_Price,

    CAST((SELECT Price FROM UNNEST(events)
     WHERE Event IN ('Price Changed', 'Listed')
     ORDER BY Date DESC
     LIMIT 1) AS INT64) AS Last_Asked_Price,

    (SELECT Price FROM UNNEST(events)
     WHERE Event = 'Sold'
     ORDER BY Date ASC
     LIMIT 1) AS Sold_Price

  FROM (
    SELECT
      MLS,
      ARRAY_AGG(STRUCT(Date, Event, Price) ORDER BY Date ASC) AS events
    FROM {{ source('StagingOrders', 'Listing_History') }}
    GROUP BY MLS
  )
)

SELECT
    ord.Order_ID,
    ord.Request_Submitted,
    ord.Client_Name,
    ord.Property_Address,
    ord.Property_Description,
    ord.Media_Request,
    ord.Contract_Duration,
    ord.Extended,
    ord.Request_to_Stage_Before AS Stage_Before,
    ord.Schedule_Staging_Date,
    ord.Schedule_Staging_Time,
    ord.End_Date,
    ord.Paid,
    ord.Status,
    ord.Schedule_Pickup_Date,
    ord.Schedule_Pickup_Time,
    ord.Pickup_Complete,
    ord.Updated_End_Date,
    ord.Listing_Status,
    ord.Listing_Updated,
    ord.Listing_Retrieved,
    ord.Last_Listing_Status,
    ord.Current_Price,
    ord.MLS,
    ord.Archive_Reason,
    ord.Payment_Amount,
    ord.Distance,
    ord.Duration_Warehouse_Client,
    ord.Unknown_Outcome,
    sts.Listing_First_Active,
    sts.Listing_First_Contingent,
    sts.Listing_First_Pending,
    sts.Listing_Relisted,
    sts.Listing_Last_Contingent,
    sts.Listing_Last_Pending,
    Listing_Last_PriceChanged,
    sts.Listing_Sold,
    First_Listed_Price,
    COALESCE(Pre_Relisted_Price, First_Listed_Price) AS Pre_Relisted_Price,
    Last_Asked_Price,
    CAST(sts.Sold_Price AS INT64) AS Sold_Price,
    (   SELECT SUM(First_Listed_Price) 
        FROM statuses 
        WHERE Listing_First_Active = (SELECT MAX(Listing_First_Active) 
                                      FROM statuses)) 
            AS most_recent_First_Listed_Price
FROM {{ source('StagingOrders', 'Orders') }} AS ord
LEFT JOIN statuses AS sts
    ON ord.MLS = sts.MLS
WHERE Client_Name <> "Tom Do"