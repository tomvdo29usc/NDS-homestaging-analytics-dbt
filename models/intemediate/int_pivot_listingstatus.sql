
WITH
  statuses AS (
    SELECT
      MLS,
      MAX(CASE WHEN e.Event = 'Listed' THEN e.Date END) AS Listing_First_Active,
      MIN(CASE WHEN e.Event = 'Contingent' THEN e.Date END)
        AS Listing_First_Contingent,
      MIN(CASE WHEN e.Event = 'Pending' THEN e.Date END)
        AS Listing_First_Pending,
      MAX(CASE WHEN e.Event = 'Relisted' THEN e.Date END) AS Listing_Relisted,
      MAX(CASE WHEN e.Event = 'Contingent' THEN e.Date END)
        AS Listing_Last_Contingent,
      MAX(CASE WHEN e.Event = 'Pending' THEN e.Date END)
        AS Listing_Last_Pending,
      MAX(CASE WHEN e.Event = 'Price Changed' THEN e.Date END)
        AS Listing_Last_PriceChanged,
      MIN(CASE WHEN e.Event = 'Sold' THEN e.Date END) AS Listing_Sold,
      CAST(
        (
          SELECT Price
          FROM UNNEST(events) AS e
          WHERE e.Event = 'Listed'
          ORDER BY e.Date ASC
          LIMIT 1
        )
        AS INT64)
        AS First_Listed_Price,
      CAST(
        (
          SELECT Price
          FROM UNNEST(events) AS e
          WHERE
            e.Event IN ('Listed', 'Price Changed')
            AND e.Date < (
              SELECT MAX(inner_e.Date)
              FROM UNNEST(events) AS inner_e
              WHERE inner_e.Event = 'Relisted'
            )
          ORDER BY e.Date DESC
          LIMIT 1
        )
        AS INT64)
        AS Pre_Relisted_Price,
      CAST(
        (
          SELECT Price
          FROM UNNEST(events) AS e
          WHERE e.Event IN ('Price Changed', 'Listed')
          ORDER BY e.Date DESC
          LIMIT 1
        )
        AS INT64)
        AS Last_Asked_Price,
      (
        SELECT Price
        FROM UNNEST(events) AS e
        WHERE e.Event = 'Sold'
        ORDER BY e.Date ASC
        LIMIT 1
      ) AS Sold_Price
    FROM
      (
        SELECT
          MLS, ARRAY_AGG(STRUCT(Date, Event, Price) ORDER BY Date ASC) AS events
        FROM {{ source('StagingOrders', 'Listing_History') }}
        GROUP BY MLS
      )
    CROSS JOIN UNNEST(events) AS e
    GROUP BY MLS, events
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