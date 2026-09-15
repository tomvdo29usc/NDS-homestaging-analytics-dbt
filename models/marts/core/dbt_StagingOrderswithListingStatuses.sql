{{ config(materialized='table') }}

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
    ord.MLS,
    ord.Archive_Reason,
    COALESCE(ord.Payment_Amount, proposal.Quote_Price) AS Payment_Amount,
    proposal.Sent_Date AS Proposal_SentDate,
    proposal.Closed_Date AS Proposal_ClosedDate,
    CASE WHEN proposal.Contract_Duration = "Unlimited Term" THEN 45 ELSE CAST(proposal.Contract_Duration AS INT64) END AS Proposal_Duration,
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
        FROM {{ ref('int_pivot_listingstatus') }}
        WHERE Listing_First_Active = (SELECT MAX(Listing_First_Active) 
                                      FROM {{ ref('int_pivot_listingstatus') }})) 
            AS most_recent_First_Listed_Price
FROM {{ ref('stg_orders') }} AS ord
LEFT JOIN {{ ref('int_pivot_listingstatus') }} AS sts
    ON ord.MLS = sts.MLS
LEFT JOIN {{ source('StagingOrders', 'Proposal') }} proposal
  ON ord.Order_ID = proposal.Order_ID
