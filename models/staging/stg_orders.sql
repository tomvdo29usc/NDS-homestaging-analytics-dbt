-- Updated: 2026-09-01 — minor audit comment added
{{ config(materialized='view') }}

SELECT
    Request_Submitted,
    Order_ID,
    Client_Name,
    TRIM(Client_Phone) AS Client_Phone,
    CASE
        WHEN Client_Phone IS NULL OR TRIM(Client_Phone) = '' THEN "ERRORCLIENTID"
        ELSE CONCAT(
            'CUS',
            RIGHT(REGEXP_REPLACE(TRIM(Client_Phone), r'[^0-9]', ''), 10)
        )
    END AS Client_ID,
    Client_Email,
    Property_Address,
    TRIM(
        REGEXP_REPLACE(
            Property_Address,
            r',\s*[^,]+,\s*[A-Za-z]{2}\s*\d{5}(?:-\d{4})?$',
            ''
        )
    ) AS Property_StreetAddress,
    REGEXP_EXTRACT(Property_Address, r',\s*([^,]+),\s*[A-Za-z]{2}\s*\d{5}(?:-\d{4})?$') AS Property_City,
    REGEXP_EXTRACT(Property_Address, r',\s*([A-Za-z]{2})\s*\d{5}(?:-\d{4})?$') AS Property_State,
    REGEXP_EXTRACT(Property_Address, r',\s*[A-Za-z]{2}\s*(\d{5}(?:-\d{4})?)$') AS Property_Zipcode,
    Property_Description,
    Request_Description,
    Request_to_Stage_Before,
    Media_Request,
    Contract_Duration,
    Extended,
    Schedule_Staging_Date,
    Schedule_Staging_Time,
    End_Date,
    Paid,
    Status,
    Schedule_Pickup_Date,
    Schedule_Pickup_Time,
    Internal_Notes,
    User_Set_Archive,
    Updated_End_Date,
    Listing_URL,
    Listing_Status,
    Listing_Updated,
    MLS,
    Listing_Retrieved,
    Last_Listing_Status,
    Last_Listing_Updated,
    Current_Price,
    Staging_Complete,
    Pickup_Complete,
    Archive_Reason,
    Payment_Amount,
    Unknown_Outcome,
    Distance,
    Duration_Warehouse_Client,
    Owner_Name,
    Owner_Email,
    Owner_Phone,
    ETA_Input,
    Unlimited_Extension,
    Co_Agent_Name,
    Co_Agent_Email,
    Co_Agent_Phone
FROM {{ source('StagingOrders', 'Orders') }}