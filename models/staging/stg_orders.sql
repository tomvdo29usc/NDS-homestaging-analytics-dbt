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
WHERE Client_Name <> "Tom Do"
    AND NOT (
        Request_Submitted IS NULL
        AND Order_ID IS NULL
        AND Client_Name IS NULL
        AND Client_Phone IS NULL
        AND Client_Email IS NULL
        AND Property_Address IS NULL
        AND Property_Description IS NULL
        AND Request_Description IS NULL
        AND Request_to_Stage_Before IS NULL
        AND Media_Request IS NULL
        AND Contract_Duration IS NULL
        AND Extended IS NULL
        AND Schedule_Staging_Date IS NULL
        AND Schedule_Staging_Time IS NULL
        AND End_Date IS NULL
        AND Paid IS NULL
        AND Status IS NULL
        AND Schedule_Pickup_Date IS NULL
        AND Schedule_Pickup_Time IS NULL
        AND Internal_Notes IS NULL
        AND User_Set_Archive IS NULL
        AND Updated_End_Date IS NULL
        AND Listing_URL IS NULL
        AND Listing_Status IS NULL
        AND Listing_Updated IS NULL
        AND MLS IS NULL
        AND Listing_Retrieved IS NULL
        AND Last_Listing_Status IS NULL
        AND Last_Listing_Updated IS NULL
        AND Current_Price IS NULL
        AND Staging_Complete IS NULL
        AND Pickup_Complete IS NULL
        AND Archive_Reason IS NULL
        AND Payment_Amount IS NULL
        AND Unknown_Outcome IS NULL
        AND Distance IS NULL
        AND Duration_Warehouse_Client IS NULL
        AND Owner_Name IS NULL
        AND Owner_Email IS NULL
        AND Owner_Phone IS NULL
        AND ETA_Input IS NULL
        AND Unlimited_Extension IS NULL
        AND Co_Agent_Name IS NULL
        AND Co_Agent_Email IS NULL
        AND Co_Agent_Phone IS NULL
    )
    AND Request_Submitted IS NOT NULL