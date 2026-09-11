-- {{ config(
--     materialized='table',
--     indexes=[
--         {'columns': ['Order_ID'], 'type': 'hash'},
--         {'columns': ['Client_ID'], 'type': 'hash'},
--         {'columns': ['Request_Submitted'], 'type': 'btree'}
--     ]
-- ) }}

-- SELECT
--     -- Order Keys
--     Order_ID,
--     Client_ID,
    
--     -- Dates
--     Request_Submitted,
--     Schedule_Staging_Date,
--     End_Date,
--     Schedule_Pickup_Date,
--     Updated_End_Date,
--     Listing_Updated,
--     Listing_Retrieved,
--     Last_Listing_Updated,
    
--     -- Order Details
--     Client_Name,
--     Client_Phone,
--     Client_Email,
    
--     -- Property Information
--     Property_Address,
--     Property_StreetAddress,
--     Property_City,
--     Property_State,
--     Property_Zipcode,
--     Property_Description,
    
--     -- Request & Media Details
--     Request_Description,
--     Request_to_Stage_Before,
--     Media_Request,
    
--     -- Contract Details
--     Contract_Duration,
--     Extended,
--     Unlimited_Extension,
    
--     -- Timing
--     Schedule_Staging_Time,
--     Schedule_Pickup_Time,
--     ETA_Input,
--     Duration_Warehouse_Client,
--     Distance,
    
--     -- Staging & Pickup Status
--     Staging_Complete,
--     Pickup_Complete,
    
--     -- Order Status & Outcome
--     Status,
--     Paid,
--     Payment_Amount,
--     Unknown_Outcome,
    
--     -- Archival & Notes
--     User_Set_Archive,
--     Archive_Reason,
--     Internal_Notes,
    
--     -- Listing Information
--     Listing_URL,
--     Listing_Status,
--     Last_Listing_Status,
--     MLS,
    
--     -- Owner Information
--     Owner_Name,
--     Owner_Email,
--     Owner_Phone,
    
--     -- Co-Agent Information
--     Co_Agent_Name,
--     Co_Agent_Email,
--     Co_Agent_Phone

-- FROM {{ ref('stg_orders') }}
