SELECT
    event_id,
    table_name,
    operation,
    loaded_at
FROM RAW.RAW_EVENTS
ORDER BY loaded_at DESC
LIMIT 10;


USE DATABASE BANKING_DB;
USE SCHEMA RAW;


SELECT
    PAYLOAD:after.customer_id::NUMBER      AS customer_id,
    PAYLOAD:after.first_name::STRING       AS first_name,
    PAYLOAD:after.last_name::STRING        AS last_name,
    PAYLOAD:after.email::STRING            AS email,
    PAYLOAD:after.phone_number::STRING     AS phone_number,
    PAYLOAD:after.date_of_birth::DATE      AS date_of_birth,
    OPERATION,
    LOADED_AT
FROM RAW_EVENTS
WHERE TABLE_NAME = 'customers'
ORDER BY LOADED_AT DESC;