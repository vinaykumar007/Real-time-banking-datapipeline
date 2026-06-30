USE ROLE ACCOUNTADMIN;
USE DATABASE BANKING_DB;
USE SCHEMA RAW;

CREATE OR REPLACE PIPE raw_events_pipe
AUTO_INGEST = TRUE
INTEGRATION = BANKING_NOTIFICATION_INTEGRATION
AS
COPY INTO RAW_EVENTS
(
    ingestion_timestamp,
    topic,
    partition,
    offset,
    table_name,
    operation,
    payload
)
FROM
(
    SELECT
        $1:ingestion_timestamp::TIMESTAMP_NTZ,
        $1:topic::STRING,
        $1:partition::NUMBER,
        $1:offset::NUMBER,
        $1:payload.source.table::STRING,
        $1:payload.op::STRING,
        $1:payload
    FROM @bronze_stage
);

SHOW NOTIFICATION INTEGRATIONS;

SHOW PIPES;

SELECT SYSTEM$PIPE_STATUS('RAW_EVENTS_PIPE');

LIST @BRONZE_STAGE;