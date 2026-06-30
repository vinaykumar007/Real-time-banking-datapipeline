USE ROLE ACCOUNTADMIN;
USE DATABASE BANKING_DB;
USE SCHEMA RAW;

CREATE OR REPLACE STAGE bronze_stage
STORAGE_INTEGRATION = banking_adls_integration
URL = 'azure://storageaccount.blob.core.windows.net/container/bronze'
FILE_FORMAT = parquet_format;
