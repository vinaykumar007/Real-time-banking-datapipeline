
# End-to-End Banking Data Engineering Pipeline

## Project Overview

This project demonstrates an end-to-end modern data engineering pipeline for a banking domain.

The system simulates a real-world banking environment by generating customer, account, and transaction data, capturing changes in real time, processing them through a modern data stack, and delivering analytics-ready datasets for reporting and business intelligence.

The project follows industry-standard data engineering practices including:

* OLTP database design
* Change Data Capture (CDC)
* Event streaming
* Data Lake architecture
* Cloud data warehousing
* Data transformation
* Orchestration
* CI/CD automation

---

## Architecture

```text
Data Generator (Faker)
          │
          ▼
     PostgreSQL
          │
          ▼
 Debezium CDC
          │
          ▼
       Kafka
          │
          ▼
 Azure Data Lake Storage Gen2
          │
          ▼
      Snowflake
          │
          ▼
         dbt
          │
          ▼
      Power BI
```

---

## Technology Stack

| Layer            | Technology                   |
| ---------------- | ---------------------------- |
| Data Generation  | Python, Faker                |
| Source Database  | PostgreSQL                   |
| CDC              | Debezium                     |
| Streaming        | Apache Kafka                 |
| Data Lake        | Azure Data Lake Storage Gen2 |
| Data Warehouse   | Snowflake                    |
| Transformations  | dbt                          |
| Orchestration    | Apache Airflow               |
| Containerization | Docker                       |
| Version Control  | Git                          |
| CI/CD            | GitHub Actions               |
| Visualization    | Power BI                     |

---

# Change Data Capture (CDC) Workflow

## Overview

This project implements a real-time Change Data Capture (CDC) pipeline using PostgreSQL, Debezium, and Apache Kafka.

Instead of polling the database for changes, Debezium reads PostgreSQL's Write Ahead Log (WAL) and streams inserts, updates, and deletes directly into Kafka topics.

```text
PostgreSQL
     │
     ▼
Write Ahead Log (WAL)
     │
     ▼
Logical Replication
     │
     ▼
Debezium Connector
     │
     ▼
Kafka Topics
     │
     ▼
Consumers (PySpark / Airflow / Analytics)
```

---

## Why CDC?

Traditional ETL pipelines often use polling:

```sql
SELECT *
FROM transactions
WHERE created_at > last_run_time;
```

This approach introduces:

* High database load
* Increased latency
* Risk of missing updates
* Scalability limitations

CDC solves these problems by streaming only changed records in real time.

---

## PostgreSQL Configuration

CDC requires PostgreSQL Logical Replication.

The PostgreSQL container is configured with:

```yaml
command:
  - postgres
  - -c
  - wal_level=logical
```

Verify:

```sql
SHOW wal_level;
```

Expected:

```text
logical
```

---

## Replication User

Debezium requires a replication-enabled PostgreSQL user.

Verify:

```sql
SELECT rolname, rolreplication
FROM pg_roles
WHERE rolname = 'banking_admin';
```

Expected:

```text
banking_admin | t
```

---

## Debezium Connector

Debezium connects to PostgreSQL and reads changes from WAL.

Connector configuration:

```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "banking_admin",
    "database.password": "********",
    "database.dbname": "banking_db",
    "topic.prefix": "banking_server",
    "schema.include.list": "banking",
    "table.include.list": "banking.customers,banking.accounts,banking.transactions",
    "plugin.name": "pgoutput",
    "slot.name": "banking_slot",
    "publication.autocreate.mode": "filtered"
  }
}
```

---

## Kafka Topics

Debezium automatically creates CDC topics.

### Business Topics

```text
banking_server.banking.customers
banking_server.banking.accounts
banking_server.banking.transactions
```

### Internal Kafka Connect Topics

```text
connect-configs
connect-offsets
connect-status
```

Purpose:

| Topic           | Description                           |
| --------------- | ------------------------------------- |
| connect-configs | Stores connector configurations       |
| connect-offsets | Stores CDC progress and WAL positions |
| connect-status  | Stores connector state                |

---

## PostgreSQL Publication

Debezium automatically creates a publication:

```sql
SELECT * FROM pg_publication;
```

Example:

```text
dbz_publication
```

The publication defines which tables participate in logical replication.

---

## Replication Slot

Debezium creates a replication slot:

```sql
SELECT slot_name, active
FROM pg_replication_slots;
```

Example:

```text
banking_slot | t
```

Replication slots ensure WAL records are not removed before Debezium consumes them.

---

## CDC Event Lifecycle

### 1. Insert Event

Application inserts a customer:

```sql
INSERT INTO banking.customers
(
    first_name,
    last_name,
    email
)
VALUES
(
    'VINAY',
    'CDC',
    'vinay_cdc@test.com'
);
```

### 2. PostgreSQL WAL

PostgreSQL writes the transaction to WAL.

### 3. Debezium Reads WAL

Debezium captures the change through logical replication.

### 4. Kafka Event Produced

Kafka topic:

```text
banking_server.banking.customers
```

receives:

```json
{
  "before": null,
  "after": {
    "customer_id": 1001,
    "first_name": "VINAY",
    "last_name": "CDC",
    "email": "vinay_cdc@test.com"
  },
  "op": "c"
}
```

---

## Debezium Event Structure

A CDC event contains:

```json
{
  "before": null,
  "after": {...},
  "source": {...},
  "op": "c",
  "ts_ms": 1782321575990
}
```

### before

Represents the row before the change.

Example:

```json
"before": null
```

for inserts.

---

### after

Represents the row after the change.

Example:

```json
"after": {
  "customer_id": 1001,
  "first_name": "VINAY"
}
```

---

### op

Operation type.

| Code | Meaning         |
| ---- | --------------- |
| c    | Create (INSERT) |
| u    | Update          |
| d    | Delete          |
| r    | Snapshot Record |

---

### source

Metadata about the originating database.

Example:

```json
{
  "db": "banking_db",
  "schema": "banking",
  "table": "customers",
  "txId": 765,
  "lsn": 74589320
}
```

---

### ts_ms

Timestamp when Debezium emitted the event.

---

## Example CDC Operations

### Insert

```sql
INSERT INTO banking.customers (...);
```

Produces:

```json
{
  "before": null,
  "after": {...},
  "op": "c"
}
```

---

### Update

```sql
UPDATE banking.customers
SET first_name = 'UPDATED'
WHERE customer_id = 1001;
```

Produces:

```json
{
  "before": {...},
  "after": {...},
  "op": "u"
}
```

---

### Delete

```sql
DELETE FROM banking.customers
WHERE customer_id = 1001;
```

Produces:

```json
{
  "before": {...},
  "after": null,
  "op": "d"
}
```

---

## Validation Steps

### Verify Connector

```powershell
Invoke-RestMethod http://localhost:8083/connectors/postgres-connector/status
```

Expected:

```json
{
  "connector": {
    "state": "RUNNING"
  },
  "tasks": [
    {
      "state": "RUNNING"
    }
  ]
}
```

---

### Verify Topics

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

Expected:

```text
banking_server.banking.customers
banking_server.banking.accounts
banking_server.banking.transactions
```

---

### Verify Messages

Kafka UI:

```text
http://localhost:8090
```

Navigate:

```text
Topics
  └── banking_server.banking.customers
         └── Messages
```

Insert or update records in PostgreSQL and observe CDC events appearing in Kafka.

---

## Key Learnings

* PostgreSQL WAL-based CDC
* Logical Replication
* Debezium Source Connector
* Kafka Connect Architecture
* Kafka Topic Design
* Replication Slots
* Publications
* Event-Driven Data Pipelines
* Real-Time Data Streaming

This CDC layer forms the foundation for the downstream Bronze → Silver → Gold Medallion Architecture implemented using PySpark Structured Streaming.

---

## Current Project Status

### Phase 1: OLTP Foundation ✅

* [x] Docker environment setup
* [x] PostgreSQL database
* [x] pgAdmin setup
* [x] Banking schema design
* [x] Entity Relationship Diagram (ERD)

### Phase 2: Data Generation 🚧

* [x] Faker-based customer generation
* [x] Account generation
* [x] Transaction generation
* [x] Bulk data loading

### Phase 3: CDC & Streaming

* [x] Debezium setup
* [x] Kafka cluster
* [x] CDC event validation

### Phase 4: Data Lake

* [ ] Azure ADLS Gen2 integration
* [ ] Raw zone ingestion
* [ ] Parquet storage

### Phase 5: Snowflake

* [ ] Bronze layer
* [ ] Silver layer
* [ ] Gold layer

### Phase 6: Analytics

* [ ] dbt models
* [ ] dbt tests
* [ ] dbt snapshots (SCD Type 2)

### Phase 7: Visualization

* [ ] Power BI dashboards

### Phase 8: CI/CD

* [ ] GitHub Actions
* [ ] Automated testing
* [ ] Deployment workflows

---

# Banking OLTP Data Model

The operational database consists of three core banking entities:

## Customers

Stores customer information.

| Column        |
| ------------- |
| customer_id   |
| first_name    |
| last_name     |
| email         |
| phone_number  |
| date_of_birth |
| created_at    |
| updated_at    |

---

## Accounts

Stores customer bank accounts.

| Column         |
| -------------- |
| account_id     |
| account_number |
| customer_id    |
| account_type   |
| account_status |
| balance        |
| currency_code  |
| created_at     |
| updated_at     |

---

## Transactions

Stores financial transactions.

| Column                |
| --------------------- |
| transaction_id        |
| account_id            |
| transaction_type      |
| amount                |
| related_account_id    |
| status                |
| description           |
| transaction_timestamp |
| created_at            |

---

## Entity Relationship Diagram


![Banking ERD](docs/erd/banking_oltp_erd.png)

---

## Project Structure

```text
Real-time-banking-datapipeline/
│
├── docs/
│   └── erd/
│
├── sql/
│   ├── schema.sql
│   └── seed.sql
│
├── data_generator/
│   ├── database.py
│   ├── generate_customers.py
│   ├── generate_accounts.py
│   └── generate_transactions.py
│
├── docker/
│   └── postgres/
│
├── airflow/
│
├── dbt/
│
├── infrastructure/
│
├── tests/
│
├── .env
├── docker-compose.yml
└── README.md
```

---

## Local Development Setup

### Start Services

```bash
docker compose up -d
```

### Verify Running Containers

```bash
docker ps
```

### Open pgAdmin

```text
http://localhost:5050
```

---

## Running the Data Generator

Preferred (module):

```bash
# run from project root
python -m data_generator.generate_data
```

Or run directly (supported):

```bash
python data_generator/generate_data.py
```

Note: The repository adds the project root to `sys.path` when the script is executed directly to make imports resolve; running with `-m` is the recommended approach.

### Debezium connector note

The Debezium connector script uses these environment variables:
`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`.
If `POSTGRES_HOST` or `POSTGRES_PORT` are not set, the script defaults to `postgres:5432` for container networking.

---

## Learning Goals

This project demonstrates practical experience with:

* Data Modeling
* PostgreSQL
* CDC Architecture
* Event Streaming
* Data Lake Design
* Snowflake Data Warehousing
* dbt Transformations
* Apache Airflow
* CI/CD Pipelines
* Cloud Data Engineering

---

## Future Enhancements

* Fraud detection models
* Real-time monitoring
* Kafka Schema Registry
* Data quality monitoring
* Infrastructure as Code (Terraform)
* Azure-native deployment

---

