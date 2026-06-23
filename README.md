
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

## Current Project Status

### Phase 1: OLTP Foundation ✅

* [x] Docker environment setup
* [x] PostgreSQL database
* [x] pgAdmin setup
* [x] Banking schema design
* [x] Entity Relationship Diagram (ERD)

### Phase 2: Data Generation 🚧

* [ ] Faker-based customer generation
* [ ] Account generation
* [ ] Transaction generation
* [ ] Bulk data loading

### Phase 3: CDC & Streaming

* [ ] Debezium setup
* [ ] Kafka cluster
* [ ] CDC event validation

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

### One suggestion

Create a folder:

```text
docs/
├── architecture/
│   └── architecture.png
├── erd/
│   └── banking_oltp_erd.png
```

Then place your architecture diagram and ERD there and reference them in the README. That immediately makes the repository look like a professional portfolio project.
