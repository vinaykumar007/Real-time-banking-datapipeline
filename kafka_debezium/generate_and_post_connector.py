import os
import json
import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

missing_vars = [
    name
    for name, value in [
        ("POSTGRES_USER", postgres_user),
        ("POSTGRES_PASSWORD", postgres_password),
        ("POSTGRES_DB", postgres_db),
    ]
    if not value
]

if missing_vars:
    raise SystemExit(
        "Missing required environment variables: "
        + ", ".join(missing_vars)
        + ". Please add them to your .env file or environment."
    )

if not os.getenv("POSTGRES_HOST"):
    print("Warning: POSTGRES_HOST is not set; defaulting to 'postgres' for Debezium container networking.")

if not os.getenv("POSTGRES_PORT"):
    print("Warning: POSTGRES_PORT is not set; defaulting to '5432'.")

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": postgres_host,
        "database.port": postgres_port,
        "database.user": postgres_user,
        "database.password": postgres_password,
        "database.dbname": postgres_db,
        "topic.prefix": "banking_server",
        "table.include.list": "banking.customers,banking.accounts,banking.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# -----------------------------
# Send request to Debezium Connect
# -----------------------------
url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

response = requests.post(url, headers=headers, json=connector_config)

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("Connector created successfully!")
elif response.status_code == 409:
    print("Connector already exists.")
else:
    print(f"Failed to create connector ({response.status_code}): {response.text}")
