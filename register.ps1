curl -X POST `
-H "Content-Type: application/json" `
--data "@debezium/postgres-connector.json" `
http://localhost:8083/connectors