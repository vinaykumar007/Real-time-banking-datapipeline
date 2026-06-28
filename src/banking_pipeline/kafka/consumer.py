from confluent_kafka import Consumer
from datetime import datetime
import json

from banking_pipeline.data_lake.bronze_writer import BronzeWriter

from banking_pipeline.kafka.config import (
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
)

CDC_OPERATIONS = {
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
    "r": "SNAPSHOT",
}


class KafkaConsumer:
    def __init__(self, topics: list[str]):
        self.bronze_writer = BronzeWriter()
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
            }
        )

        self.consumer.subscribe(topics)
        print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")

    def consume(self):
        print("🚀 Kafka Consumer started... Press Ctrl+C to stop.\n")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(msg.error())
                    continue

                event = json.loads(msg.value().decode("utf-8"))

                payload = event["payload"]
                source = payload["source"]
                table_name = source["table"]
                bronze_record = {
                                 "ingestion_timestamp": datetime.utcnow().isoformat(),
                                 "topic": msg.topic(),
                                 "partition": msg.partition(),
                                 "offset": msg.offset(),
                                 "payload": payload,
                                 }
                
                

                self.bronze_writer.write(
                                         table_name=table_name,
                                                             data=bronze_record,
                                        )

                
                print(f"✅ Bronze event written")
                print(f"Topic      : {msg.topic()}")
                print(f"Table      : {table_name}")
                operation = CDC_OPERATIONS.get(payload["op"], payload["op"])

                print(f"Operation  : {operation}")
                print(f"Offset     : {msg.offset()}")
                print("-" * 80)

        except KeyboardInterrupt:
            print("\nStopping consumer...")

        finally:
            self.consumer.close()