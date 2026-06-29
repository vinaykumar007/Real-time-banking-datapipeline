from confluent_kafka import Consumer
from datetime import datetime
import json

from banking_pipeline.data_lake.bronze_writer import BronzeWriter
from banking_pipeline.data_lake.buffer import EventBuffer
from banking_pipeline.data_lake.config import (BRONZE_BUFFER_SIZE, BRONZE_FLUSH_INTERVAL_SECONDS,)

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
        self.buffer = EventBuffer(max_size=BRONZE_BUFFER_SIZE)
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
    def _flush_buffer(
                      self,table_name: str,reason: str,) -> None:
        """
        Flush buffered events to the Bronze layer.
        """

        print(
            f"\n🚀 Flushing buffer ({reason}) "
            f"- {self.buffer.size()} events"
        )

        self.bronze_writer.write(
            table_name=table_name,
            events=self.buffer.get_events(),
        )

        self.buffer.clear()

        print("✅ Buffer cleared\n")
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
                self.buffer.add(bronze_record)
                flush_reason = None

                if self.buffer.is_full():
                    flush_reason = "BUFFER SIZE"

                elif self.buffer.should_flush(
                    BRONZE_FLUSH_INTERVAL_SECONDS
                ):
                    flush_reason = "TIME INTERVAL"

                if flush_reason:
                    self._flush_buffer(
                        table_name=table_name,
                        reason=flush_reason,
                    )

                operation = CDC_OPERATIONS.get(
                                               payload["op"],
                                               payload["op"],)

                print(
                    f"📦 Buffered ({self.buffer.size()}/{self.buffer.max_size}) | "
                    f"Table: {table_name} | "
                    f"Operation: {operation}"
                )

        except KeyboardInterrupt:
            print("\n🛑 Shutdown requested.")
        
            if self.buffer.size() > 0:
                self._flush_buffer(
                    table_name=table_name,
                    reason="SHUTDOWN",
                )
        
            print("👋 Kafka consumer stopped.")


        finally:
            self.consumer.close()