from confluent_kafka import Consumer

from banking_pipeline.kafka.config import (
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
)


class KafkaConsumer:
    def __init__(self, topics: list[str]):
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

                print(f"Topic : {msg.topic()}")
                print(f"Partition : {msg.partition()}")
                print(f"Offset : {msg.offset()}")
                print(msg.value().decode("utf-8"))
                print("-" * 80)

        except KeyboardInterrupt:
            print("\nStopping consumer...")

        finally:
            self.consumer.close()