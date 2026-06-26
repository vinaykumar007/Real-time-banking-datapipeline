from banking_pipeline.kafka.consumer import KafkaConsumer


def main():
    consumer = KafkaConsumer(
        topics=[
            "banking_server.banking.customers",
        ]
    )

    consumer.consume()


if __name__ == "__main__":
    main()