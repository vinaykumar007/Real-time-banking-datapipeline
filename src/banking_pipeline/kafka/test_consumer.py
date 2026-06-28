from banking_pipeline.kafka.consumer import KafkaConsumer


def main():
    TOPICS = [
    "banking_server.banking.customers",
    "banking_server.banking.accounts",
    "banking_server.banking.transactions",
    ]

    consumer = KafkaConsumer(TOPICS)

    consumer.consume()


if __name__ == "__main__":
    main()