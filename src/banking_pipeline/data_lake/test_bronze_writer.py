from banking_pipeline.data_lake.bronze_writer import BronzeWriter


def main():
    writer = BronzeWriter()

    event = {
        "customer_id": 1001,
        "first_name": "Vinay",
        "last_name": "CDC",
        "email": "vinay@test.com",
        "operation": "create",
    }

    writer.write(
        table_name="customers",
        data=event,
    )

    print("✅ Bronze event uploaded successfully!")


if __name__ == "__main__":
    main()