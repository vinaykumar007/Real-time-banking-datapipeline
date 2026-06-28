from banking_pipeline.data_lake.writers.parquet_writer import ParquetWriter

writer = ParquetWriter()

table = writer.create_table(
    {
        "id": 1,
        "name": "Vinay",
        "balance": 5000,
    }
)

print(table)