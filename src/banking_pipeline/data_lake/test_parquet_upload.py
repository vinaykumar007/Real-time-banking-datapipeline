from banking_pipeline.data_lake.writers.parquet_writer import ParquetWriter

writer = ParquetWriter()

writer.write(
    directory="bronze/test",
    file_name="sample.parquet",
    data={
        "id": 1,
        "name": "Vinay",
        "balance": 5000,
    },
)

print("Parquet uploaded successfully!")