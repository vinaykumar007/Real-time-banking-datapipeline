import pyarrow.parquet as pq

table = pq.read_table("sample.parquet")

print("===== PARQUET SCHEMA =====")
print(table.schema)

print("\n===== DATA =====")
print(table.to_pandas())