import pyarrow.parquet as pq

table = pq.read_table("sample.parquet")

print(table.to_pandas())