import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from banking_pipeline.data_lake.uploader import ADLSUploader


class ParquetWriter:
    def __init__(self):
        self.uploader = ADLSUploader()

    def write(
        self,
        directory: str,
        file_name: str,
        events: list[dict],
    ) -> None:

        table = pa.Table.from_pylist(events)

        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = Path(temp_dir) / file_name

            pq.write_table(
                table,
                parquet_file,
            )

            self.uploader.upload_file(
                directory=directory,
                file_name=file_name,
                local_file_path=str(parquet_file),
            )