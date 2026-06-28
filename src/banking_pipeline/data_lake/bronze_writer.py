from datetime import datetime



from banking_pipeline.data_lake.config import BRONZE_FILE_FORMAT
from banking_pipeline.data_lake.writers.json_writer import JSONWriter
from banking_pipeline.data_lake.writers.parquet_writer import ParquetWriter


class BronzeWriter:
    def __init__(self):
        if BRONZE_FILE_FORMAT == "parquet":
            self.writer = ParquetWriter()
        else:
            self.writer = JSONWriter()

    def write(self, table_name: str,events: list[dict]) -> None:
        now = datetime.utcnow()

        directory = (
            f"bronze/{table_name}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}"
        )

        extension = "parquet" if BRONZE_FILE_FORMAT == "parquet" else "json"

        file_name = (
                    f"{table_name}_"
                    f"{now.strftime('%Y%m%d_%H%M%S_%f')}."
                    f"{extension}"
                    )

        self.writer.write(
            directory=directory,
            file_name=file_name,
            events=events,
        )