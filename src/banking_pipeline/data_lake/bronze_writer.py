from datetime import datetime

from banking_pipeline.data_lake.writers.json_writer import JSONWriter


class BronzeWriter:
    def __init__(self):
        self.writer = JSONWriter()

    def write(self, table_name: str, data: dict) -> None:
        now = datetime.utcnow()

        directory = (
            f"bronze/{table_name}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}"
        )

        file_name = (
            f"{table_name}_"
            f"{now.strftime('%Y%m%d_%H%M%S_%f')}.json"
        )

        self.writer.write(
            directory=directory,
            file_name=file_name,
            data=data,
        )