import json

from banking_pipeline.data_lake.adls_client import ADLSClient


class ADLSUploader:
    def __init__(self):
        self.client = ADLSClient()
        self.file_system = self.client.get_file_system_client()

    def upload_text(
        self,
        directory: str,
        file_name: str,
        content: str,
    ) -> None:

        directory_client = self.file_system.get_directory_client(directory)
        directory_client.create_directory()

        file_client = directory_client.create_file(file_name)
        file_client.upload_data(content, overwrite=True)

    def upload_json(
        self,
        directory: str,
        file_name: str,
        data: dict,
    ) -> None:

        json_content = json.dumps(data, indent=2)

        self.upload_text(
            directory=directory,
            file_name=file_name,
            content=json_content,
        )

    def upload_file(
        self,
        directory: str,
        file_name: str,
        local_file_path: str,
    ) -> None:
        """
        Upload any local file (JSON, Parquet, CSV, etc.)
        """

        directory_client = self.file_system.get_directory_client(directory)
        directory_client.create_directory()
        file_client = directory_client.get_file_client(file_name)
        with open(local_file_path, "rb") as file:
            file_client.upload_data(
                file,
                overwrite=True,
            )