from azure.storage.filedatalake import DataLakeServiceClient

from banking_pipeline.data_lake.config import (
    AZURE_STORAGE_ACCOUNT,
    AZURE_STORAGE_CONTAINER,
)
from banking_pipeline.data_lake.credentials import get_credential


class ADLSClient:
    def __init__(self, storage_account: str | None = None):
        credential = get_credential()
        account_name = storage_account or AZURE_STORAGE_ACCOUNT

        if not account_name:
            raise ValueError("AZURE_STORAGE_ACCOUNT is not set.")

        self.container_name = AZURE_STORAGE_CONTAINER

        account_url = f"https://{account_name}.dfs.core.windows.net"

        self.service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=credential,
        )

    def get_file_system_client(self):
        return self.service_client.get_file_system_client(
            self.container_name
        )

    def list_file_systems(self):
        return [
            fs.name
            for fs in self.service_client.list_file_systems()
        ]