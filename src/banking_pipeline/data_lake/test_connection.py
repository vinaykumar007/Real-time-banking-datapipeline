
from banking_pipeline.data_lake.adls_client import ADLSClient
def main():
    client = ADLSClient()

    print("\nConnected to Azure Data Lake!\n")

    print("Containers:\n")

    for container in client.list_file_systems():
        print(container)


if __name__ == "__main__":
    main()