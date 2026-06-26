from datetime import datetime

from banking_pipeline.data_lake.uploader import ADLSUploader


def main():
    uploader = ADLSUploader()

    data = {
        "message": "ADLS upload successful",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "upload_test",
    }

    uploader.upload_json(
        directory="bronze/test",
        file_name="connection_test.json",
        data=data,
    )

    print("✅ File uploaded successfully!")


if __name__ == "__main__":
    main()