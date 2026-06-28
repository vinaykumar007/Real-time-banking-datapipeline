from banking_pipeline.data_lake.uploader import ADLSUploader


class JSONWriter:
    def __init__(self):
        self.uploader = ADLSUploader()

    def write(
        self,
        directory: str,
        file_name: str,
        data: dict,
    ) -> None:
        self.uploader.upload_json(
            directory=directory,
            file_name=file_name,
            data=data,
        )