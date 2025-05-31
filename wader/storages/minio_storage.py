from tempfile import NamedTemporaryFile

from minio import Minio

from wader.storages.storage import Storage


class MinIOStorage(Storage):
    def __init__(self, client: Minio):
        self._client = client

    def get(self, object_path: str) -> bytes:
        bucket_name, object_name = self._interpret_object_path(object_path)
        response = self._client.get_object(bucket_name, object_name)
        content = response.read()
        response.close()

        return content

    def put(self, object_path: str, value: bytes):
        with NamedTemporaryFile('wb+') as temp_file:
            temp_file.write(value)
            temp_file.close()

            self.upload(object_path, temp_file.name)

    def upload(self, object_path: str, source_path: str):
        bucket_name, object_name = self._interpret_object_path(object_path)

        if not self._client.bucket_exists(bucket_name):
            self._client.make_bucket(bucket_name)

        self._client.fput_object(bucket_name, object_name, source_path)

    @staticmethod
    def _interpret_object_path(object_path: str) -> tuple[str, str]:
        bucket_name, object_name = object_path.split("/", maxsplit=1)
        return bucket_name, object_name
