from wader.storages.storage import Storage


class StorageManager:
    def __init__(self, storages: dict[str, Storage]):
        self._storages = storages

    def register(self, storage_id, storage: Storage):
        self._storages[storage_id] = storage

    def get(self, storage_id: str) -> Storage:
        return self._storages[storage_id]
