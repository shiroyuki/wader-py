from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def get(self, object_path: str) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def put(self, object_path: str, value: bytes):
        raise NotImplementedError()

    @abstractmethod
    def upload(self, object_path: str, source_path: str):
        raise NotImplementedError()