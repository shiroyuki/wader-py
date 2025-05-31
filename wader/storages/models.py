from pydantic import BaseModel


class ObjectInfo(BaseModel):
    version: str = '1.0'
    storage_id: str
    mime_type: str
    object_path: str