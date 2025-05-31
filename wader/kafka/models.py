from typing import Any, Optional, Callable
from uuid import uuid4

from pydantic import BaseModel, Field

from wader.storages.models import ObjectInfo


class Job(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex)
    topic: Optional[str] = None
    params: dict[str, Any] = Field(default_factory=dict)
    blobs: dict[str, ObjectInfo] = Field(default_factory=dict)


class Execution(BaseModel):
    id: str
    job_id: str
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    output: Any
    error: Any = None
    next_jobs: list[Job] = Field(default_factory=list)
    block_next_handler: bool = False


type HandlingLambda = Callable[[Job], Execution | None]
