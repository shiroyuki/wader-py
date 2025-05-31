import json
from abc import abstractmethod, ABC
from collections import defaultdict
from typing import Optional

from aiokafka import AIOKafkaConsumer
from imagination.debug import get_logger_for

from wader.kafka.models import HandlingLambda, Job, Execution
from wader.storages.manager import StorageManager


class Handler(ABC):
    @abstractmethod
    def handle(self, job: Job) -> Execution | None:
        raise NotImplementedError()


class Consumer:
    def __init__(self, storages: StorageManager):
        self._log = get_logger_for(self)
        self._storages = storages
        self._topic_handlers: dict[str, list[Handler | HandlingLambda]] = defaultdict(list)

    def on(self, topic: str, handle: Handler | HandlingLambda) -> "Consumer":
        self._topic_handlers[topic].append(handle)
        return self

    async def run(self, servers: list[str], topics: list[str], group_id: Optional[str] = None) -> None:
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=servers,
            group_id=group_id,
        )

        self._log.info('Servers: %s', servers)
        self._log.info('Topics: %s', topics)
        self._log.info('Group ID: %s', group_id)

        async with consumer:
            async for record in consumer:
                if record.topic not in self._topic_handlers:
                    job = Job(**json.loads(record.value))

                    if not job.topic:
                        job.topic = record.topic

                    # TODO Store the job information in the local database.

                    for handle in self._topic_handlers[job.topic]:
                        execution = handle.handle(job) if isinstance(handle, Handler) else handle(job)

                        # TODO Store the execution information

                        # TODO Publish the execution information

                        if execution.block_next_handler:
                            break
                        # end if
                    # end for
                else:
                    self._log.debug('No handler for %s', record)
            # end async for
        # end async with




