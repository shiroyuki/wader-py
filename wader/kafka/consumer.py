import json
from abc import abstractmethod, ABC
from collections import defaultdict
from typing import Optional

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from imagination.debug import get_logger_for
from opentelemetry import trace

from wader.helpers.dev import get_fqcn_of
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
        self._log.debug(f'T/{topic}: {handle}')
        self._topic_handlers[topic].append(handle)
        return self

    async def run(self, servers: list[str], topics: list[str], group_id: Optional[str] = None) -> None:
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=servers,
            group_id=group_id,
        )

        self._log.debug('Servers: %s', servers)
        self._log.debug('Topics: %s', topics)
        self._log.debug('Group ID: %s', group_id)

        self._log.debug('Starting...')
        async with consumer:
            self._log.info('Started')
            async for record in consumer:
                tracer = trace.get_tracer(get_fqcn_of(self))

                with tracer.start_as_current_span('consumer'):
                    await self.process_record(record)
            # end async for
        # end async with

    async def process_record(self, record: ConsumerRecord):
        self._log.info(f'Received ({record})')

        if record.topic in self._topic_handlers:
            job = Job(**json.loads(record.value))

            if not job.topic:
                job.topic = record.topic

            # TODO Store the job information in the local database.

            for handle in self._topic_handlers[job.topic]:
                execution = handle.handle(job) if isinstance(handle, Handler) else handle(job)

                # TODO Store the execution information

                # TODO Publish the execution information

                if execution and execution.block_next_handler:
                    self._log.debug(f'Blocked the next handler ({execution})')
                    break
                # end if
            # end for
        else:
            self._log.debug('No handler for %s', record)




