import json
from abc import abstractmethod, ABC
from collections import defaultdict
from typing import Optional

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from imagination.debug import get_logger_for
from opentelemetry import trace

from wader.helpers.dev import get_fqcn_of
from wader.kafka.models import HandlingLambda, Job, Execution


class Handler(ABC):
    @abstractmethod
    def handle(self, job: Job) -> Execution | None:
        raise NotImplementedError()


class KillOrder(RuntimeError):
    """ This error/exception is used to immediately terminate the process if possible. """


class StopOrder(RuntimeError):
    """ This error/exception is used to cleanly stop the consumption loop. """


class Consumer:
    def __init__(self):
        self._log = get_logger_for(self)
        self._topic_handlers: dict[str, list[Handler | HandlingLambda]] = defaultdict(list)

    def set_logger(self, log):
        self._log = log

    def on(self, topic: str, handle: Handler | HandlingLambda) -> "Consumer":
        self._log.debug(f'T/{topic}: {handle}')
        self._topic_handlers[topic].append(handle)
        return self

    async def run(self, consumer: AIOKafkaConsumer) -> None:
        self._log.debug('Starting...')
        async with consumer:
            self._log.info('Started')
            async for record in consumer:
                try:
                    tracer = trace.get_tracer(get_fqcn_of(self))

                    with tracer.start_as_current_span('consumer'):
                        await self.process_record(record)
                except StopOrder:
                    self._log.warning('The stop order has been received. Stopping...')
                    break
                except KillOrder:
                    self._log.warning('The kill order has been received. Terminating itself...')

                    import sys
                    sys.exit(1)
                except Exception as e:
                    self._log.error('Unexpected exception occurred', exc_info=e)
                # end try
            # end async for
        # end async with
        self._log.info('Stopped')

    async def process_record(self, record: ConsumerRecord):
        self._log.debug(f'Received ({record})')

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




