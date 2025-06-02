import asyncio
import logging
import os

from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from imagination.debug import get_logger
from minio import Minio

from wader.kafka.consumer import Consumer

load_dotenv()

log = get_logger('playground_kafka_consumer', logging.INFO)

async def main():
    consumer = AIOKafkaConsumer(
        *['wader-alpha', 'wader-bravo', 'wader-charlie'],
        bootstrap_servers=['localhost:9092'],
        group_id='wader-playground',
    )

    await Consumer() \
        .on('wader-alpha', lambda job: log.info(f'T/{job.topic}: job = {job}')) \
        .on('wader-bravo', lambda job: log.info(f'T/{job.topic}: job = {job}')) \
        .on('wader-charlie', lambda job: log.info(f'T/{job.topic}: job = {job}')) \
        .run(consumer)


asyncio.run(main())
