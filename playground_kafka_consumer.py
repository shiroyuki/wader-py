import logging
import os

from imagination.debug import get_logger
from minio import Minio

from wader.kafka.consumer import Consumer
from wader.storages.manager import StorageManager
from wader.storages.minio_storage import MinIOStorage

log = get_logger('playground_kafka_consumer', logging.INFO)

minio = Minio(os.getenv('MINIO_ENDPOINT') or 'localhost:9000',
              access_key=os.environ['MINIO_ACCESS_KEY'],
              secret_key=os.environ['MINIO_SECRET_KEY'],
              secure=False)

(
    Consumer(StorageManager({'s3': MinIOStorage(minio)}))
    .on('wader-alpha', lambda job: log.info(f'T/{job.topic}: job = {job}'))
    .on('wader-bravo', lambda job: log.info(f'T/{job.topic}: job = {job}'))
    .on('wader-charlie', lambda job: log.info(f'T/{job.topic}: job = {job}'))
    .run(
        servers=['localhost:11434'],
        topics=['wader-alpha', 'wader-bravo', 'wader-charlie'],
        group_id='wader-playground'
    )
)
