import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from pymongo import MongoClient


class MongoHandler(logging.NullHandler):
    def __init__(self, *args, **kwargs):
        self.client = kwargs.pop('client', MongoClient())
        self.database = self.client.get_database(kwargs.pop('db', 'logs'))
        self.collection = self.database.get_collection('collection', 'unspecified')
        self.executor = ThreadPoolExecutor(thread_name_prefix='MongoHandler')
        super().__init__(*args, **kwargs)
        self.setLevel(logging.INFO)

    def _worker(self, record: logging.LogRecord):
        self.collection.insert_one({'level': record.levelname, 'message': record.msg,
                                    'created_at': datetime.utcfromtimestamp(record.created),
                                    'logger': record.name})

    def handle(self, record: logging.LogRecord):
        self.executor.submit(self._worker, record)
