__author__ = "Baishali Dutta"
__copyright__ = "Copyright (C) 2022 Baishali Dutta"
__license__ = "Apache License 2.0"
__version__ = "0.1"

import logging
import random
import string
import time

from confluent_kafka import Consumer
from elasticsearch import Elasticsearch

INDEX_NAME = "system-logs-zookeeper-dummy"


class Elastic:

    def __init__(self):
        self.host = "localhost"
        self.port = 9200
        self.es = None
        self.connect()
        self.index = INDEX_NAME

    def connect(self):
        self.es = Elasticsearch([{'host': self.host, 'port': self.port}])
        if self.es.ping():
            print("Elasticsearch connected successfully")
        else:
            print("Not connected")

    def create_index(self):
        if self.es.indices.exists(self.index):
            print("Deleting '%s' index..." % (self.index))
            res = self.es.indices.delete(index=self.index)
            print(" response: '%s'" % (res))
            request_body = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
            print("Creating '%s' index..." % (self.index))
            res = self.es.indices.create(index=self.index, body=request_body, ignore=400)
            print(" Response: '%s'" % (res))

    def push_to_index(self, message):
        try:
            response = self.es.index(
                index=INDEX_NAME,
                doc_type="log",
                document=message
            )
            print("Write response is :: {}\n\n".format(response))
        except Exception as e:
            print("Exception is :: {}".format(str(e)))


class KafkaConsumer:
    def __init__(self, topic):
        letters = string.ascii_lowercase
        self.group = ''.join(random.choice(letters) for i in range(10))
        self.clientId = ''.join(random.choice(letters) for i in range(10))
        try:
            self.con = Consumer({"bootstrap.servers": "localhost:9092",
                                 "broker.address.family": "v4",
                                 "group.id": self.group,
                                 "client.id": self.clientId,
                                 "default.topic.config": {"auto.offset.reset": "earliest"}
                                 })
            self.con.subscribe([topic])
        except Exception as _:
            logging.exception("Couldn't create the consumer")
        self.topic = topic

    def read_messages(self):
        try:
            msg = self.con.poll()
            if msg is None:
                return 0
            elif msg.error():
                return 0
            return msg
        except Exception as e:
            print("Exception during reading message :: {}".format(e))
            return 0


if __name__ == '__main__':
    es_obj = Elastic()
    es_obj.create_index()
    con = KafkaConsumer("data_log")
    while True:
        message = con.read_messages()
        if not message:
            continue
        time.sleep(0.4)
        message = message.value().decode('utf-8')
        es_obj.push_to_index(message)

    con.close()
