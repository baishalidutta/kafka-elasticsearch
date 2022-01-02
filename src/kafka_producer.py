import json
import time

from confluent_kafka import Producer


class LogParser:

    @staticmethod
    def fetch_log(file_obj):
        for row in file_obj:
            yield row

    @staticmethod
    def read_log_file():
        filename = "../Zookeeper_2k.log"
        return open(filename, "r")

    @staticmethod
    def serialize_log(log):
        log.strip()
        get_message = log.split(" ")
        if len(get_message):
            message = " ".join(get_message[7:])
            message_type = get_message[3]
            if message_type not in ["INFO", "ERROR", "WARN"]:
                return None
            date = get_message[0][:]
            timestamp = get_message[1]
            datetime = date + " " + timestamp
        log_dict = {
            "message": message.strip(),
            "timestamp": datetime.rpartition(',')[0],
            "type": message_type
        }
        return log_dict


class KafkaProducer:

    def __init__(self):
        self.bootstrap_servers = "localhost:9092"
        self.topic = "data_log"
        try:
            self.p = Producer({"bootstrap.servers": "localhost:9092",
                               "client.id": "elasticsearch-kafka-baishali",
                               "enable.idempotence": True,  # EOS processing
                               "broker.address.family": "v4",
                               "batch.size": 64000,
                               "linger.ms": 10,
                               "acks": "all",  # Wait for the leader and all ISR to send response back
                               "retries": 5,
                               "delivery.timeout.ms": 1000})  # Total time to make retries
        except Exception as _:
            self.p = None

    def produce(self, msg):
        serialized_message = json.dumps(msg)
        self.p.produce(topic=self.topic, value=serialized_message)
        self.p.poll(0)
        self.p.flush()
        time.sleep(1)


if __name__ == '__main__':
    logFile = LogParser.read_log_file()
    logFileGen = LogParser.fetch_log(logFile)
    producer = KafkaProducer()
    while True:
        try:
            data = next(logFileGen)
            serialized_data = LogParser.serialize_log(data)
            print("Message is :: {}".format(serialized_data))
            producer.produce(serialized_data)
        except StopIteration:
            exit()
        except KeyboardInterrupt:
            print("Printing last message before exiting :: {}".format(serialized_data))
            exit()
