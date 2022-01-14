# Kafka Elasticsearch Log Processing

Project for real time log processing using kafka, Python and Elasticsearch

It's assumed that zookeeper, kafka, elasticsearch and Kibana are running in the localhost, it follows this process:

- Randomly pick any log entry from zookeeper log file and send it to a kafka topic
- Read the topic data with several subscribers
- Store the received log entry to elasticsearch
- Open Kibana to visualize log entries

# Usage:

* Create the required topic

```bash
kafka-topics.sh --zookeeper localhost:2181 --topic data_log --create --partitions 3 --replication-factor 1
```

* Check the topics are created

```bash
kafka-topics.sh --zookeeper localhost:2181 --list
```

* Start the producer, run the file

```bash
kafka_producer.py
```

* Start the elasticsearch consumer to store entries to elasticsearch

```bash
es-python.py
```
