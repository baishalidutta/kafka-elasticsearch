# Kafka Elasticsearch Log Processing

Project for real-time log processing using kafka, Python and Elasticsearch

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
elastic-search.py
```

## Developer

Baishali Dutta (<a href='mailto:me@itsbaishali.com'>me@itsbaishali.com</a>)

## Contribution [![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/baishalidutta/kafka-elasticsearch/issues)

If you would like to contribute and improve the model further, check out the [Contribution Guide](https://github.com/baishalidutta/kafka-elasticsearch/blob/main/CONTRIBUTING.md)