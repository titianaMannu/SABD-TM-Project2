#!/bin/bash
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-output-topic-query1-weekly
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-output-topic-query1-monthly
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-output-topic-query2-monthly
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-output-topic-query2-weekly

docker-compose down