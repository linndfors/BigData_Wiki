#!/bin/bash

docker run -it --rm --network bigdata_wiki_spark-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-topics.sh --create  --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic input
docker run -it --rm --network bigdata_wiki_spark-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-topics.sh --create  --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic processed

sleep 10

docker exec -it bigdata_wiki-kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092
