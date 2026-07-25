#!/bin/bash

# Create topics
docker exec -it final-kafka-1st-1-1 kafka-topics --create --topic product --partitions 3 --replication-factor 3 \
--bootstrap-server kafka-1st-1:1092 --command-config '/etc/kafka/secrets/client.properties' --if-not-exists

docker exec -it final-kafka-1st-1-1 kafka-topics --create --topic filter-product --partitions 3 --replication-factor 3 \
--bootstrap-server kafka-1st-1:1092 --command-config '/etc/kafka/secrets/client.properties' --if-not-exists

docker exec -it final-kafka-1st-1-1 kafka-topics --create --topic block-product --partitions 3 --replication-factor 3 \
--bootstrap-server kafka-1st-1:1092 --command-config '/etc/kafka/secrets/client.properties' --if-not-exists


docker exec -it final-kafka-1st-1-1 kafka-topics --create --topic event --partitions 3 --replication-factor 3 \
--bootstrap-server kafka-1st-1:1092 --command-config '/etc/kafka/secrets/client.properties' --if-not-exists
