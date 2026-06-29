#!/bin/bash

# Create topics
docker exec -it practice-6-kafka-1-1 kafka-topics --create --topic topic-1 --partitions 3 --replication-factor 3 \
--bootstrap-server kafka-1:1092 --command-config '/etc/kafka/secrets/client.properties' --if-not-exists

docker exec -it practice-6-kafka-1-1 kafka-topics --create --topic topic-2 --partitions 3 --replication-factor 3 \
  --bootstrap-server kafka-1:1092 --command-config /etc/kafka/secrets/client.properties --if-not-exists
