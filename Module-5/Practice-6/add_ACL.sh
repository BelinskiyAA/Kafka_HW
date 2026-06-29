#!/bin/bash

# topic-1: producer WRITE+DESCRIBE
docker exec -it practice-6-kafka-1-1 kafka-acls --bootstrap-server kafka-1:1092 --command-config /etc/kafka/secrets/client.properties --add --allow-principal "User:producer" --topic topic-1 --operation WRITE --operation DESCRIBE

# topic-1: consumer READ+DESCRIBE
docker exec -it practice-6-kafka-1-1 kafka-acls --bootstrap-server kafka-1:1092 --command-config /etc/kafka/secrets/client.properties --add --allow-principal "User:consumer" --topic topic-1 --operation READ --operation DESCRIBE

# topic-2: only producer WRITE+DESCRIBE
docker exec -it practice-6-kafka-1-1 kafka-acls --bootstrap-server kafka-1:1092 --command-config /etc/kafka/secrets/client.properties --add --allow-principal "User:producer" --topic topic-2 --operation WRITE --operation DESCRIBE