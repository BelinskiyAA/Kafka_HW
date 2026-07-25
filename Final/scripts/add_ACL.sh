#!/bin/bash

# Пользователь producer: WRITE и DESCRIBE на топик product (первый кластер)
docker exec -it kafka-1st-1 kafka-acls --bootstrap-server kafka-1st-1:1092 \
  --command-config /etc/kafka/secrets/client.properties \
  --add --allow-principal "User:producer" \
  --topic product --operation WRITE --operation DESCRIBE

# Пользователь consumer: WRITE и DESCRIBE на топик event (первый кластер)
docker exec -it kafka-1st-1 kafka-acls --bootstrap-server kafka-1st-1:1092 \
  --command-config /etc/kafka/secrets/client.properties \
  --add --allow-principal "User:consumer" \
  --topic event --operation WRITE --operation DESCRIBE