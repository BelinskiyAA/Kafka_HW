## Задание.
Освоить балансировку партиций и распределение нагрузки с помощью Partition Reassignment Tools.
Попрактиковаться в диагностике и устранении проблем кластера.

1. Создайте новый топик balanced_topic с 8 партициями и фактором репликации 3.

`docker exec -it kafka-0 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic balanced_topic --partitions 8 --replication-factor 3`
Вывод

> WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.

> Created topic balanced_topic.

2. Определите текущее распределение партиций.

`docker exec -it kafka-0 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic balanced_topic`

```
Topic: balanced_topic   TopicId: caPdQ-4jTdOaFA6fsHxk0A PartitionCount: 8       ReplicationFactor: 3    Configs: 
        Topic: balanced_topic   Partition: 0    Leader: 2       Replicas: 2,0,1 Isr: 2,0,1
        Topic: balanced_topic   Partition: 1    Leader: 0       Replicas: 0,1,2 Isr: 0,1,2
        Topic: balanced_topic   Partition: 2    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0
        Topic: balanced_topic   Partition: 3    Leader: 2       Replicas: 2,0,1 Isr: 2,0,1
        Topic: balanced_topic   Partition: 4    Leader: 0       Replicas: 0,1,2 Isr: 0,1,2
        Topic: balanced_topic   Partition: 5    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0
        Topic: balanced_topic   Partition: 6    Leader: 0       Replicas: 0,2,1 Isr: 0,2,1
        Topic: balanced_topic   Partition: 7    Leader: 2       Replicas: 2,1,0 Isr: 2,1,0
```

3. Создайте JSON-файл reassignment.json для перераспределения партиций.
4. Перераспределите партиции.
```
docker exec -it kafka-0   kafka-reassign-partitions.sh \
--bootstrap-server localhost:9092 \
--broker-list "1,2,3" \
--topics-to-move-json-file "/tmp/reassignment.json" \
--generate
```

Вывод
```
Current partition replica assignment
{"version":1,"partitions":[]}

Proposed partition reassignment configuration
{"version":1,"partitions":[]}
```

Команда
```
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file /tmp/reassignment.json --execute
```
Вывод
```
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file /tmp/reassignment.json --execute
Current partition replica assignment

{"version":1,"partitions":[{"topic":"balanced_topic","partition":0,"replicas":[2,0,1],"log_dirs":["any","any","any"]},{"topic":"balanced_topic","partition":1,"replicas":[0,1,2],"log_dirs":["any","any","any"]},{"topic":"balanced_topic","partition":2,"replicas":[1,2,0],"log_dirs":["any","any","any"]},{"topic":"balanced_topic","partition":3,"replicas":[2,0,1],"log_dirs":["any","any","any"]},{"topic":"balanced_topic","partition":4,"replicas":[0,1,2],"log_dirs":["any","any","any"]},{"topic":"balanced_topic","partition":5,"replicas":[1,2,0],"log_dirs":["any","any","any"]},{"topic":"balanced_topic","partition":6,"replicas":[0,2,1],"log_dirs":["any","any","any"]},{"topic":"balanced_topic","partition":7,"replicas":[2,1,0],"log_dirs":["any","any","any"]}]}

Save this to use as the --reassignment-json-file option during rollback
Successfully started partition reassignments for balanced_topic-0,balanced_topic-1,balanced_topic-2,balanced_topic-3,balanced_topic-4,balanced_topic-5,balanced_topic-6,balanced_topic-7
```
5. Проверьте статус перераспределения.
`docker exec -it kafka-0  kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file /tmp/reassignment.json --verify`
Вывод
```
Status of partition reassignment:
Reassignment of partition balanced_topic-0 is completed.
Reassignment of partition balanced_topic-1 is completed.
Reassignment of partition balanced_topic-2 is completed.
Reassignment of partition balanced_topic-3 is completed.
Reassignment of partition balanced_topic-4 is completed.
Reassignment of partition balanced_topic-5 is completed.
Reassignment of partition balanced_topic-6 is completed.
Reassignment of partition balanced_topic-7 is completed.

Clearing broker-level throttles on brokers 0,1,2
Clearing topic-level throttles on topic balanced_topic
```

6. Убедитесь, что конфигурация изменилась.
`docker exec -it kafka-0 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic balanced_topic`

Вывод
```
Topic: balanced_topic   TopicId: caPdQ-4jTdOaFA6fsHxk0A PartitionCount: 8       ReplicationFactor: 3    Configs: 
        Topic: balanced_topic   Partition: 0    Leader: 2       Replicas: 0,1,2 Isr: 2,0,1
        Topic: balanced_topic   Partition: 1    Leader: 0       Replicas: 2,0,1 Isr: 0,1,2
        Topic: balanced_topic   Partition: 2    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0
        Topic: balanced_topic   Partition: 3    Leader: 2       Replicas: 0,1,2 Isr: 2,0,1
        Topic: balanced_topic   Partition: 4    Leader: 0       Replicas: 1,2,0 Isr: 0,1,2
        Topic: balanced_topic   Partition: 5    Leader: 1       Replicas: 2,0,1 Isr: 1,2,0
        Topic: balanced_topic   Partition: 6    Leader: 0       Replicas: 2,1,0 Isr: 0,2,1
        Topic: balanced_topic   Partition: 7    Leader: 2       Replicas: 0,2,1 Isr: 2,1,0
```

`docker exec -it kafka-0 `

7. Смоделируйте сбой брокера:
  - Остановите брокер kafka-1.
    `docker stop kafka-1`
  - Проверьте состояние топиков после сбоя.
  `docker exec -it kafka-0 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic balanced_topic`

  Вывод
  ```
  Topic: balanced_topic   TopicId: caPdQ-4jTdOaFA6fsHxk0A PartitionCount: 8       ReplicationFactor: 3    Configs: 
          Topic: balanced_topic   Partition: 0    Leader: 0       Replicas: 0,1,2 Isr: 2,0
          Topic: balanced_topic   Partition: 1    Leader: 2       Replicas: 2,0,1 Isr: 0,2
          Topic: balanced_topic   Partition: 2    Leader: 2       Replicas: 1,2,0 Isr: 2,0
          Topic: balanced_topic   Partition: 3    Leader: 0       Replicas: 0,1,2 Isr: 2,0
          Topic: balanced_topic   Partition: 4    Leader: 2       Replicas: 1,2,0 Isr: 0,2
          Topic: balanced_topic   Partition: 5    Leader: 2       Replicas: 2,0,1 Isr: 2,0
          Topic: balanced_topic   Partition: 6    Leader: 2       Replicas: 2,1,0 Isr: 0,2
          Topic: balanced_topic   Partition: 7    Leader: 0       Replicas: 0,2,1 Isr: 2,0
  ```
  - Запустите брокер заново.
    `docker compose up kafka-1 -d`
  - Проверьте, восстановилась ли синхронизация реплик.
    ```
    Topic: balanced_topic   TopicId: caPdQ-4jTdOaFA6fsHxk0A PartitionCount: 8       ReplicationFactor: 3    Configs: 
        Topic: balanced_topic   Partition: 0    Leader: 0       Replicas: 0,1,2 Isr: 2,0,1
        Topic: balanced_topic   Partition: 1    Leader: 2       Replicas: 2,0,1 Isr: 0,2,1
        Topic: balanced_topic   Partition: 2    Leader: 1       Replicas: 1,2,0 Isr: 2,0,1
        Topic: balanced_topic   Partition: 3    Leader: 0       Replicas: 0,1,2 Isr: 2,0,1
        Topic: balanced_topic   Partition: 4    Leader: 1       Replicas: 1,2,0 Isr: 0,2,1
        Topic: balanced_topic   Partition: 5    Leader: 2       Replicas: 2,0,1 Isr: 2,0,1
        Topic: balanced_topic   Partition: 6    Leader: 2       Replicas: 2,1,0 Isr: 0,2,1
        Topic: balanced_topic   Partition: 7    Leader: 0       Replicas: 0,2,1 Isr: 2,0,1
        ```