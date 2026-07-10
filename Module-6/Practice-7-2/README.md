### Задание 2. Интеграция Kafka с внешними системами (Apache NiFi)
В качестве системы для интеграции с Kafka используется Apache NiFi. Producer публикует сообщения в топик input-topic, далее эти сообщения слушает NiFi и записывает их в топик ouput-topic.

Код приложения размещен в practice\cmd\. Каждая папка содержит код продьюсера или консьюмера.
### Запуск проекта

- ` docker compose up --build -d` 
  Собираем  и запускаем проект

- `docker ps`
  Проверяем, что все образы были подняты [docker ps](img/docker-ps.png)
  
- В NiFi настроена связка из двух процессоров - [nifi-process](img/nifi.png).
ConsumeKafkaRecord_2_0 - слушает топик input-topic и получает сообщения.
PublishKafkaRecord_2_0 - публикует полученные сообщения в топик ouput-topic. 
  
##### Запуск продьюсера
Указываем имя брокера, наименование топика
`docker exec -it practice producer kafka-broker-1:9092 input-topic`
[producer](img/producer.png)

##### Запуск консьюмера
Указываем имя брокера, группу и наименование топика
`docker exec -it practice consumer kafka-broker-1:9092 output-topic`
[consumer](img/consumer.png)
