## Задание 2. (дополнительно) Анализ и агрегирование сообщений с помощью ksqlDB

### Запуск проекта

- `docker-compose build && docker-compose up -d` 
  Собираем проект и поднимаем кластер и приложение
- `docker ps`
  Проверяем, что все образы были подняты
- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic messages`
Создаем топики

### Запросы на создание 

`CREATE STREAM messages_stream (
	user_id BIGINT,
    recipient_id BIGINT,
    message STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='messages',   -- Имя топика Kafka
    VALUE_FORMAT='JSON',      -- Формат данных (JSON, AVRO, DELIMITED, и т.д.)
    PARTITIONS=1              -- Число партиций потока (по умолчанию 1)
); `


`
CREATE TABLE user_statistics AS
SELECT  user_id, COUNT_DISTINCT(*) as message_amount, COUNT_DISTINCT(recipient_id) as recipient_amount
FROM messages_stream
GROUP BY user_id
EMIT CHANGES; 
`

Запросы 
общего количества отправленных сообщений;
`SELECT  COUNT(*) as count
FROM messages_stream
EMIT CHANGES; `

числа уникальных получателей;
`SELECT  COUNT_DISTINCT(recipient_id) as count
FROM messages_stream
EMIT CHANGES; `