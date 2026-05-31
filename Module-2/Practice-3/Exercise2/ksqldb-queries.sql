-- создание исходного потока;
CREATE STREAM messages_stream (
	user_id BIGINT,
    recipient_id BIGINT,
    message STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='messages',   -- Имя топика Kafka
    VALUE_FORMAT='JSON',      -- Формат данных (JSON, AVRO, DELIMITED, и т.д.)
    PARTITIONS=1              -- Число партиций потока (по умолчанию 1)
); 

--таблицы общего количества отправленных сообщений;
SELECT  COUNT(*) as count
FROM messages_stream
EMIT CHANGES; 

--таблицы с числом уникальных получателей для всех сообщений;
SELECT  COUNT_DISTINCT(recipient_id) as count
FROM messages_stream
EMIT CHANGES; 


-- Создайте таблицу user_statistics для агрегирования данных по каждому пользователю:
-- сообщения, отправленные каждым пользователем;
-- количество уникальных получателей для каждого пользователя;
CREATE TABLE user_statistics AS
SELECT  user_id, COUNT_DISTINCT(*) as message_amount, COUNT_DISTINCT(recipient_id) as recipient_amount
FROM messages_stream
GROUP BY user_id
EMIT CHANGES; 