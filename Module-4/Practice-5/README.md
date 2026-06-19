### Описание проекта
Проект состоит из трех-нодового кластера кафки, ui для кафки, инстанса postgres, конектора для кафки, прометеуса и графаны, а также приложения собраного в отдельный образ докера. Все поднимается через docker-compose

Код приложения размещен в Practice5\cmd\. 
### Запуск проекта

- Скачиваем Self-Hosted JDBC Connector с сайта https://debezium.io/documentation/reference/stable/install.html
- Распаковываем в confluent-hub-components\
- `docker-compose build` 
  Собираем проект
- `docker-compose up -d`
  Запускаем кластер и приложение
- `docker ps`
  Проверяем, что все образы были подняты

##### Подготовка БД
- ` docker exec -it postgres psql -h 127.0.0.1 -U postgres-user -d customers`
Подключаемся к консоли БД 

- Создаем таблицы
```
psql (16.4 (Debian 16.4-1.pgdg110+2))
Type "help" for help.

customers=# CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE
CREATE TABLE
customers=# 
```

##### Настройка конектора
- Проверяем наличие коннектора
`curl localhost:8083/connector-plugins`

```[
    {
        "class": "io.debezium.connector.postgresql.PostgresConnector",
        "type": "source",
        "version": "3.5.2.Final"
    },
  ***
]
```

- Добавляем коннектор
`curl -X PUT -H 'Content-Type: application/json' --data @connector-config.json http://localhost:8083/connectors/pg-connector/config`

- Проверяем статус конектора
`curl -s -X GET "http://localhost:8083/connectors/pg-connector/status"`
```
{
    "name": "pg-connector",
    "connector": {
        "state": "RUNNING",
        "worker_id": "localhost:8083"
    },
    "tasks": [
        {
            "id": 0,
            "state": "RUNNING",
            "worker_id": "localhost:8083"
        }
    ],
    "type": "source"
}
```

##### Добавляем данные в БД
- Через консоль БД добавляем строки

```
customers=# -- Добавление пользователей
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');

-- Добавление заказов
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4);
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
customers=# 
```

##### Запуск Consumer
Указываем имя брокера
`docker exec -it practice-5-practice-1 consumer kafka-0:9092`
```
Консьюмер создан rdkafka#consumer-1
% Получено сообщение в топик customers.public.users[0]@1:
{UserID:25 Name:John Doe Email:john@example.com CreateAt:1781811906756838}
% Заголовки: [__debezium.context.connectorLogicalName="customers" __debezium.context.taskId="0" __debezium.context.connectorName="postgresql" __debezium.context.runId="019edc3f-fa39-77b5-bc6b-a14514f73fe6"]
% Получено сообщение в топик customers.public.users[4]@1:
{UserID:27 Name:Alice Johnson Email:alice@example.com CreateAt:1781811906759312}
% Заголовки: [__debezium.context.connectorLogicalName="customers" __debezium.context.taskId="0" __debezium.context.connectorName="postgresql" __debezium.context.runId="019edc3f-fa39-77b5-bc6b-a14514f73fe6"]
% Получено сообщение в топик customers.public.users[7]@3:
{UserID:26 Name:Jane Smith Email:jane@example.com CreateAt:1781811906758152}
% Заголовки: [__debezium.context.connectorLogicalName="customers" __debezium.context.taskId="0" __debezium.context.connectorName="postgresql" __debezium.context.runId="019edc3f-fa39-77b5-bc6b-a14514f73fe6"]
% Получено сообщение в топик customers.public.users[7]@4:
{UserID:28 Name:Bob Brown Email:bob@example.com CreateAt:1781811906760127}
% Заголовки: [__debezium.context.connectorLogicalName="customers" __debezium.context.taskId="0" __debezium.context.connectorName="postgresql" __debezium.context.runId="019edc3f-fa39-77b5-bc6b-a14514f73fe6"]
```


#### Графана
Графики доступны 

http://localhost:3000/d/kafka-connect-overview-0/kafka-connect-overview-0?orgId=1&from=1781722179728&to=1781725779728


#### Описание параметров конектора

Параметры для конектора находятся в `Practice-5\connector-config.json`

    Тип конектора
   "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
   Параметры подключения к БД
   "database.hostname": "",
   "database.port": "",
   "database.user": "",
   "database.password": "",
   "database.dbname": "",
   "database.server.name": "",
   "table.whitelist": "public.customers",
   Настройка списка таблиц, с которыми будет работать debezium
   "table.include.list": "public.users,public.orders",
   Настройка трасформеров
   "transforms": "unwrap",
   "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
   "transforms.unwrap.drop.tombstones": "false",
   "transforms.unwrap.delete.handling.mode": "rewrite",
   Префикс топика
   "topic.prefix": "customers",
   Автосоздание топика
   "topic.creation.enable": "true",
   Настройка репликации и партиций
   "topic.creation.default.replication.factor": "2",
   "topic.creation.default.partitions": "9",
   "skipped.operations": "none",
   Настройка конверторов для ключей и значений
   "key.converter": "org.apache.kafka.connect.json.JsonConverter",
   "key.converter.schemas.enable": false,
   "value.converter": "org.apache.kafka.connect.json.JsonConverter",
   "value.converter.schemas.enable": false