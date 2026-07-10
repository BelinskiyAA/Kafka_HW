### Задание 1. Развёртывание и настройка Kafka-кластера в Yandex Cloud

Код приложения размещен в practice\cmd\. Каждая папка содержит код продьюсера или консьюмера.
В \sh размещены скрипты для добавления и просмотра схемы в Schema Registry
Описание самой схемы расположено в \schema\message.avsc
Скриншоты расположены в \img\

### Кластер
В облаке развернут кластер кафки из трех брокеров
Парметры кластера можно посмотреть [cluster](img/cluster.png) а также yc_kafka_managed_service_terraform.cfg

Также создан топик. Его параметры указаны в [topic](img/topic.png)

### Настройка Schema Registry
Схема создается через скрипт в [schema-registry-create.sh](sh/schema-registry-create.sh)
Проверяем через скрипт [schema-registry-chek.sh](sh/schema-registry-chek.sh)

Пример работы скриптов показаны в [schema](img/schema.png)

### Запуск проекта

- `docker build -t practice7 .`
Собираем проект

- `docker run -d --rm --name practice7 practice7`
Запускаем проект

- `docker exec -it practice7 producer`
Запуск продьюсера

- `docker exec -it practice7 consumer`
Запуск консьюмера

- Пример работы - [producer и consumer](img/producer_consumer.png).
