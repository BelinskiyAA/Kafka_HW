#!/usr/bin/env bash

set -e

# Адрес REST API Kafka Connect (можно переопределить переменной окружения)
CONNECT_URL="${CONNECT_URL:-http://kafka-connect-hdfs:8083}"
# Путь к JSON-конфигу (первый аргумент или значение по умолчанию)
CONFIG_FILE="/scripts/analytics-hdfs-sink.json"

# Проверка существования файла конфигурации
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Файл конфигурации $CONFIG_FILE не найден!"
    exit 1
fi

# Ожидание готовности Kafka Connect
echo "Ожидание готовности Kafka Connect по адресу $CONNECT_URL ..."
while ! curl -s "$CONNECT_URL/connectors" > /dev/null; do
    sleep 2
done
echo "Kafka Connect готов."

# Извлечение имени коннектора из JSON-файла
CONNECTOR_NAME=$(grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "$CONFIG_FILE" | head -1 | sed 's/.*"\(.*\)"/\1/')
if [ -z "$CONNECTOR_NAME" ]; then
    echo "Не удалось извлечь имя коннектора из $CONFIG_FILE"
    exit 1
fi

# Регистрация коннектора
echo "Регистрация коннектора $CONNECTOR_NAME..."
RESPONSE=$(curl -s -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$CONFIG_FILE")

# Проверка результата
if echo "$RESPONSE" | grep -q '"error_code"'; then
    echo "Ошибка при создании коннектора:"
    echo "$RESPONSE"
    exit 1
else
    echo "Коннектор успешно зарегистрирован:"
    echo "$RESPONSE"
fi

# Вывод статуса коннектора (с форматированием через python, если доступен)
echo
if command -v python3 >/dev/null 2>&1; then
    curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool
else
    curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
fi