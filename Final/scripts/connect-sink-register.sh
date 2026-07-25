#!/bin/bash

# Адрес REST API Kafka Connect внутри контейнера
CONNECT_URL="http://localhost:8083"

# Путь к JSON-конфигу (монтируется из хоста)
CONFIG_FILE="/scripts/products-jdbc-sink.json"

# Проверяем, что файл конфига существует
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Файл конфигурации $CONFIG_FILE не найден!"
    exit 1
fi

# Ждём, пока Kafka Connect станет доступен
echo "Ожидание готовности Kafka Connect..."
while ! curl -s "$CONNECT_URL/connectors" > /dev/null; do
    sleep 2
done
echo "Kafka Connect готов."

# Проверяем, не зарегистрирован ли уже коннектор
CONNECTOR_NAME=$(grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "$CONFIG_FILE" | head -1 | sed 's/.*"\(.*\)"/\1/')
if curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME" | grep -q '"name"'; then
    echo "Коннектор $CONNECTOR_NAME уже зарегистрирован. Удаляем..."
    curl -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
    sleep 2
fi

# Регистрируем коннектор
echo "Регистрация коннектора $CONNECTOR_NAME..."
RESPONSE=$(curl -s -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$CONFIG_FILE")

# Проверяем результат
if echo "$RESPONSE" | grep -q '"error_code"'; then
    echo "Ошибка при создании коннектора:"
    echo "$RESPONSE"
    exit 1
else
    echo "Коннектор успешно зарегистрирован:"
    echo "$RESPONSE"
fi