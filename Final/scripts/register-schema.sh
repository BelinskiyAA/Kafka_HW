#!/bin/bash

# Настройки
SCHEMA_REGISTRY_URL="http://localhost:8081"
SUBJECTS=("product-value" "filter-product-value" "event-value")
SCHEMA_FILES=("./schema/product.avsc" "./schema/product_row.avsc" "./schema/event.avsc")

for i in "${!SUBJECTS[@]}"; do
    SUBJECT="${SUBJECTS[$i]}"
    SCHEMA_FILE="${SCHEMA_FILES[$i]}"

    if [ ! -f "$SCHEMA_FILE" ]; then
        echo "Ошибка: файл $SCHEMA_FILE не найден."
        exit 1
    fi

    # Читаем схему, удаляем \r, заменяем \n на пробел,
    # экранируем обратные слэши и двойные кавычки для JSON
    SCHEMA_STRING=$(cat "$SCHEMA_FILE" \
        | tr -d '\r' \
        | tr '\n' ' ' \
        | sed 's/\\/\\\\/g' \
        | sed 's/"/\\"/g')

    PAYLOAD="{\"schema\":\"$SCHEMA_STRING\"}"

    RESPONSE=$(curl -s -X POST "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "$PAYLOAD")

    if echo "$RESPONSE" | grep -q '"id"'; then
        SCHEMA_ID=$(echo "$RESPONSE" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
        echo "Схема успешно зарегистрирована!"
        echo "   Subject: $SUBJECT"
        echo "   Schema ID: $SCHEMA_ID"
    else
        echo "Ошибка при регистрации схемы для $SUBJECT:"
        echo "$RESPONSE"
        exit 1
    fi
done