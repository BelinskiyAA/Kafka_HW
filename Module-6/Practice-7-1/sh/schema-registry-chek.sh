#!/usr/bin/env bash

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-https://rc1b-e5iq89a7c4jucj5p.mdb.yandexcloud.net:443}"
SUBJECT="${SUBJECT:-practice7-value}"
KAFKA_USERNAME="${KAFKA_USERNAME:-practice7}"
KAFKA_PASS="${KAFKA_PASS:-practice7}"
CA_CERT="${CA_CERT:-/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt}"

echo "GET /subjects"
curl -sS --cacert "${CA_CERT}" -u "${KAFKA_USERNAME}:${KAFKA_PASS}" "${SCHEMA_REGISTRY_URL}/subjects"
echo "\n\n"
echo "GET /subjects/${SUBJECT}/versions"
curl -sS --cacert "${CA_CERT}" -u "${KAFKA_USERNAME}:${KAFKA_PASS}" "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions"
echo