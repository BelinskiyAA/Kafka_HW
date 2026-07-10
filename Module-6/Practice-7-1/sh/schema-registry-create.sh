#!/usr/bin/env bash

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-https://rc1b-e5iq89a7c4jucj5p.mdb.yandexcloud.net:443}"
SCHEMA_FILE="$(cd "$(dirname "$0")/.." && pwd)/schema/message.avsc"
SUBJECT="${SUBJECT:-practice7-value}"
KAFKA_USERNAME="${KAFKA_USERNAME:-practice7}"
KAFKA_PASS="${KAFKA_PASS:-practice7}"
CA_CERT="${CA_CERT:-/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt}"

curl -sS --cacert "${CA_CERT}" --user "${KAFKA_USERNAME}:${KAFKA_PASS}" \
  -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$(jq -Rs '{schema: .}' < "${SCHEMA_FILE}")" \
  --url "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions"
echo
