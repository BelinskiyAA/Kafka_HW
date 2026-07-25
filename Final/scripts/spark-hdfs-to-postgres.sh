#!/usr/bin/env bash

cd "$(dirname "$0")/.."

echo "Waiting for Spark master ..."
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8087 >/dev/null; then
    break
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "Spark master UI is not ready (is spark-master up?)" >&2
    exit 1
  fi
done

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /apps/jars/spark-avro_2.12-3.5.3.jar,/apps/jars/postgresql-42.7.4.jar \
  /apps/hdfs_to_recommendations.py
