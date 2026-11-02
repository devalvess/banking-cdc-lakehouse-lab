#!/bin/bash
# Cria ou atualiza o conector Debezium Db2 no Kafka Connect (idempotente).
set -euo pipefail
cd "$(dirname "$0")"

until curl -sf http://localhost:8083/connectors >/dev/null; do
  echo "aguardando Kafka Connect..."; sleep 5
done

curl -sf -X PUT -H "Content-Type: application/json" \
  --data @db2-core-connector.json \
  http://localhost:8083/connectors/db2-core/config | jq .

sleep 5
curl -s http://localhost:8083/connectors/db2-core/status | jq .
