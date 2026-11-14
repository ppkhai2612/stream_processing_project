#!/usr/bin/env bash
# Create (or recreate) the load-test topic with a chosen partition count.
# Usage: ./setup_topic.sh [topic] [partitions] [replication-factor]
set -euo pipefail

TOPIC="${1:-loadtest}"
PARTITIONS="${2:-6}"
REPLICATION="${3:-1}"

echo "Deleting topic '${TOPIC}' if it exists..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic "${TOPIC}" 2>/dev/null || true

sleep 2

echo "Creating topic '${TOPIC}' with ${PARTITIONS} partitions, RF=${REPLICATION}..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic "${TOPIC}"