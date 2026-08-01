#!/usr/bin/env bash
set -euo pipefail

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic crypto.market-trades.raw.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=259200000 \
  --if-not-exists

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic crypto.market-trades.dlq.v1 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9092
