#!/usr/bin/env bash
set -euo pipefail

set -a
source .env
set +a

envsubst < kafka-connect/connector-config.json \
  | curl -s -X POST -H "Content-Type: application/json" -d @- http://localhost:8083/connectors \
  | jq .
