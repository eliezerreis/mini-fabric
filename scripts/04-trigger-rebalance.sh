#!/usr/bin/env bash
# Demonstrates Kafka consumer group rebalance:
#   Scales profile-consumer replicas up then back down.
#   Each change forces a rebalance — watch logs for REVOKED/ASSIGNED messages.
#
# Requires Docker Compose to be running.

set -euo pipefail

echo "=== Rebalance Demo ==="
echo ""
echo "Step 1: current consumer group state"
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group profile-consumer-group 2>/dev/null || true

echo ""
echo "Step 2: scaling profile-consumer to 3 replicas..."
echo "  Watch logs: docker compose logs -f profile-consumer"
docker compose up --scale profile-consumer=3 -d --no-recreate profile-consumer

echo "  Sleeping 10s for rebalance to complete..."
sleep 10

echo ""
echo "Step 3: consumer group after scale-up"
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group profile-consumer-group 2>/dev/null || true

echo ""
echo "Step 4: scaling back to 1 (triggers another rebalance)..."
docker compose up --scale profile-consumer=1 -d profile-consumer

echo ""
echo "Look for REBALANCE: partitions REVOKED/ASSIGNED lines in profile-consumer logs."
echo "Note: during rebalance the consumer group stops processing — that is the lag window."
