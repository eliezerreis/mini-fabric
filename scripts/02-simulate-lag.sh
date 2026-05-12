#!/usr/bin/env bash
# Demonstrates consumer lag:
#   1. Sends a burst of 200 events to profile.events
#   2. Then checks consumer lag via Kafka tooling
#
# To build more visible lag, restart profile-consumer with SIMULATE_SLOW_MS=500
#   docker compose stop profile-consumer
#   SIMULATE_SLOW_MS=500 docker compose up -d profile-consumer

set -euo pipefail

API="${PROFILE_API_URL:-http://localhost:8080}"
BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
COUNT="${1:-200}"

echo "Sending burst of $COUNT events to build consumer lag..."
curl -s -X POST "$API/simulate/burst?count=$COUNT" | python3 -m json.tool

echo ""
echo "Checking consumer lag for group profile-consumer-group..."
kafka-consumer-groups.sh \
  --bootstrap-server "$BROKER" \
  --describe \
  --group profile-consumer-group

echo ""
echo "Open Kafka UI → http://localhost:8090 → Consumer Groups → profile-consumer-group"
echo "You will see LAG per partition in real-time."
