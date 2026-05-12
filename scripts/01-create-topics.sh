#!/usr/bin/env bash
# Run from project root after kafka is up: ./scripts/01-create-topics.sh

set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "Creating topics on $BROKER ..."

kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic profile.events --partitions 6 --replication-factor 1

kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic profile.events-retry-0 --partitions 6 --replication-factor 1

kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic profile.events-retry-1 --partitions 6 --replication-factor 1

kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic profile.events.dlq --partitions 3 --replication-factor 1

echo ""
echo "Topics:"
kafka-topics.sh --bootstrap-server "$BROKER" --list
