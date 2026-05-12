#!/usr/bin/env bash
# Demonstrates poison message handling:
#   1. Sends a message that will fail consumer processing
#   2. Watches it flow: main topic → retry-0 → retry-1 → dlq
#
# What to watch:
#   - profile-consumer logs: ERROR ... attempt 1/2/3
#   - dlq-consumer logs: DLQ event received
#   - Kafka UI: check profile.events.dlq topic for the message

set -euo pipefail

API="${PROFILE_API_URL:-http://localhost:8080}"
PROFILE_ID="${1:-poison-demo-$(date +%s)}"

echo "Injecting poison message for profileId=$PROFILE_ID ..."
curl -s -X POST "$API/simulate/poison?profileId=$PROFILE_ID" | python3 -m json.tool

echo ""
echo "Timeline (watch logs):"
echo "  t+0s  → profile-consumer: attempt 1 → FAIL"
echo "  t+2s  → profile-consumer: attempt 2 on profile.events-retry-0 → FAIL"
echo "  t+6s  → profile-consumer: attempt 3 on profile.events-retry-1 → FAIL"
echo "  t+6s  → dlq-consumer: persists failure to MongoDB"
echo ""
echo "Verify in MongoDB: db.failed_events.find({profileId: '$PROFILE_ID'})"
