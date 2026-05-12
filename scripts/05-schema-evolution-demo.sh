#!/usr/bin/env bash
# Demonstrates schema evolution with Confluent Schema Registry:
#   1. Register v1 schema
#   2. Verify v2 is backward-compatible
#   3. Register v2 schema
#   4. Send a MERGED event (v2-only) — v1 consumers will read it as UPDATED

set -euo pipefail

MIGRATOR="${SCHEMA_MIGRATOR_URL:-http://localhost:8083}"
API="${PROFILE_API_URL:-http://localhost:8080}"

echo "=== Schema Evolution Demo ==="
echo ""

echo "Step 1: Register v1 schema"
curl -s -X POST "$MIGRATOR/schemas/v1/register" | python3 -m json.tool

echo ""
echo "Step 2: Check v2 backward-compatibility before registering"
curl -s -X POST "$MIGRATOR/schemas/v2/check" | python3 -m json.tool

echo ""
echo "Step 3: Register v2 schema"
curl -s -X POST "$MIGRATOR/schemas/v2/register" | python3 -m json.tool

echo ""
echo "Step 4: List registered versions"
curl -s "$MIGRATOR/schemas/versions" | python3 -m json.tool

echo ""
echo "Step 5: Send v2-only MERGED event"
curl -s -X POST "$API/simulate/merge?targetProfileId=profile-1&sourceProfileId=profile-2" | python3 -m json.tool

echo ""
echo "What to observe:"
echo "  - v2 consumer (profile-consumer) handles EventType.MERGED and stores mergedFromId"
echo "  - v1 consumer would receive EventType.MERGED as the enum default (UPDATED)"
echo "  - Schema Registry enforced BACKWARD compatibility at registration time"
