#!/usr/bin/env bash
#
# Reproduces FENCED_LEADER_EPOCH deterministically.
#
# 1. Seeds data (all batches written with leader epoch 0)
# 2. Stops a broker to force leader elections (epoch bumps to 1)
# 3. Restarts the broker
# 4. Starts consumer — reads batches with epoch 0, caches it,
#    next fetch sends stale epoch 0 -> broker rejects (current is 1)
#
set -euo pipefail

echo "=== Step 0: Set up Docker Compose ==="
docker compose up -d
docker exec kafka-client npm install

echo "=== Step 1: Seed data ==="
docker exec kafka-client node seed.mjs

echo ""
echo "=== Step 2: Stop kafka2 to force leader elections ==="
docker compose stop kafka2
echo "Waiting 15s for failover..."
sleep 15

echo ""
echo "=== Step 3: Restart kafka2 ==="
docker compose start kafka2
echo "Waiting 10s for broker to rejoin..."
sleep 10

echo ""
echo "=== Step 4: Start consumer (should crash with FENCED_LEADER_EPOCH) ==="
docker exec kafka-client node consume.mjs

echo ""
echo "=== Step 5: Apply fix and consume again (should succeed) ==="
echo "run this manually: exec kafka-client node consume-patched.mjs"
echo "this will patch the library, so you'll have to rm -rf node_modules and npm install again to restore original behavior"
