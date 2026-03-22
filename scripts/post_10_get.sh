#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8000}

echo "=== POST 10 транзакцій (msg1 - msg10) через facade-service ==="
for i in $(seq 1 10); do
  resp=$(curl -sS -X POST "$BASE_URL/transaction" -H "Content-Type: application/json" \
    -d "{\"user_id\":\"User1\",\"amount\":$i}")
  echo "msg$i: $resp"
done

echo ""
echo "=== GET /user/User1 (читання транзакцій) ==="
curl -sS "$BASE_URL/user/User1" | python3 -m json.tool

echo ""
echo "=== GET /accounts ==="
curl -sS "$BASE_URL/accounts" | python3 -m json.tool
