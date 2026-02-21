#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8000}

printf "== POST /transaction (multiple messages) ==\n"
curl -sS -X POST "$BASE_URL/transaction" -H "Content-Type: application/json" \
  -d '{"user_id":"UserA","amount":100}' | tee /dev/stderr
printf "\n"

curl -sS -X POST "$BASE_URL/transaction" -H "Content-Type: application/json" \
  -d '{"user_id":"UserA","amount":-25}' | tee /dev/stderr
printf "\n"

curl -sS -X POST "$BASE_URL/transaction" -H "Content-Type: application/json" \
  -d '{"user_id":"UserB","amount":50}' | tee /dev/stderr
printf "\n\n"

printf "== GET /user/UserA ==\n"
curl -sS "$BASE_URL/user/UserA" | tee /dev/stderr
printf "\n\n"

printf "== GET /user/UserB ==\n"
curl -sS "$BASE_URL/user/UserB" | tee /dev/stderr
printf "\n\n"

printf "== GET /accounts ==\n"
curl -sS "$BASE_URL/accounts" | tee /dev/stderr
printf "\n\n"

printf "== GET /metrics ==\n"
curl -sS "$BASE_URL/metrics" | tee /dev/stderr
printf "\n"
