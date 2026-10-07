#!/usr/bin/env bash
set -euo pipefail

CB_HOST="${COUCHBASE_HOST:-flight-couchbase-srv}"
CB_PORT="${COUCHBASE_PORT:-8093}"
CB_USER="${COUCHBASE_USER:-Administrator}"
CB_PASS="${COUCHBASE_PASSWORD:?COUCHBASE_PASSWORD is required}"

EXPECTED_AV123="${EXPECTED_AV123:-37}"
EXPECTED_AV456="${EXPECTED_AV456:-63}"
EXPECTED_FC_AV123="${EXPECTED_FC_AV123:-37}"
EXPECTED_FC_AV456="${EXPECTED_FC_AV456:-63}"
TEST_MODE="${TEST_MODE:-initial}"

MAX_WAIT=300
INTERVAL=10

query() {
  local stmt="$1"
  curl -s -u "${CB_USER}:${CB_PASS}" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc --arg s "$stmt" '{statement: $s}')" \
    "http://${CB_HOST}:${CB_PORT}/query/service"
}

echo "Modo: $TEST_MODE"

echo "Esperando Couchbase + keyspace..."
READY=false
for i in $(seq 1 30); do
  RESP=$(query "SELECT COUNT(*) AS c FROM \`flight-reservations\`.\`reservations\`.\`processed\`;" || true)
  if echo "$RESP" | grep -q '"status":"success"'; then
    READY=true; echo "Keyspace listo."; break
  fi
  echo "Intento $i/30: keyspace no disponible, esperando ${INTERVAL}s..."
  sleep "$INTERVAL"
done
if [ "$READY" != "true" ]; then
  echo "ERROR: keyspace no disponible en 300s."
  exit 1
fi

check_condition() {
  local label="$1"
  local stmt="$2"
  local jq_filter="$3"
  local expected="$4"
  local elapsed=0
  while [ "$elapsed" -lt "$MAX_WAIT" ]; do
    RESP=$(query "$stmt")
    VALUE=$(echo "$RESP" | jq -r "$jq_filter // empty")
    if [ "$VALUE" = "$expected" ]; then
      echo "OK: $label = $VALUE"
      return 0
    fi
    echo "Esperando $label=$expected (actual: ${VALUE:-vacío})..."
    sleep "$INTERVAL"
    elapsed=$((elapsed + INTERVAL))
  done
  echo "FALLO: $label esperado $expected, obtenido ${VALUE:-vacío}"
  return 1
}

echo "=== Validación de COUNT(*) por vuelo ==="
check_condition "COUNT AV123" \
  "SELECT COUNT(*) AS cnt FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV123';" \
  '.results[0].cnt' "$EXPECTED_AV123"

check_condition "COUNT AV456" \
  "SELECT COUNT(*) AS cnt FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV456';" \
  '.results[0].cnt' "$EXPECTED_AV456"

echo "=== Validación de MAX(flightCount) por vuelo (estado keyed) ==="
check_condition "MAX(flightCount) AV123" \
  "SELECT MAX(flightCount) AS max_fc FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV123';" \
  '.results[0].max_fc' "$EXPECTED_FC_AV123"

check_condition "MAX(flightCount) AV456" \
  "SELECT MAX(flightCount) AS max_fc FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV456';" \
  '.results[0].max_fc' "$EXPECTED_FC_AV456"

if [ "$TEST_MODE" = "post-migration" ]; then
  echo "=== Validación de documentos post-migración ==="
  check_condition "RES-10101 flightCount" \
    "SELECT flightCount FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE reservationId = 'RES-10101';" \
    '.results[0].flightCount' "$EXPECTED_FC_AV123"

  check_condition "RES-10102 flightCount" \
    "SELECT flightCount FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE reservationId = 'RES-10102';" \
    '.results[0].flightCount' "$EXPECTED_FC_AV456"
fi

echo "=== TODAS LAS VALIDACIONES PASARON ==="