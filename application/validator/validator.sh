#!/usr/bin/env bash

set -euo pipefail

CB_HOST="${COUCHBASE_HOST:-flight-couchbase-srv}"
CB_PORT="${COUCHBASE_PORT:-8093}"
CB_USER="${COUCHBASE_USER:-Administrator}"
CB_PASS="${COUCHBASE_PASSWORD:?COUCHBASE_PASSWORD is required}"

TEST_MODE="${TEST_MODE:-initial}"

MAX_WAIT="${MAX_WAIT:-300}"
INTERVAL="${INTERVAL:-10}"

if [[ "$TEST_MODE" == "initial" ]]; then
    DEFAULT_AV123=37
    DEFAULT_AV456=63

elif [[ "$TEST_MODE" == "post-migration" ]]; then
    DEFAULT_AV123=38
    DEFAULT_AV456=64

else
    echo "ERROR: TEST_MODE inválido: $TEST_MODE"
    exit 1
fi

EXPECTED_AV123="${EXPECTED_AV123:-$DEFAULT_AV123}"
EXPECTED_AV456="${EXPECTED_AV456:-$DEFAULT_AV456}"

EXPECTED_FC_AV123="${EXPECTED_FC_AV123:-$DEFAULT_AV123}"
EXPECTED_FC_AV456="${EXPECTED_FC_AV456:-$DEFAULT_AV456}"


query() {
    local stmt="$1"

    local payload

    payload="$(
        jq -nc --arg statement "$stmt" \
            '{statement: $statement}'
    )"

    curl \
        --fail \
        --silent \
        --show-error \
        --connect-timeout 5 \
        --max-time 30 \
        -u "${CB_USER}:${CB_PASS}" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "http://${CB_HOST}:${CB_PORT}/query/service"
}


check_condition() {
    local label="$1"
    local stmt="$2"
    local jq_filter="$3"
    local expected="$4"

    local elapsed=0
    local response
    local value

    while (( elapsed < MAX_WAIT )); do
        if response="$(query "$stmt" 2>/dev/null)"; then
            value="$(
                printf '%s' "$response" |
                    jq -r "${jq_filter} // empty" 2>/dev/null ||
                    true
            )"

            if [[ "$value" == "$expected" ]]; then
                echo "OK: ${label} = ${value}"
                return 0
            fi
        else
            value=""
        fi

        echo "Esperando ${label}=${expected}; actual: ${value:-vacío}..."

        sleep "$INTERVAL"
        elapsed=$((elapsed + INTERVAL))
    done

    echo "FALLO: ${label}; esperado=${expected}; obtenido=${value:-vacío}"

    return 1
}


echo "Modo de validación: ${TEST_MODE}"

echo "Esperando Couchbase y el keyspace..."

READY=false

for i in $(seq 1 30); do
    if response="$(
        query \
            "SELECT COUNT(*) AS c FROM \`flight-reservations\`.\`reservations\`.\`processed\`;" \
            2>/dev/null
    )"; then
        if printf '%s' "$response" |
            jq -e '.status == "success"' >/dev/null 2>&1; then

            READY=true

            echo "Keyspace disponible."
            break
        fi
    fi

    echo "Intento ${i}/30: keyspace no disponible; esperando ${INTERVAL}s..."

    sleep "$INTERVAL"
done

if [[ "$READY" != "true" ]]; then
    echo "ERROR: el keyspace no estuvo disponible dentro del tiempo esperado."
    exit 1
fi


echo "=== Validación de COUNT(*) por vuelo ==="

check_condition \
    "COUNT AV123" \
    "SELECT COUNT(*) AS cnt FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV123';" \
    '.results[0].cnt' \
    "$EXPECTED_AV123"

check_condition \
    "COUNT AV456" \
    "SELECT COUNT(*) AS cnt FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV456';" \
    '.results[0].cnt' \
    "$EXPECTED_AV456"


echo "=== Validación de MAX(flightCount) por vuelo ==="

check_condition \
    "MAX(flightCount) AV123" \
    "SELECT MAX(flightCount) AS max_fc FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV123';" \
    '.results[0].max_fc' \
    "$EXPECTED_FC_AV123"

check_condition \
    "MAX(flightCount) AV456" \
    "SELECT MAX(flightCount) AS max_fc FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE flightNumber = 'AV456';" \
    '.results[0].max_fc' \
    "$EXPECTED_FC_AV456"


if [[ "$TEST_MODE" == "post-migration" ]]; then
    echo "=== Validación de los documentos post-migración ==="

    check_condition \
        "RES-10101 flightCount" \
        "SELECT flightCount FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE reservationId = 'RES-10101';" \
        '.results[0].flightCount' \
        "$EXPECTED_FC_AV123"

    check_condition \
        "RES-10102 flightCount" \
        "SELECT flightCount FROM \`flight-reservations\`.\`reservations\`.\`processed\` WHERE reservationId = 'RES-10102';" \
        '.results[0].flightCount' \
        "$EXPECTED_FC_AV456"
fi


echo "=== TODAS LAS VALIDACIONES PASARON ==="