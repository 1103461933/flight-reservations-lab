"""
Productor con modo de operación determinista.

TEST_MODE=initial:  genera 100 eventos
  AV123 → 37 (RES-10001..RES-10037)
  AV456 → 63 (RES-10038..RES-10100)

TEST_MODE=post-migration: genera 2 eventos
  RES-10101 → AV123
  RES-10102 → AV456

Si hay errores de delivery, el producer falla explícitamente.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "flight-kafka-kafka-bootstrap:9092"
)
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flight-reservations")
TEST_MODE = os.environ.get("TEST_MODE", "initial")
INTERVAL_MS = int(os.environ.get("INTERVAL_MS", "50"))


if TEST_MODE == "initial":
    FLIGHT_PLAN = [("AV123", "BOG", "MIA")] * 37 + [("AV456", "MDE", "JFK")] * 63
elif TEST_MODE == "post-migration":
    FLIGHT_PLAN = [
        ("AV123", "BOG", "MIA"),
        ("AV456", "MDE", "JFK"),
    ]
else:
    sys.exit(f"TEST_MODE inválido: {TEST_MODE} (usar 'initial' o 'post-migration')")

STATUSES = ["CONFIRMED", "PENDING", "CANCELLED"]

delivery_errors = []


def delivery_report(err, msg):
    if err is not None:
        delivery_errors.append(f"Error al entregar mensaje: {err}")
    else:
        print(f"Mensaje entregado a {msg.topic()} [{msg.partition()}]")


def generate_event(index: int, flight: str, origin: str, destination: str) -> dict:
    return {
        "reservationId": f"RES-{10000 + index}",
        "passengerId": f"PAX-{index:03d}",
        "flightNumber": flight,
        "origin": origin,
        "destination": destination,
        "departure": datetime.now(timezone.utc).isoformat(),
        "seat": f"{(index % 30) + 1}{'ABCDEF'[index % 6]}",
        "status": STATUSES[index % len(STATUSES)],
        "price": round(100.0 + (index * 7.5) % 800.0, 2),
        "currency": "USD",
    }


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    total = len(FLIGHT_PLAN)
    print(f"TEST_MODE={TEST_MODE}, enviando {total} eventos a {KAFKA_TOPIC}...")

    start_index = 1 if TEST_MODE == "initial" else 101

    for i in range(total):
        flight, origin, destination = FLIGHT_PLAN[i]
        event = generate_event(start_index + i, flight, origin, destination)
        producer.produce(
            KAFKA_TOPIC,
            key=event["flightNumber"],
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(INTERVAL_MS / 1000.0)

    producer.flush()

    if delivery_errors:
        print("Errores de delivery detectados:")
        for e in delivery_errors:
            print(e)
        sys.exit(1)

    print(f"Productor finalizado. Enviados {total} eventos sin errores.")


if __name__ == "__main__":
    main()