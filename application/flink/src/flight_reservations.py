"""
Job PyFlink: procesa reservas de vuelo desde Kafka y escribe en Couchbase.

El estado gestionado por Flink mantiene el número de reservas y los ingresos
acumulados por vuelo.

El sink utiliza upsert por reservationId para que las escrituras repetidas
del mismo evento no generen documentos duplicados.

Las excepciones de escritura en Couchbase se propagan para permitir que
Flink reinicie el job y recupere su estado desde un checkpoint.
"""

import json
import logging
import math
import os
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    RuntimeContext,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    SinkFunction,
)
from pyflink.datastream.state import ValueStateDescriptor


KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    "flight-kafka-kafka-bootstrap:9092",
)

KAFKA_TOPIC = os.environ.get(
    "KAFKA_TOPIC",
    "flight-reservations",
)

KAFKA_GROUP = os.environ.get(
    "KAFKA_GROUP",
    "flink-reservation-group",
)

COUCHBASE_CONNECTION = os.environ.get(
    "COUCHBASE_CONNECTION",
    "couchbase://flight-couchbase-srv",
)

COUCHBASE_BUCKET = os.environ.get(
    "COUCHBASE_BUCKET",
    "flight-reservations",
)

COUCHBASE_SCOPE = os.environ.get(
    "COUCHBASE_SCOPE",
    "reservations",
)

COUCHBASE_COLLECTION = os.environ.get(
    "COUCHBASE_COLLECTION",
    "processed",
)

CHECKPOINT_INTERVAL_MS = int(
    os.environ.get("CHECKPOINT_INTERVAL_MS", "30000")
)

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def extract_flight_key(value: str) -> str:
    """
    Extrae flightNumber sin permitir que un JSON inválido haga fallar
    la selección de clave antes de llegar al procesador.
    """

    try:
        event = json.loads(value)

        if not isinstance(event, dict):
            return "__INVALID_EVENT__"

        flight_number = event.get("flightNumber")

        if not isinstance(flight_number, str) or not flight_number.strip():
            return "__INVALID_EVENT__"

        return flight_number

    except (json.JSONDecodeError, TypeError):
        return "__INVALID_EVENT__"


class CouchbaseSink(SinkFunction):
    """
    Sink de Couchbase basado en upsert.

    Las excepciones de escritura se propagan para que Flink pueda
    reiniciar el job y recuperar el estado gestionado.
    """

    def __init__(
        self,
        connection_string,
        bucket_name,
        scope_name,
        collection_name,
    ):
        self.connection_string = connection_string
        self.bucket_name = bucket_name
        self.scope_name = scope_name
        self.collection_name = collection_name

        self.cluster = None
        self.collection = None

    def open(self, runtime_context: RuntimeContext):
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        from couchbase.auth import PasswordAuthenticator

        password = os.environ.get("COUCHBASE_PASSWORD")

        if not password:
            raise RuntimeError(
                "La variable COUCHBASE_PASSWORD no está configurada."
            )

        authenticator = PasswordAuthenticator(
            os.environ.get("COUCHBASE_USER", "Administrator"),
            password,
        )

        self.cluster = Cluster.connect(
            self.connection_string,
            ClusterOptions(authenticator),
        )

        bucket = self.cluster.bucket(self.bucket_name)
        scope = bucket.scope(self.scope_name)

        self.collection = scope.collection(self.collection_name)

        logger.info(
            "Sink Couchbase conectado a %s/%s/%s",
            self.bucket_name,
            self.scope_name,
            self.collection_name,
        )

    def invoke(self, value, context):
        try:
            document = json.loads(value)

            reservation_id = document.get("reservationId")

            if not isinstance(reservation_id, str) or not reservation_id:
                raise ValueError(
                    "El documento no contiene un reservationId válido."
                )

            self.collection.upsert(
                reservation_id,
                document,
            )

            logger.info(
                "Documento escrito en Couchbase: %s",
                reservation_id,
            )

        except Exception:
            logger.exception(
                "Error escribiendo el documento en Couchbase."
            )

            # No ocultar el error: Flink debe poder detectar el fallo.
            raise

    def close(self):
        if self.cluster is not None:
            try:
                self.cluster.close()
                logger.info("Conexión a Couchbase cerrada.")
            finally:
                self.cluster = None
                self.collection = None


class ReservationProcessor(KeyedProcessFunction):
    """
    Mantiene estado por flightNumber.

    Estados:
      - reservation_count: número de reservas procesadas por vuelo.
      - total_revenue: suma acumulada del precio final por vuelo.
    """

    def open(self, runtime_context: RuntimeContext):
        self.reservation_count = runtime_context.get_state(
            ValueStateDescriptor(
                "reservation_count",
                Types.LONG(),
            )
        )

        self.total_revenue = runtime_context.get_state(
            ValueStateDescriptor(
                "total_revenue",
                Types.DOUBLE(),
            )
        )

    def process_element(self, value, ctx):
        try:
            event = json.loads(value)

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Evento JSON inválido ignorado: %s",
                value,
            )
            return

        if not isinstance(event, dict):
            logger.warning("El evento no es un objeto JSON.")
            return

        reservation_id = event.get("reservationId")

        if (
            not isinstance(reservation_id, str)
            or not reservation_id.startswith("RES-")
        ):
            logger.warning(
                "reservationId inválido: %s",
                reservation_id,
            )
            return

        flight_number = event.get("flightNumber")

        if not isinstance(flight_number, str) or not flight_number.strip():
            logger.warning(
                "flightNumber inválido para %s",
                reservation_id,
            )
            return

        try:
            price = float(event.get("price", 0.0))

        except (ValueError, TypeError):
            logger.warning(
                "Precio inválido para %s",
                reservation_id,
            )
            return

        if not math.isfinite(price) or price < 0:
            logger.warning(
                "Precio negativo o no finito para %s",
                reservation_id,
            )
            return

        origin = event.get("origin", "???")
        destination = event.get("destination", "???")

        final_price = round(price * 1.16, 2)

        route = f"{origin}-{destination}"

        current_count = self.reservation_count.value() or 0
        current_revenue = self.total_revenue.value() or 0.0

        new_count = current_count + 1
        new_revenue = current_revenue + final_price

        self.reservation_count.update(new_count)
        self.total_revenue.update(new_revenue)

        processed = {
            "reservationId": reservation_id,
            "passengerId": event.get("passengerId"),
            "flightNumber": flight_number,
            "origin": origin,
            "destination": destination,
            "departure": event.get("departure"),
            "seat": event.get("seat"),
            "status": "PROCESSED",
            "price": price,
            "finalPrice": final_price,
            "currency": event.get("currency", "USD"),
            "route": route,
            "flightCount": new_count,
            "totalRevenue": round(new_revenue, 2),
            "processedAt": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "Procesado %s: vuelo=%s count=%d revenue=%.2f",
            reservation_id,
            flight_number,
            new_count,
            new_revenue,
        )

        yield json.dumps(processed, separators=(",", ":"))


def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)

    env.get_checkpoint_config().set_checkpointing_mode(
        CheckpointingMode.EXACTLY_ONCE
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka flight-reservations",
    )

    result = (
        stream
        .key_by(extract_flight_key)
        .process(ReservationProcessor())
    )

    result.add_sink(
        CouchbaseSink(
            COUCHBASE_CONNECTION,
            COUCHBASE_BUCKET,
            COUCHBASE_SCOPE,
            COUCHBASE_COLLECTION,
        )
    ).name("Couchbase reservation sink")

    env.execute("Flight Reservation Processor")


if __name__ == "__main__":
    build_job()