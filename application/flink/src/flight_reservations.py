"""
Job PyFlink: procesa reservas de vuelo desde Kafka y escribe en Couchbase.

El sink propaga excepciones para forzar recuperación desde
checkpoint/savepoint ante fallos de Couchbase.
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    RuntimeContext,
)
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, SinkFunction
from pyflink.datastream.state import ValueStateDescriptor

KAFKA_BOOTSTRAP = "flight-kafka-kafka-bootstrap:9092"
KAFKA_TOPIC = "flight-reservations"
KAFKA_GROUP = "flink-reservation-group"
COUCHBASE_CONNECTION = "couchbase://flight-couchbase-srv"
COUCHBASE_BUCKET = "flight-reservations"
COUCHBASE_SCOPE = "reservations"
COUCHBASE_COLLECTION = "processed"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CouchbaseSink(SinkFunction):
    """Sink idempotente a Couchbase con propagación de errores."""

    def __init__(self, connection_string, bucket_name, scope_name, collection_name):
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

        password = os.environ["COUCHBASE_PASSWORD"]
        auth = PasswordAuthenticator("Administrator", password)
        self.cluster = Cluster.connect(
            self.connection_string, ClusterOptions(auth)
        )
        bucket = self.cluster.bucket(self.bucket_name)
        scope = bucket.scope(self.scope_name)
        self.collection = scope.collection(self.collection_name)
        logger.info("Couchbase sink conectado a %s/%s/%s",
                     self.bucket_name, self.scope_name, self.collection_name)

    def invoke(self, value, context):
        doc = json.loads(value)
        key = doc["reservationId"]
        try:
            self.collection.upsert(key, doc)
            logger.info("Documento escrito: %s", key)
        except Exception as e:
            logger.error("Error escribiendo en Couchbase: %s", e)
            raise


class ReservationProcessor(KeyedProcessFunction):
    """
    KeyedProcessFunction con ValueState:
      - reservation_count: número de reservas por vuelo
      - total_revenue: suma de precios finales por vuelo
    """

    def open(self, runtime_context: RuntimeContext):
        self.reservation_count = runtime_context.get_state(
            ValueStateDescriptor("reservation_count", Types.INT())
        )
        self.total_revenue = runtime_context.get_state(
            ValueStateDescriptor("total_revenue", Types.DOUBLE())
        )

    def process_element(self, value, ctx):
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            logger.warning("Evento no JSON ignorado: %s", value)
            return

        reservation_id = event.get("reservationId", "")
        if not reservation_id.startswith("RES-"):
            logger.warning("reservationId inválido: %s", reservation_id)
            return

        flight_number = event.get("flightNumber", "UNKNOWN")
        origin = event.get("origin", "???")
        destination = event.get("destination", "???")
        price = float(event.get("price", 0.0))

        final_price = round(price * 1.16, 2)
        route = f"{origin}-{destination}"

        count = self.reservation_count.value() or 0
        revenue = self.total_revenue.value() or 0.0
        count += 1
        revenue += final_price
        self.reservation_count.update(count)
        self.total_revenue.update(revenue)

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
            "flightCount": count,
            "routeCount": count,
            "totalRevenue": round(revenue, 2),
            "processedAt": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "Procesado %s vuelo=%s count=%d revenue=%.2f",
            reservation_id, flight_number, count, revenue,
        )
        yield json.dumps(processed)


def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60000)
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
        source, WatermarkStrategy.no_watermarks(), "Kafka flight-reservations"
    )

    result = (
        stream.key_by(lambda x: json.loads(x).get("flightNumber", "UNKNOWN"))
        .process(ReservationProcessor())
    )

    result.add_sink(
        CouchbaseSink(
            COUCHBASE_CONNECTION,
            COUCHBASE_BUCKET,
            COUCHBASE_SCOPE,
            COUCHBASE_COLLECTION,
        )
    )

    env.execute("Flight Reservation Processor")


if __name__ == "__main__":
    build_job()