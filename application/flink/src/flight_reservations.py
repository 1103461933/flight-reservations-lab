"""
Job PyFlink: procesa reservas de vuelo desde Kafka y escribe en Couchbase.

Características:
- Consume eventos desde Kafka.
- Valida las reservas.
- Calcula el precio final aplicando un 16 %.
- Mantiene el número de reservas y los ingresos acumulados por vuelo.
- Escribe los resultados en Couchbase mediante upsert.
- Configura checkpoints EXACTLY_ONCE en Flink.

Importante:
La escritura en Couchbase es una operación externa y no participa
en una transacción exactamente-una-vez con los checkpoints de Flink.

Por tanto, el upsert utiliza reservationId como clave para que una
repetición del mismo evento no genere documentos duplicados.
"""

import json
import logging
import math
import os

from datetime import datetime, timezone

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema

from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
)

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

from pyflink.datastream.functions import (
    KeyedProcessFunction,
    MapFunction,
    RuntimeContext,
)

from pyflink.datastream.state import ValueStateDescriptor


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("flight-reservations")


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

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


# ============================================================
# COUCHBASE CONFIGURATION
# ============================================================

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

COUCHBASE_USER = os.environ.get(
    "COUCHBASE_USER",
    "Administrator",
)

# No se establece una contraseña por defecto.
# La contraseña debe proceder de un Kubernetes Secret.
COUCHBASE_PASSWORD = os.environ.get("COUCHBASE_PASSWORD")


# ============================================================
# CHECKPOINT CONFIGURATION
# ============================================================

CHECKPOINT_INTERVAL_MS = int(
    os.environ.get("CHECKPOINT_INTERVAL_MS", "30000")
)


# ============================================================
# COUCHBASE WRITER
# ============================================================

class CouchbaseWriter(MapFunction):
    """
    Escribe el resultado procesado en Couchbase.

    MapFunction permite implementar open() y close() para administrar
    los recursos utilizados por la función.

    Cada subtask que ejecute esta función establece su propia conexión
    a Couchbase.
    """

    def open(self, runtime_context: RuntimeContext):
        """
        Inicializa la conexión antes de procesar los eventos.
        """

        self.cluster = None
        self.collection = None

        password = os.environ.get("COUCHBASE_PASSWORD")

        if not password:
            raise RuntimeError(
                "COUCHBASE_PASSWORD no está configurada. "
                "Verifica el Kubernetes Secret couchbase-auth."
            )

        username = os.environ.get(
            "COUCHBASE_USER",
            "Administrator",
        )

        connection_string = os.environ.get(
            "COUCHBASE_CONNECTION",
            "couchbase://flight-couchbase-srv",
        )

        bucket_name = os.environ.get(
            "COUCHBASE_BUCKET",
            "flight-reservations",
        )

        scope_name = os.environ.get(
            "COUCHBASE_SCOPE",
            "reservations",
        )

        collection_name = os.environ.get(
            "COUCHBASE_COLLECTION",
            "processed",
        )

        logger.info(
            "Initializing Couchbase connection. "
            "Bucket=%s, scope=%s, collection=%s",
            bucket_name,
            scope_name,
            collection_name,
        )

        authenticator = PasswordAuthenticator(
            username,
            password,
        )

        self.cluster = Cluster.connect(
            connection_string,
            ClusterOptions(authenticator),
        )

        self.cluster.wait_until_ready(
            __import__("datetime").timedelta(seconds=15)
        )

        bucket = self.cluster.bucket(bucket_name)

        self.collection = (
            bucket
            .scope(scope_name)
            .collection(collection_name)
        )

        logger.info("Couchbase connection initialized successfully.")

    def map(self, value: str) -> str:
        """
        Escribe un evento procesado en Couchbase y devuelve el evento.

        Se utiliza reservationId como clave del documento para que
        una repetición del mismo evento actualice el documento existente.
        """

        if self.collection is None:
            raise RuntimeError(
                "La conexión a Couchbase no está inicializada."
            )

        document = json.loads(value)

        reservation_id = document.get("reservationId")

        if not reservation_id:
            raise ValueError(
                "El evento procesado no contiene reservationId."
            )

        self.collection.upsert(
            reservation_id,
            document,
        )

        logger.info(
            "Reservation upserted successfully: %s",
            reservation_id,
        )

        return value

    def close(self):
        """
        Libera los recursos al detenerse la función.
        """

        if self.cluster is not None:
            try:
                self.cluster.close()
                logger.info("Couchbase connection closed.")
            finally:
                self.cluster = None
                self.collection = None


# ============================================================
# RESERVATION PROCESSOR
# ============================================================

class ReservationProcessor(KeyedProcessFunction):
    """
    Valida y procesa las reservas.

    El stream se agrupa por flightNumber. Para cada vuelo se mantiene:

    - flight_count: número acumulado de reservas.
    - flight_revenue: ingresos acumulados.

    Ambos valores utilizan estado gestionado por Flink, por lo que
    pueden recuperarse a partir de los checkpoints.
    """

    def open(self, runtime_context: RuntimeContext):
        """
        Inicializa el estado gestionado de Flink.
        """

        count_descriptor = ValueStateDescriptor(
            "flight_count",
            Types.LONG(),
        )

        revenue_descriptor = ValueStateDescriptor(
            "flight_revenue",
            Types.DOUBLE(),
        )

        self.flight_count_state = runtime_context.get_state(
            count_descriptor
        )

        self.flight_revenue_state = runtime_context.get_state(
            revenue_descriptor
        )

        logger.info("ReservationProcessor initialized.")

    def process_element(self, value: str, ctx):
        """
        Procesa una reserva válida y produce un evento JSON con los
        resultados. Los mensajes inválidos se registran y se omiten
        para evitar que un único registro derribe el job.
        """

        # --------------------------------------------------------
        # JSON PARSING
        # --------------------------------------------------------

        try:
            reservation = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            payload_length = len(value) if isinstance(value, str) else 0
            logger.warning(
                "Skipping invalid Kafka event: malformed JSON "
                "(payload_length=%s).",
                payload_length,
            )
            return

        # El JSON debe representar un objeto, no una lista, número,
        # cadena, booleano ni null.
        if not isinstance(reservation, dict):
            logger.warning(
                "Skipping invalid Kafka event: JSON root must be an object."
            )
            return

        reservation_id = reservation.get("reservationId")
        flight_number = reservation.get("flightNumber")
        price = reservation.get("price")

        # --------------------------------------------------------
        # VALIDATION
        # --------------------------------------------------------

        if not isinstance(reservation_id, str):
            logger.warning(
                "Skipping invalid reservation: reservationId must be a string."
            )
            return

        if not reservation_id.startswith("RES-"):
            logger.warning(
                "Skipping invalid reservation: reservationId must start with RES-."
            )
            return

        if not isinstance(flight_number, str) or not flight_number.strip():
            logger.warning(
                "Skipping invalid reservation %s: flightNumber must be a non-empty string.",
                reservation_id,
            )
            return

        # bool es una subclase de int en Python; se excluye expresamente.
        if isinstance(price, bool) or not isinstance(price, (int, float)):
            logger.warning(
                "Skipping invalid reservation %s: price must be numeric.",
                reservation_id,
            )
            return

        price = float(price)

        if not math.isfinite(price) or price < 0:
            logger.warning(
                "Skipping invalid reservation %s: price must be finite and non-negative.",
                reservation_id,
            )
            return

        # --------------------------------------------------------
        # BUSINESS LOGIC
        # --------------------------------------------------------

        final_price = round(price * 1.16, 2)

        current_count = self.flight_count_state.value()
        current_revenue = self.flight_revenue_state.value()

        if current_count is None:
            current_count = 0

        if current_revenue is None:
            current_revenue = 0.0

        new_count = current_count + 1

        # Los ingresos acumulados incluyen el precio final con IVA.
        new_revenue = round(
            current_revenue + final_price,
            2,
        )

        self.flight_count_state.update(new_count)
        self.flight_revenue_state.update(new_revenue)

        # --------------------------------------------------------
        # OUTPUT EVENT
        # --------------------------------------------------------

        processed_reservation = {
            "reservationId": reservation_id,
            "flightNumber": flight_number,
            "price": price,
            "finalPrice": final_price,
            "flightCount": new_count,
            "totalRevenue": new_revenue,
            "processedAt": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        logger.info(
            "Reservation processed: reservationId=%s, "
            "flightNumber=%s, flightCount=%s, totalRevenue=%s",
            reservation_id,
            flight_number,
            new_count,
            new_revenue,
        )

        yield json.dumps(
            processed_reservation,
            ensure_ascii=False,
            allow_nan=False,
        )


# ============================================================
# KAFKA FLINK JOB
# ============================================================

def build_job():
    """
    Construye y ejecuta el job de procesamiento de reservas.
    """

    logger.info("Starting Flight Reservation Processor.")

    logger.info(
        "Kafka configuration: bootstrap=%s, topic=%s, group=%s",
        KAFKA_BOOTSTRAP,
        KAFKA_TOPIC,
        KAFKA_GROUP,
    )

    # --------------------------------------------------------
    # EXECUTION ENVIRONMENT
    # --------------------------------------------------------

    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(
        CHECKPOINT_INTERVAL_MS,
        CheckpointingMode.EXACTLY_ONCE,
    )

    # --------------------------------------------------------
    # KAFKA SOURCE
    # --------------------------------------------------------

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    source_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Flight Reservations Kafka Source",
        Types.STRING(),
    )

    # --------------------------------------------------------
    # KEY SELECTION
    # --------------------------------------------------------

    def extract_flight_number(raw_event: str) -> str:
        """
        Extrae flightNumber para agrupar las reservas por vuelo.
        Los mensajes malformados o sin una clave de vuelo válida se
        asignan a claves temporales y se omiten posteriormente por
        ReservationProcessor.
        """

        try:
            event = json.loads(raw_event)
        except (json.JSONDecodeError, TypeError, ValueError):
            return "__invalid_event__"

        if not isinstance(event, dict):
            return "__invalid_event__"

        flight_number = event.get("flightNumber")

        if not isinstance(flight_number, str) or not flight_number.strip():
            return "__invalid_flight__"

        return flight_number

    keyed_stream = source_stream.key_by(
        extract_flight_number
    )

    # --------------------------------------------------------
    # PROCESS RESERVATIONS
    # --------------------------------------------------------

    processed_stream = keyed_stream.process(
        ReservationProcessor(),
        output_type=Types.STRING(),
    )

    # --------------------------------------------------------
    # WRITE TO COUCHBASE
    # --------------------------------------------------------

    couchbase_stream = processed_stream.map(
        CouchbaseWriter(),
        output_type=Types.STRING(),
    )

    # --------------------------------------------------------
    # LOG PROCESSED EVENTS
    # --------------------------------------------------------

    couchbase_stream.print().name(
        "Processed reservations output"
    )

    # --------------------------------------------------------
    # EXECUTE JOB
    # --------------------------------------------------------

    #env.execute(
    #    "Flight Reservation Processor"
    #)
    
    flink_job_name = os.environ.get(
        "FLINK_JOB_NAME",
        "Flight Reservation Processor",
    )

    env.execute(flink_job_name)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    build_job()