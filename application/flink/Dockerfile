FROM flink:1.19.3-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

ENV PYFLINK_CLIENT_EXECUTABLE=python

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

# Instalar el plugin Azure en el directorio de plugins de Flink.
# El build debe fallar si no existe el JAR correspondiente.
RUN set -eux; \
    mkdir -p /opt/flink/plugins/azure-fs-hadoop; \
    set -- /opt/flink/opt/flink-azure-fs-hadoop-1.19.3.jar; \
    if [ ! -f "$1" ]; then \
        echo "ERROR: no se encontró flink-azure-fs-hadoop-1.19.3.jar"; \
        exit 1; \
    fi; \
    cp "$1" /opt/flink/plugins/azure-fs-hadoop/

# Copiar las dependencias Jackson sin sombrear que estén disponibles
# en la distribución. Son necesarias si el plugin las requiere.
RUN set -eux; \
    find /opt/flink/lib /opt/flink/opt \
        -maxdepth 1 \
        -type f \
        \( \
            -name 'jackson-core-*.jar' \
            -o -name 'jackson-databind-*.jar' \
            -o -name 'jackson-annotations-*.jar' \
    \) \
        -exec cp -v {} /opt/flink/plugins/azure-fs-hadoop/ \;

RUN mkdir -p /opt/flink/usrlib \
    && chown -R flink:flink /opt/flink/usrlib \
    && chown -R flink:flink /opt/flink/plugins/azure-fs-hadoop

COPY src/flight_reservations.py /opt/flink/usrlib/flight_reservations.py

USER flink

WORKDIR /opt/flink