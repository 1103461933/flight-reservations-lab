FROM flink:1.19.3-scala_2.12-java17

USER root

# ==================================================
# 1. Install Python and system dependencies
# ==================================================
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

ENV PYFLINK_CLIENT_EXECUTABLE=python
ENV PYTHONUNBUFFERED=1

# ==================================================
# 2. Install Python application dependencies
# ==================================================
COPY application/flink/requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

# ==================================================
# 3. Install PyFlink runtime JAR
# ==================================================
RUN set -eux; \
    test -f /opt/flink/opt/flink-python-1.19.3.jar; \
    cp /opt/flink/opt/flink-python-1.19.3.jar \
       /opt/flink/lib/flink-python-1.19.3.jar; \
    chown flink:flink /opt/flink/lib/flink-python-1.19.3.jar

# ==================================================
# 4. Install Kafka connector for Flink 1.19
# ==================================================
RUN set -eux; \
    rm -f /opt/flink/lib/flink-connector-kafka-*.jar; \
    rm -f /opt/flink/lib/flink-sql-connector-kafka-*.jar; \
    curl -fSL \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar" \
      -o /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar; \
    chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar

# ==================================================
# 5. Install Azure filesystem plugin
# ==================================================
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop

COPY flink/flink-azure-fs-hadoop-1.19.3.jar \
     /opt/flink/plugins/azure-fs-hadoop/

COPY flink/extra-jars/ \
     /opt/flink/plugins/azure-fs-hadoop/

RUN chown -R flink:flink /opt/flink/plugins/azure-fs-hadoop

# ==================================================
# 6. Copy PyFlink application
# ==================================================
RUN mkdir -p /opt/flink/usrlib \
    && chown -R flink:flink /opt/flink/usrlib

COPY application/flink/src/flight_reservations.py \
     /opt/flink/flight_reservations.py

RUN chown flink:flink /opt/flink/usrlib/flight_reservations.py

# ==================================================
# 7. Runtime configuration
# ==================================================
USER flink