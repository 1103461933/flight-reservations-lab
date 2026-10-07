FROM flink:1.20-scala_2.12-java17

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Copiar el plugin de Azure Blob Storage para Flink (soporta abfss://).
# Si el JAR no existe, el build FALLA.
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop \
    && cp /opt/flink/opt/flink-azure-fs-hadoop-*.jar \
          /opt/flink/plugins/azure-fs-hadoop/

COPY src/flight_reservations.py /opt/flink/usrlib/flight_reservations.py

USER flink
WORKDIR /opt/flink