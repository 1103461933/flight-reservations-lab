FROM flink:1.19.3

USER root

RUN rm -rf /opt/flink/plugins/azure-fs-hadoop \
    && mkdir -p /opt/flink/plugins/azure-fs-hadoop

# Install the Flink Azure filesystem plugin
COPY flink-azure-fs-hadoop-1.19.3.jar \
     /opt/flink/plugins/azure-fs-hadoop/

# Add the Jackson dependencies required by Hadoop Azure 3.4.1
COPY extra-jars/ \
     /opt/flink/plugins/azure-fs-hadoop/

# Set the correct permissions
RUN chown -R flink:flink /opt/flink/plugins/azure-fs-hadoop

USER flink