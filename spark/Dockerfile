FROM spark:4.0.1-java21-python3

USER root

# Install required packages
RUN apt-get update && \
    apt-get install -y netcat rsync  && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip --no-cache-dir && \
    pip install pandas lz4 jupyterlab notebook findspark --no-cache-dir

ENV SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

WORKDIR /opt/spark-apps

# Spark ports
ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_WORKER_PORT=7001 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master" \
    SPARK_LOG_DIR=/opt/spark/logs

EXPOSE 8080 8081 7077 7001 4042 8888

# Logging
RUN mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_LOG_DIR/spark-master.out && \
    touch $SPARK_LOG_DIR/spark-worker.out && \
    ln -sf /dev/stdout $SPARK_LOG_DIR/spark-master.out && \
    ln -sf /dev/stdout $SPARK_LOG_DIR/spark-worker.out

# Copy config and start script
COPY spark-defaults.conf "${SPARK_HOME}/conf"
