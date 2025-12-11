FROM spark:4.0.1-java21-python3

USER root

# ---- OS & Python packages ----
RUN apt-get update && \
    apt-get install -y netcat rsync curl openssh-client && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip --no-cache-dir && \
    pip install pandas lz4 jupyterlab notebook findspark --no-cache-dir

# ---- Environment ----
ENV SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1 \
    SPARK_LOG_DIR=/opt/spark/logs

WORKDIR /opt/spark-apps

# ---- Spark ports & env ----
ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_WORKER_PORT=7001 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

EXPOSE 8080 8081 7077 7001 4042 8888

# ---- Logging dir with safe permissions ----
RUN mkdir -p "${SPARK_LOG_DIR}" && \
    chmod -R a+rw "${SPARK_LOG_DIR}"

# ---- Ensure jars dir exists ----
RUN mkdir -p /opt/spark/jars

# ---- Hadoop AWS + AWS SDK jars ----
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.530.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.530/aws-java-sdk-bundle-1.12.530.jar

# ---- Make jars readable by Spark user ----
RUN chmod -R a+r /opt/spark/jars

# ---- Spark configuration ----
COPY spark-defaults.conf "${SPARK_HOME}/conf"

# ---- Jupyter-friendly home for spark user ----
RUN mkdir -p /home/spark && \
    chown -R spark:spark /home/spark

ENV HOME=/home/spark

# ---- Drop to spark user ----
USER spark

# (Optional) default command can be set via docker-compose or here, e.g.:
# CMD ["/opt/spark/sbin/start-master.sh"]
