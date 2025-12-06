FROM apache/airflow:3.1.3

# Copy requirements from the build context (./airflow) into the image
# The build context for this Dockerfile is `./airflow`, so the file is at `dags/requirements.txt`
COPY dags/requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install apache-airflow-providers-amazon

# We use Copy for production deployments and not local development.
# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/

# --- Install Java and Spark ---
USER root

# Install Java (required by Spark)
RUN apt-get update && apt-get install -y curl openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

# Install Spark
# Install Spark 4.0.1 with Hadoop 3
ENV SPARK_VERSION=4.0.1
ENV HADOOP_PROFILE=hadoop3

RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE} /opt/spark \
 && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:/opt/spark/bin"

USER airflow


ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

