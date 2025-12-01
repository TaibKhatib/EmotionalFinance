FROM apache/airflow:3.1.3

# Copy requirements from the build context (./airflow) into the image
# The build context for this Dockerfile is `./airflow`, so the file is at `dags/requirements.txt`
COPY dags/requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install apache-airflow-providers-amazon

# We use Copy for production deployments and not local development.
# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

