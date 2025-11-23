FROM apache/airflow:3.1.3

COPY dags/requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# We use Copy for production deployments and not local development.
# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
