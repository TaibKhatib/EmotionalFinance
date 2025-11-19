FROM apache/airflow:2.8.3

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
