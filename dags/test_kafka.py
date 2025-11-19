from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer

def produce_message():
    producer = KafkaProducer(bootstrap_servers="kafka:9092")
    producer.send("test_topic", b"hello from airflow")
    producer.flush()

def consume_message():
    consumer = KafkaConsumer(
        "test_topic",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=2000,
    )
    for msg in consumer:
        print("Airflow consumed:", msg.value)

with DAG(
    dag_id="test_kafka",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="produce",
        python_callable=produce_message
    )

    t2 = PythonOperator(
        task_id="consume",
        python_callable=consume_message
    )

    t1 >> t2
