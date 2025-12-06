from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="test_spark_connection",
    start_date=datetime(2025, 12, 1),
    schedule=None,  # Manual trigger
    catchup=False,
)
def test_spark_connection_dag():

    test_spark = BashOperator(
        task_id="run_spark_test",
        bash_command="spark-submit /opt/airflow/src/print_spark_version.py",
    )

    # If you later add Python @task functions, you can define dependencies like:
    # some_task() >> test_spark

    test_spark


dag = test_spark_connection_dag()
