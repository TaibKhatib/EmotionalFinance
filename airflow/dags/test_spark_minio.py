from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    start_date=datetime(2025, 12, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    dag_id="test_airflow_spark"
)
def test_spark_connection_dag():

    @task
    def test_spark_task():
        test_spark = SparkSubmitOperator(
            task_id="test_run_spark",
            # This connection will be defined in the Airflow UI
            conn_id="spark_default",
            # Path to your test script as seen from the spark-master container
            application="/opt/spark-apps/print_spark_version.py",
            # Extra configs: point to your spark master and set app name
            conf={
                "spark.master": "spark://spark-master:7077",
                "spark.app.name": "TestFromAirflow",
            },
            # In many simple setups, you do NOT pass 'master' explicitly if using conf,
            # but this also works:
            # master="spark://spark-master:7077",
        )

        test_spark

    test_spark_task()


dag = test_spark_connection_dag()



# The following DAG is commented out because it requires a Spark environment installed in Airflow.
# To enable it, ensure Spark is set up and uncomment the code below.
# The dockerfile is already prepared to include Spark installation.
# @dag(
#     dag_id="test_spark_connection",
#     start_date=datetime(2025, 12, 1),
#     schedule=None,  # Manual trigger
#     catchup=False,
# )
# def test_spark_connection_dag():

#     test_spark = BashOperator(
#         task_id="run_spark_test",
#         bash_command="spark-submit /opt/airflow/src/print_spark_version.py",
#     )

#     # If you later add Python @task functions, you can define dependencies like:
#     # some_task() >> test_spark

#     test_spark


# dag = test_spark_connection_dag()
