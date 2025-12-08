from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

@dag(
    start_date=datetime(2025, 12, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    dag_id="test_minio"
)
def test_minio_dag():

    @task
    def minio_test_task():
        hook = S3Hook(aws_conn_id='minio_basic')
        s3 = hook.get_conn()
        # # List buckets
        buckets = s3.list_buckets()
        print("Buckets:", buckets)

        # Upload a test file
        hook.load_string(
            "Hello from Airflow", 
            key="test0.txt", 
            bucket_name="airflow-test", 
            replace=True
        )
        print("Uploaded test0.txt")

        # Optional: delete the file
        # hook.delete_objects(bucket_name="airflow-test", keys=["test0.txt"])
        # print("Deleted test0.txt")

    # Call the task
    minio_test_task()

# Assign DAG to a variable (required by Airflow)
dag = test_minio_dag()

