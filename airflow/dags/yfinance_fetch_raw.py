from datetime import datetime, timedelta
import io

import boto3
import pandas as pd
import yfinance as yf

from airflow.decorators import dag, task

@dag(
    dag_id="yfinance_to_minio_raw",
    start_date=datetime(2025, 12, 1),
    schedule=None,  # manual for now
    catchup=False,
)
def yfinance_to_minio_raw_dag():
    @task
    def fetch_and_upload_raw(
        tickers: list[str] = ["AAPL", "MSFT"],
        days_back: int = 7,
        bucket: str = "market-raw",
    ):
        # 1) Compute date range
        end = datetime.utcnow().date()
        start = end - timedelta(days=days_back)

        # 2) Download daily OHLCV with yfinance
        df = yf.download(
            tickers=" ".join(tickers),
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
        )

        # Ensure DataFrame is not empty
        if df.empty:
            raise ValueError("No data returned from yfinance")

        # # 3) Convert to CSV in memory
        # csv_buf = io.StringIO()
        # df.to_csv(csv_buf)
        # csv_buf.seek(0)

        # Convert to parquet in memory
        parquet_buf = io.BytesIO()
        df.to_parquet(parquet_buf, index=True)
        parquet_buf.seek(0)

        # 4) Upload to MinIO via S3-compatible client
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",  # inside Docker network
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        key = f"stocks/multi_ticker_{start}_{end}.parquet"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buf.getvalue(),
        )

        # Optional: return the key for downstream Spark task
        return {"bucket": bucket, "key": key}
    
    @task
    def inspect_data(info: dict):
        bucket = info["bucket"]
        key = info["key"]

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",  # inside Docker network
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        obj = s3.get_object(Bucket=bucket, Key=key)
        parquet_buf = io.BytesIO(obj['Body'].read())
        df = pd.read_parquet(parquet_buf)

        print("Data sample:")
        print(df.head())

    info = fetch_and_upload_raw()
    inspect_data(info)

dag = yfinance_to_minio_raw_dag()
