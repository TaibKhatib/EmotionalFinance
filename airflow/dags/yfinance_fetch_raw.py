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
        tickers: list[str] = ["COPN.SW"],
        bucket: str = "market-raw",
    ):
        
        today = datetime.today()
        start_date = datetime(today.year - 5, 12, 12)  # Dec 12, 5 years ago
        end_date = datetime(today.year - 3, today.month, today.day)  # today - 3 years

        # 1) Download daily data with yfinance
        df = yf.download(
            tickers=" ".join(tickers),
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
        )

        # Ensure DataFrame is not empty
        if df.empty:
            raise ValueError("No data returned from yfinance")

        # 2) Upload to MinIO via S3-compatible client
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",  # inside Docker network
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        uploaded_keys = []

        iso = df.index.isocalendar()  # returns a DataFrame with year, week, day
        ticker_prefix = "_".join(tickers)  # e.g., "COPN.SW" or "COPN.SW_AAPL"

        for (year, week), weekly_df in df.groupby([iso.year, iso.week]):
            parquet_buf = io.BytesIO()
            weekly_df.to_parquet(
                parquet_buf,
                index=True,
                engine="pyarrow",
                coerce_timestamps="us",           # downcast to microseconds
                allow_truncated_timestamps=True,  # allow precision loss
            )
            parquet_buf.seek(0)

            key = f"{ticker_prefix}/year={year}/week={week:02d}/data_{year}-W{week:02d}.parquet"
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=parquet_buf.getvalue(),
            )
            uploaded_keys.append(key)


        # Optional: return the key for downstream Spark task
        return {"bucket": bucket, "keys": uploaded_keys}
    
    @task
    def inspect_data(info: dict):
        bucket = info["bucket"]
        # assuming fetch_and_upload_raw returns {"bucket": ..., "keys": [...]}
        first_key = info["keys"][0]

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        obj = s3.get_object(Bucket=bucket, Key=first_key)
        parquet_buf = io.BytesIO(obj["Body"].read())
        df = pd.read_parquet(parquet_buf)

        print("Data sample:")
        print(df.head())

    raw_data_info = fetch_and_upload_raw()
    inspect_data(raw_data_info)

dag = yfinance_to_minio_raw_dag()

