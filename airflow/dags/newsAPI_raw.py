from datetime import datetime, timedelta
import io, json, requests, boto3
from airflow.decorators import dag, task

API_TOKEN = "0nFkRB6ejXIvuYtTtDYK7qQ1t60rbgdLvHq1DBYK"
BASE_URL = "https://api.thenewsapi.com/v1/news/all"

@dag(
    dag_id="thenewsapi_to_minio_daily",
    start_date=datetime(2025, 12, 1),
    schedule=None,   # or "0 2 * * *" for daily
    catchup=False,
)
def thenewsapi_to_minio_daily_dag():

    @task
    def fetch_thenewsapi_daily(
        days_back: int = 90,
        bucket: str = "news-raw",
    ):
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days_back)

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        uploaded_keys = []
        cur = start_date
        while cur <= end_date:
            day_start = datetime(cur.year, cur.month, cur.day, 0, 0, 0)
            day_end   = datetime(cur.year, cur.month, cur.day, 23, 59, 59)

            params = {
                "api_token": API_TOKEN,
                "search": '("Cosmo Pharmaceuticals" OR "Cosmo Pharma" OR "TRB Chemedica" '
                          'OR "Norgine" OR "EndoChoice" OR "PhaseBio" OR "Alkermes" OR "Genmab")',
                "language": "en",
                "published_after": day_start.isoformat() + "Z",
                "published_before": day_end.isoformat() + "Z",
                "page": 1,
                "limit": 50,
            }

            resp = requests.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("data", [])

            if not articles:
                cur += timedelta(days=1)
                continue

            buf = io.BytesIO()
            buf.write(json.dumps(data).encode("utf-8"))
            buf.seek(0)

            year, month, day = cur.year, cur.month, cur.day
            key = f"thenewsapi/year={year}/month={month:02d}/day={day:02d}/news_{year}-{month:02d}-{day:02d}.json"

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=buf.getvalue(),
                ContentType="application/json",
            )
            uploaded_keys.append(key)

            cur += timedelta(days=1)

        return {"bucket": bucket, "keys": uploaded_keys}

    fetch_thenewsapi_daily()

dag = thenewsapi_to_minio_daily_dag()
