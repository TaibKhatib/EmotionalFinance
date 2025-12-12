from datetime import datetime, timedelta
import io, json, requests, boto3
from airflow.decorators import dag, task

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

@dag(
    dag_id="gdelt_to_minio_5y_daily",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
)
def gdelt_to_minio_5y_daily_dag():

    @task
    def fetch_and_upload_gdelt_daily(
        years_back: int = 5,
        bucket: str = "urls-raw",
    ):
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=365 * years_back)

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        base_query = (
            '("Cosmo Pharmaceuticals" OR "Cosmo Pharma" OR "TRB Chemedica" '
            'OR "Norgine" OR "EndoChoice" OR "PhaseBio" OR "EndoChoice" '
            'OR "Alkermes" OR "Genmab") sourcelang:english'
        )

        cur = start_date
        uploaded_keys = []

        while cur <= end_date:
            day_start = datetime(cur.year, cur.month, cur.day, 0, 0, 0)
            day_end   = datetime(cur.year, cur.month, cur.day, 23, 59, 59)

            params = {
                "query": base_query,
                "mode": "artList",
                "format": "json",
                "maxrecords": 250,  # adjust if needed
                "startdatetime": day_start.strftime("%Y%m%d%H%M%S"),
                "enddatetime": day_end.strftime("%Y%m%d%H%M%S"),
            }

            resp = requests.get(GDELT_DOC_API, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            # Skip days with no articles
            if not data or "articles" not in data or len(data["articles"]) == 0:
                cur += timedelta(days=1)
                continue

            buf = io.BytesIO()
            buf.write(json.dumps(data).encode("utf-8"))
            buf.seek(0)

            year = cur.year
            month = cur.month
            day = cur.day
            key = (
                f"gdelt/year={year}/month={month:02d}/day={day:02d}/"
                f"news_{year}-{month:02d}-{day:02d}.json"
            )

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=buf.getvalue(),
                ContentType="application/json",
            )
            uploaded_keys.append(key)

            cur += timedelta(days=1)

        return {"bucket": bucket, "keys": uploaded_keys}

    fetch_and_upload_gdelt_daily()

dag = gdelt_to_minio_5y_daily_dag()
