import json
import boto3
from datetime import datetime

from airflow.decorators import dag, task

import hashlib

from urllib.parse import urlparse
import urllib.robotparser
import trafilatura

from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log

from trafilatura import settings, fetch_url

cfg = settings.use_config()
cfg["DEFAULT"]["USER_AGENT"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

# downloaded = fetch_url(url, config=cfg)

# -------------------------------
# Helpers
# -------------------------------

def is_allowed_by_robots(url: str, user_agent: str = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)") -> bool:
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()

        return rp.can_fetch(user_agent, url)
    except:
        return False


def scrap_url(url: str) -> dict:
    try:
        downloaded = trafilatura.fetch_url(url, config=cfg)
    except Exception as e:
        print(f"[trafilatura.fetch_url] {url} -> {e}")
        return {"url": url, "content": None}

    if not downloaded:
        return {"url": url, "content": None}

    try:
        content = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
        )
    except Exception as e:
        print(f"[trafilatura.extract] {url} -> {e}")
        return {"url": url, "content": None}

    return {"url": url, "content": content}



# -------------------------------
# DAG
# -------------------------------

@dag(
    dag_id="gdelt_scrape_dynamic",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
)
def gdelt_scrape_dynamic():


    # -------------------------------
    # 1. List all S3 objects under gdelt/
    # -------------------------------
    @task
    def list_gdelt_files(bucket="urls-raw"):
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        files = []
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="gdelt/")

        for obj in resp.get("Contents", []):
            files.append(obj["Key"])

        return {"bucket": bucket, "files": files}


    # -------------------------------
    # 2. Extract all URLs from all files
    # -------------------------------
    @task
    def extract_url_batches(info: dict, batch_size: int = 100) -> list[list[dict]]:
        """
        Return a list of batches, each a list of {"url": ..., "date": ...}.
        """
        bucket = info["bucket"]
        files = info["files"]

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        seen_urls = set()
        all_articles: list[dict] = []

        for key in files:
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = json.loads(obj["Body"].read())

            articles = content.get("articles", [])
            for art in articles:
                url = art.get("url")
                date = art.get("seendate") or art.get("date")
                if not url or not date:
                    continue
                if url in seen_urls:
                    continue

                seen_urls.add(url)
                all_articles.append({"url": url, "date": date})

        # Split into batches
        batches: list[list[dict]] = [
            all_articles[i : i + batch_size]
            for i in range(0, len(all_articles), batch_size)
        ]

        log.info("Extracted %d unique articles in %d batches of up to %d",
                len(all_articles), len(batches), batch_size)

        return batches



    # -------------------------------
    # 3. Scrape each URL (one task per URL)
    # -------------------------------
    @task
    def scrape_batch(batch: list[dict], out_bucket: str = "news-raw") -> None:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )

        # Ensure bucket exists (or create it once manually in MinIO)
        # ensure_bucket_exists(s3, out_bucket)

        for article in batch:
            url = article["url"]
            article_date = article["date"]

            # 1) robots.txt check
            try:
                if not is_allowed_by_robots(url):
                    continue
            except Exception as e:
                # log and skip robots-related issues
                print(f"[robots] Error for {url}: {e}")
                continue

            # 2) download + extract
            try:
                result = scrap_url(url)   # wrap trafilatura inside try/except too
            except Exception as e:
                print(f"[scrape] Error for {url}: {e}")
                continue

            if not result or not result.get("content"):
                continue

            # 3) upload to MinIO
            try:
                digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
                out_key = f"scraped/{digest}.json"

                body = json.dumps(
                    {"url": url, "date": article_date, "text": result["content"]}
                ).encode("utf-8")

                s3.put_object(
                    Bucket=out_bucket,
                    Key=out_key,
                    Body=body,
                    ContentType="application/json",
                )
            except Exception as e:
                print(f"[s3] Error for {url}: {e}")
                continue



    # -------------------------------
    # DAG Flow
    # -------------------------------
    files = list_gdelt_files()
    batches = extract_url_batches(files)

    # Dynamic task mapping over the list returned by extract_urls
    scrape_batch.expand(batch=batches)

dag = gdelt_scrape_dynamic()

