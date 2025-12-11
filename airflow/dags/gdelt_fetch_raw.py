import json
import requests
import urllib.robotparser
from urllib.parse import urlparse
from datetime import datetime

import boto3
import pandas as pd

from scrapy.crawler import CrawlerProcess

# IMPORTANT: update this to your real spider path:
from airflow.dags.scrapy import ArticleSpider

from airflow.decorators import dag, task


# -------------------------
# Scrapability checks
# -------------------------

def is_allowed_by_robots(url: str, user_agent: str = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)") -> bool:
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()

        return rp.can_fetch(user_agent, url)
    except Exception:
        return False


def is_html(url: str) -> bool:
    try:
        r = requests.head(url, timeout=4, allow_redirects=True)
        ct = r.headers.get("Content-Type", "").lower()
        return "text/html" in ct
    except Exception:
        return False


def is_reachable(url: str) -> bool:
    try:
        r = requests.get(url, timeout=6, allow_redirects=True)
        return r.status_code < 400
    except Exception:
        return False


def is_scrapable(url: str) -> bool:
    """
    robots.txt → reachability → HTML content
    """
    return (
        is_allowed_by_robots(url)
        # and is_reachable(url)
        # and is_html(url)
    )


# -------------------------
# Airflow DAG
# -------------------------

@dag(
    dag_id="gdelt_to_minio_raw",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
)
def gdelt_to_minio_raw_dag():

    MINIO_ENDPOINT = "http://minio:9000"   
    MINIO_ACCESS_KEY = "minio"         
    MINIO_SECRET_KEY = "minio123"        
    MINIO_BUCKET = "news-raw"

    def upload_to_minio(local_path: str, object_name: str):
        """Uploads a local file to MinIO."""
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        s3.upload_file(local_path, MINIO_BUCKET, object_name)


    @task
    def scrape_urls(urls: list[str]):
        """
        Runs Scrapy to extract article content.
        Saves articles.json locally, then uploads to MinIO.
        """
        output_file = "articles.json"

        process = CrawlerProcess(settings={
            "USER_AGENT": "Mozilla/5.0",
            "LOG_LEVEL": "ERROR",
            "FEED_FORMAT": "json",
            "FEED_URI": output_file,
        })

        process.crawl(ArticleSpider, urls=urls)
        process.start()

        # Upload to MinIO
        upload_to_minio(output_file, f"gdelt/{output_file}")

        return f"Uploaded to MinIO at gdelt/{output_file}"

    @task
    def fetch_gdelt_urls() -> list[str]:
        """
        A function to fetch URLs from GDELT.
        """

        api_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        params = {
            "query": "(apple OR AAPL) ( decision OR innovate OR forecast) sourcelang:english",
            "mode": "artList",
            "timespan": "1w",
            "format": "json"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        result = requests.get(api_url, params=params, headers=headers)
        articles = result.json().get("articles", [])
        urls = [article["url"] for article in articles]
        return urls

    @task
    def filter_scrapable(urls: list[str]) -> list[str]:
        """
        Filters the list of URLs to only those that are scrapable.
        """
        return [url for url in urls if is_scrapable(url)]
    
    urls = fetch_gdelt_urls()
    scrapable_urls = filter_scrapable(urls)
    scrape_urls(scrapable_urls)

workflow = gdelt_to_minio_raw_dag()

