import json
import requests
import urllib.robotparser
from urllib.parse import urlparse
from datetime import datetime, timedelta
import boto3
import pandas as pd

from trafilatura import extract, fetch_url, extract_metadata

# IMPORTANT: update this to your real spider path:
# from airflow.dags.scrapy import ArticleSpider

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

def scrap_url(url: str) -> dict:
    """
    Scrapes the article content from the given URL using Trafilatura.
    Returns a dictionary with the URL and extracted content.
    """

    downloaded = fetch_url(url)
    if downloaded:
        result = extract(downloaded, include_comments=False, include_tables=False)
        meta = extract_metadata(downloaded, default_url=url)
        pub_date = meta.date if meta is not None else None
        return {"url": url, "content": result, "pub_date": pub_date}
    else:
        return {"url": url, "content": None, "pub_date": None}


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
    MINIO_BUCKET = "test"

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
        Runs the scrap_url function to extract article content.
        Saves articles.json locally, then uploads to MinIO.
        """
        scraped_data = []
        for url in urls:
            scraped_data.append(scrap_url(url))
        
        local_file = "/tmp/articles.json"
        with open(local_file, "w") as f:
            json.dump(scraped_data, f)
        
        object_name = f"articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        upload_to_minio(local_file, object_name)

    @task
    def fetch_gdelt_urls() -> list[str]:
        """
        A function to fetch URLs from GDELT.
        """

        api_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        #(approval OR trial OR EMA OR FDA OR merger OR acquisition OR regulation OR drug OR phase OR patent OR competitor)
        params = {
            "query": '("Cosmo Pharmaceuticals" OR "Cosmo Pharma" OR "TRB Chemedica" OR "Norgine" OR "EndoChoice" OR "PhaseBio" OR "EndoChoice" OR "Alkermes" OR "Genmab") sourcelang:english',
            "mode": "artList",
            "startdatetime": "20241215000000",
            "enddatetime": "20250115000000",
            "format": "json"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        try:
            result = requests.get(api_url, params=params, headers=headers, timeout=10)
            result.raise_for_status()
            data = result.json()
            articles = data.get("articles", [])
            if not articles:
                print("No articles returned from GDELT.")
                return []
            urls = [article["url"] for article in articles]
            return urls
        
        except requests.RequestException as e:
            print(f"GDELT request failed: {e}")
            return []
        except ValueError as e:
            print(f"Error parsing JSON from GDELT: {e}")
            return []

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

