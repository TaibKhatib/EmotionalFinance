import json
import boto3
from datetime import datetime
from scrapy.exceptions import DropItem

class MinioPipeline:
    def __init__(self, bucket, endpoint, access_key, secret_key):
        self.bucket = bucket
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            bucket=crawler.settings.get("MINIO_BUCKET"),
            endpoint=crawler.settings.get("MINIO_ENDPOINT"),
            access_key=crawler.settings.get("MINIO_ACCESS_KEY"),
            secret_key=crawler.settings.get("MINIO_SECRET_KEY"),
        )

    def open_spider(self, spider):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        self.buffer = []

    def close_spider(self, spider):
        # Convert buffer to JSON lines
        data = "\n".join([json.dumps(item) for item in self.buffer])

        filename = f"scraped/{spider.name}/{datetime.utcnow().isoformat()}.jsonl"

        self.s3.put_object(
            Bucket=self.bucket,
            Key=filename,
            Body=data.encode("utf-8")
        )

    def process_item(self, item, spider):
        self.buffer.append(dict(item))
        return item
