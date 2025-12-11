import scrapy
from trafilatura import extract

class ArticleSpider(scrapy.Spider):
    name = "article_spider"

    def __init__(self, urls=None, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = urls or []

    def parse(self, response):
        # Clean extraction using trafilatura
        text = extract(response.text) or ""
        yield {
            "url": response.url,
            "title": response.css("title::text").get(),
            "content": text,
        }
