import json
import os
import re
import time
from datetime import datetime, timezone

import boto3
import scrapy

from scraper.items import ProductItem

class WebscraperEcommerceSpider(scrapy.Spider):
    name = "webscraper_ecommerce"
    allowed_domains = ["webscraper.io"]
    start_urls = [
        "https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops",
        "https://webscraper.io/test-sites/e-commerce/allinone/computers/tablets",
        "https://webscraper.io/test-sites/e-commerce/allinone/phones/touch",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()
        self.failed_urls = []

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)

    def handle_error(self, failure):
        self.failed_urls.append(
            {"url": failure.request.url, "error": repr(failure.value)}
        )
        self.logger.warning(f"Fallo en {failure.request.url}: {failure.value}")

    def parse(self, response):
         
        # ej: "computers/laptops" -> category="computers", subcategory="laptops"
         path_parts = [p for p in response.url.split("/") if p][-2:]
         category, subcategory = path_parts if len(path_parts) == 2 else (path_parts[0], "")

         for card in response.css("div.thumbnail"):
            item = ProductItem()

            detail_url = card.css("div.caption a.title::attr(href)").get()
            item["product_url"] = response.urljoin(detail_url) if detail_url else None
            item["sku"] = detail_url.rstrip("/").split("/")[-1] if detail_url else None

            item["title"] = card.css("a.title::attr(title)").get(default="").strip()
            item["description"] = card.css("p.description::text").get(default="").strip()

            price_raw = card.css("h4.price [itemprop=price]::text").get(default="")
            price_clean = re.sub(r"[^\d.]", "", price_raw)
            item["price_usd"] = float(price_clean) if price_clean else None

            review_raw = card.css("[itemprop=reviewCount]::text").get(default="")
            item["review_count"] = int(review_raw) if review_raw.strip().isdigit() else 0

            item["category"] = category
            item["subcategory"] = subcategory

            image_url = card.css("img::attr(src)").get()
            item["image_urls"] = [response.urljoin(image_url)] if image_url else []

            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

            yield item

    def closed(self, reason):
        manifest = {
            "spider": self.name,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "close_reason": reason,
            "item_scraped_count": self.crawler.stats.get_value("item_scraped_count", 0),
            "failed_urls_count": len(self.failed_urls),
            "failed_urls": self.failed_urls,
            "image_bytes_total": self.crawler.stats.get_value("image_bytes_total", 0),
            "duration_seconds": round(time.time() - self.start_time, 2),
        }
        self._upload_manifest(manifest)

    def _upload_manifest(self, manifest):
        bucket = os.getenv("AWS_S3_BUCKET")
        date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"raw/manifests/{self.name}/{date_path}/{self.name}_manifest.json"

        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        self.logger.info(f"Manifest subido a s3://{bucket}/{key}")