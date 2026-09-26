import json
import os
import re
import time
from datetime import datetime, timezone

import boto3
import scrapy

from scraper.items import CarItem


class WebscraperCarsSpider(scrapy.Spider):
    name = "webscraper_cars"
    allowed_domains = ["webscraper.io"]
    start_urls = ["https://webscraper.io/test-sites/pagination"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()
        self.failed_urls = []

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)

    def parse(self, response):
        for detail_url in response.css("a.card-head-url::attr(href)").getall():
            yield response.follow(
                detail_url, callback=self.parse_detail, errback=self.handle_error
            )

        next_page = response.css("a.page-link.next::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page, callback=self.parse, errback=self.handle_error
            )

    def handle_error(self, failure):
        self.failed_urls.append(
            {"url": failure.request.url, "error": repr(failure.value)}
        )
        self.logger.warning(f"Fallo en {failure.request.url}: {failure.value}")

    def parse_detail(self, response):
        item = CarItem()

        item["product_url"] = response.url
        item["sku"] = response.url.rstrip("/").split("-")[-1]

        item["title"] = response.css("h2.title::text").get(default="").strip()
        item["subtitle"] = response.css("p.subtitle::text").get(default="").strip()
        item["description"] = response.css("p.description::text").get(default="").strip()
        item["status"] = response.css(".availability::text").get(default="").strip()

        item["price_usd"] = response.css("h3.price .amount::attr(data-price)").get()

        item["brand"] = response.css("td.make::text").get(default="").strip()
        item["model"] = response.css("td.model::text").get(default="").strip()
        item["year"] = response.css("td.year::text").get(default="").strip()
        item["engine"] = response.css("td.engine::text").get(default="").strip()
        item["drivetrain"] = response.css("td.drivetrain::text").get(default="").strip()
        item["power"] = response.css("td.power::text").get(default="").strip()
        item["country"] = response.css("td.country::text").get(default="").strip()

        mileage_raw = response.css("td.mileage::text").get(default="")
        item["mileage_km"] = re.sub(r"[^\d]", "", mileage_raw) or None

        item["condition"] = response.css("td.condition::text").get(default="").strip()
        item["exterior_color"] = response.css("td.exterior-color::text").get(default="").strip()
        item["interior_color"] = response.css("td.interior-color::text").get(default="").strip()
        item["body_style"] = response.css("td.body-style::text").get(default="").strip()

        item["rarity_rating"] = response.css(".rarity-rating::attr(data-rating)").get()
        item["color_options"] = response.css(".color-swatches button.swatch::attr(value)").getall()
        item["tags"] = [t.strip() for t in response.css(".tags .tag::text").getall()]

        item["seller_name"] = response.css(".seller-info .seller::text").get(default="").strip()
        item["last_updated"] = response.css(".seller-info .last-updated::text").get(default="").strip()

        images = response.css(".img-thumbnails img::attr(src)").getall()
        if not images:
            images = response.css("#main-product-image::attr(src)").getall()
        item["image_urls"] = [response.urljoin(u) for u in images if u]

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
        key = f"raw/manifests/{date_path}/{self.name}_manifest.json"

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