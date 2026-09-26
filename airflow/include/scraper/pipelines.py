from datetime import datetime, timezone

import scrapy
from scrapy.pipelines.images import ImagesPipeline


class CarImagesPipeline(ImagesPipeline):
    """Descarga imágenes y las organiza como /YYYY/MM/DD/site_name/sku_N.jpg"""

    def get_media_requests(self, item, info):
        for index, image_url in enumerate(item.get("image_urls", []), start=1):
            yield scrapy.Request(
                image_url,
                meta={"sku": item["sku"], "image_index": index},
            )

    def file_path(self, request, response=None, info=None, *, item=None):
        date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        site_name = info.spider.name
        sku = request.meta["sku"]
        image_index = request.meta["image_index"]

        return f"{site_name}/{date_path}/{sku}_{image_index}.jpg"

    def image_downloaded(self, response, request, info, *, item=None):
        # acumula bytes de imágenes para el manifest diario
        info.spider.crawler.stats.inc_value("image_bytes_total", len(response.body))
        return super().image_downloaded(response, request, info, item=item)