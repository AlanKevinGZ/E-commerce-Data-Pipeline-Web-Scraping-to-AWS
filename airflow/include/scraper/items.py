import scrapy


class CarItem(scrapy.Item):
    sku = scrapy.Field()
    product_url = scrapy.Field()
    title = scrapy.Field()
    subtitle = scrapy.Field()
    description = scrapy.Field()
    brand = scrapy.Field()
    model = scrapy.Field()
    year = scrapy.Field()
    engine = scrapy.Field()
    drivetrain = scrapy.Field()
    power = scrapy.Field()
    country = scrapy.Field()
    mileage_km = scrapy.Field()
    condition = scrapy.Field()
    exterior_color = scrapy.Field()
    interior_color = scrapy.Field()
    body_style = scrapy.Field()
    status = scrapy.Field()          # Disponible / Reservado / Vendido
    price_usd = scrapy.Field()
    rarity_rating = scrapy.Field()
    color_options = scrapy.Field()
    tags = scrapy.Field()
    seller_name = scrapy.Field()
    last_updated = scrapy.Field()
    image_urls = scrapy.Field()      # requerido por ImagesPipeline
    images = scrapy.Field()          # resultado generado por ImagesPipeline
    scraped_at = scrapy.Field()



class ProductItem(scrapy.Item):
    sku = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    price_usd = scrapy.Field()
    review_count = scrapy.Field()
    category = scrapy.Field()
    subcategory = scrapy.Field()
    product_url = scrapy.Field()
    image_urls = scrapy.Field()
    images = scrapy.Field()
    scraped_at = scrapy.Field()