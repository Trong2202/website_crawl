"""
Crawlers package - Thu thập dữ liệu từ các website
"""

from crawlers.listing_crawler import (
    crawl_listing_lamthaocosmetics,
    crawl_listing_thegioiskinfood,
)

from crawlers.product_crawler import (
    crawl_product_detail_lamthaocosmetics,
    crawl_product_detail_thegioiskinfood,
)

from crawlers.review_crawler import (
    crawl_reviews_thegioiskinfood,
)

__all__ = [
    "crawl_listing_lamthaocosmetics",
    "crawl_listing_thegioiskinfood",
    "crawl_product_detail_lamthaocosmetics",
    "crawl_product_detail_thegioiskinfood",
    "crawl_reviews_thegioiskinfood",
]
# Hiện tại logic crawl đã được tích hợp trong crawl_brands.py và crawl_products.py
