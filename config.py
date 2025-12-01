"""
Cấu hình cho pipeline crawl dữ liệu mỹ phẩm
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "raw")

# Website Configuration
WEBSITE_1_BASE = "https://lamthaocosmetics.vn"
WEBSITE_1_BRANDS = f"{WEBSITE_1_BASE}/collections/all"
WEBSITE_1_PRODUCTS = f"{WEBSITE_1_BASE}/collections/vendors?q={{brand}}&page={{page}}"
WEBSITE_1_NAME = "lamthaocosmetics"

WEBSITE_2_BASE = "https://thegioiskinfood.com"
WEBSITE_2_BRANDS = f"{WEBSITE_2_BASE}/pages/thuong-hieu"
WEBSITE_2_PRODUCTS = f"{WEBSITE_2_BASE}/collections/{{brand}}"
WEBSITE_2_NAME = "thegioiskinfood"

# Brands file
BRANDS_FILE = "brands.txt"

# Crawl Configuration
REQUEST_DELAY = 0.5  # Default delay (seconds) - reduced for performance
MAX_RETRIES = 3  # Số lần retry khi request thất bại
TIMEOUT = 30  # Timeout cho mỗi request (seconds)

# Site-specific delays (seconds)
WEBSITE_1_DELAY = 0.5  # lamthaocosmetics - faster site
WEBSITE_2_DELAY = 1.0  # thegioiskinfood - more conservative

# Review-specific optimization (BIG DATA)
MAX_REVIEW_CONCURRENT_PAGES = 20  # Fetch multiple pages concurrently per product
REVIEW_DELAY = 0.3                 # Faster delay for review API

# Concurrent processing settings
MAX_CONCURRENT_REQUESTS = 8  # Max concurrent requests (5-10 range)
MAX_CONCURRENT_BRANDS = 2  # Process multiple brands in parallel

# Headers để giả lập browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0"
}

# Review API Configuration (thegioiskinfood only)
REVIEW_API_BASE = "https://customer-reviews-api.haravan.app/api/buyer/product_rating"
REVIEW_API_ORG_ID = "1000006063"
REVIEW_API_LIMIT = 10

