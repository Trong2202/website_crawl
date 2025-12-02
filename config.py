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
REQUEST_DELAY = 0.3  # Độ trễ mặc định (giây) - đã tối ưu hiệu suất
MAX_RETRIES = 3  # Số lần retry khi request thất bại
TIMEOUT = 30  # Timeout cho mỗi request (giây)

# Độ trễ riêng cho từng website (giây) - ĐÃ TỐI ƯU
WEBSITE_1_DELAY = 0.3  # lamthaocosmetics - giảm từ 0.5s
WEBSITE_2_DELAY = 0.5  # thegioiskinfood - giảm từ 1.0s (API front-end, không chặn bot)

# Tối ưu riêng cho reviews (DỮ LIỆU LỚN) - ĐÃ TỐI ƯU
MAX_REVIEW_CONCURRENT_PAGES = 20 # Số trang reviews crawl đồng thời (tăng từ 10 cho API reviews chậm)
REVIEW_DELAY = 0.2               # Độ trễ giữa các request reviews - giảm từ 0.3s (API chậm, tăng concurrency)

# Cài đặt xử lý đồng thời - ĐÃ TỐI ƯU
MAX_CONCURRENT_REQUESTS = 20 # Số requests đồng thời tối đa - tăng từ 8 (2.5 brands × 10 products)
MAX_CONCURRENT_BRANDS = 5  # Số brands xử lý đồng thời - tăng từ 3 (crawl 15 brands nhanh hơn, KHÔNG giới hạn tổng số brands)

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

# Cấu hình API Reviews (chỉ cho thegioiskinfood)
REVIEW_API_BASE = "https://customer-reviews-api.haravan.app/api/buyer/product_rating"
REVIEW_API_ORG_ID = "1000006063"
REVIEW_API_LIMIT = 10  # Số reviews mỗi trang API trả về

