"""
Crawler cho listing pages (collection pages) - BƯỚC 2
Thu thập product_id và brand_id từ collection pages và lưu vào listing_api

IMPORTANT: listing_api chỉ có product_id và brand_id, KHÔNG có data column
Product_id phải là số thuần như "1067440535"
"""
import re
from typing import List, Dict, Any
from uuid import UUID
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from utils.logger import get_logger
from utils.helpers import make_request, parse_html, normalize_brand_name, delay_request
import config

logger = get_logger()


def crawl_listing_lamthaocosmetics(brand: str, session_id: UUID, db) -> List[Dict[str, Any]]:
    """
    Crawl collection page từ lamthaocosmetics với pagination
    Extract: product_id (numeric) và brand_id
    """
    brand_normalized = normalize_brand_name(brand)
    
    listings = []
    page = 1
    max_pages = 100  # Safety limit
    
    logger.info(f"[LISTING] Bắt đầu crawl {brand} từ {config.WEBSITE_1_NAME}")
    
    while page <= max_pages:
        # Build URL with pagination
        url = config.WEBSITE_1_PRODUCTS.format(brand=brand_normalized, page=page)
        
        logger.info(f"[LISTING] Crawl {brand} trang {page}: {url}")
        
        try:
            response = make_request(url)
            if not response:
                logger.warning(f"[LISTING] Không nhận được response (page {page})")
                break
            
            soup = parse_html(response.text)
            if not soup:
                logger.warning(f"[LISTING] Không parse được HTML (page {page})")
                break
            
            product_cards = soup.select("div.product-inner")
            
            if not product_cards or len(product_cards) == 0:
                logger.info(f"[LISTING] Hết sản phẩm ở trang {page}")
                break
            
            logger.info(f"[LISTING] {config.WEBSITE_1_NAME} trang {page}: tìm thấy {len(product_cards)} sản phẩm")
            
            page_count = 0
            for card in product_cards:
                title_link = card.select_one("h3.titleproduct a")
                if not title_link:
                    continue
                
                relative_url = title_link.get("href", "").strip()
                name = title_link.get_text(strip=True)
                
                if not relative_url or not name:
                    continue
                
                # Extract product numeric ID từ data-proid attribute
                product_id = card.get("data-proid")
                
                # Fallback: extract từ URL
                if not product_id:
                    logger.warning(f"[LISTING] No data-proid for {name}, skipping")
                    continue
                
                # Extract data theo format json.txt
                # LamThaoCosmetics - Collection Page
                listing_data = {
                    "id": int(product_id) if product_id.isdigit() else None,
                    "sku": None,
                    "name": name,
                    "url": relative_url,
                    "brand": {
                        "name": brand
                    }
                }
                
                # Insert vào database - Full JSON data
                if db.insert_listing(session_id, config.WEBSITE_1_NAME, listing_data):
                    listings.append({
                        "product_id": product_id,
                        "product_url": relative_url,
                    })
                    page_count += 1
            
            logger.success(f"[LISTING] {config.WEBSITE_1_NAME} trang {page}: Lưu {page_count}/{len(product_cards)} listings")
            
            # Next page
            page += 1
            delay_request()  # Delay between pages
            
        except Exception as exc:
            logger.error(f"[LISTING] Lỗi crawl {brand} trang {page}: {exc}")
            break
    
    logger.success(f"[LISTING] {config.WEBSITE_1_NAME}: Hoàn thành {brand} - tổng {len(listings)} listings từ {page-1} trang")
    return listings



def crawl_listing_thegioiskinfood(brand: str, session_id: UUID, db) -> List[Dict[str, Any]]:
    """
    Crawl collection page từ thegioiskinfood
    Extract: product_id (numeric) và brand_id
    """
    brand_normalized = normalize_brand_name(brand)
    url = config.WEBSITE_2_PRODUCTS.format(brand=brand_normalized)
    
    logger.info(f"[LISTING] Crawl {brand} từ {config.WEBSITE_2_NAME}: {url}")
    
    try:
        response = make_request(url)
        if not response:
            return []
        
        soup = parse_html(response.text)
        if not soup:
            return []
        
        listings = []
        product_cards = soup.select("div.proLoop")
        
        logger.info(f"[LISTING] {config.WEBSITE_2_NAME}: tìm thấy {len(product_cards)} sản phẩm")
        
        for card in product_cards:
            title_link = card.select_one("p.productName a")
            if not title_link:
                continue
            
            relative_url = title_link.get("href", "").strip()
            name = title_link.get_text(strip=True)
            
            brand_text_elem = card.select_one(".loopvendor .fill-vendor")
            brand_text = brand_text_elem.get_text(strip=True) if brand_text_elem else brand
            
            if not relative_url or not name:
                continue
            
            # Extract product numeric ID từ data-product-id hoặc data-id
            product_id = None
            
            # Method 1: hrv-crv-container (review widget)
            review_container = card.select_one("[data-product-id]")
            if review_container:
                product_id = review_container.get("data-product-id")
            
            # Method 2: button favorites
            if not product_id:
                fav_button = card.select_one("button.js-favorites[data-id]")
                if fav_button:
                    product_id = fav_button.get("data-id")
            
            if not product_id:
                logger.warning(f"[LISTING] No product ID for {name}, skipping")
                continue
            
            # Extract price info
            price_elem = card.select_one(".proPrice .pro-price")
            market_price_elem = card.select_one(".proPrice .pro-price-del .compare-price")
            
            final_price_str = price_elem.get_text(strip=True) if price_elem else "0₫"
            market_price_str = market_price_elem.get_text(strip=True) if market_price_elem else final_price_str
            
            # Parse numeric price (simple version)
            try:
                price_val = int(re.sub(r"[^\d]", "", final_price_str))
            except:
                price_val = 0
                
            # Availability
            sold_out = card.select_one(".sold-out") is not None
            
            # Construct JSON data
            listing_data = {
                "id": int(product_id) if product_id and product_id.isdigit() else None,
                "name": name,
                "url": relative_url,
                "brand": {
                    "name": brand_text,
                    "url": f"/collections/all?vendors={brand_text}"
                }
            }
            
            # Insert vào database - Full JSON data
            if db.insert_listing(session_id, config.WEBSITE_2_NAME, listing_data):
                listings.append({
                    "product_id": product_id,
                    "product_url": relative_url,
                })
        
        logger.success(f"[LISTING] {config.WEBSITE_2_NAME}: Lưu {len(listings)}/{len(product_cards)} listings")
        return listings
        
    except Exception as exc:
        logger.error(f"[LISTING] Lỗi crawl {brand}: {exc}")
        return []
