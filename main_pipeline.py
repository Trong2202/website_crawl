import sys
import re
import uuid
from datetime import datetime
from typing import List, Dict, Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from utils.logger import get_logger
from utils.helpers import (
    read_brands_from_file,
    delay_request,
    make_request,
    parse_html,
    normalize_brand_name,
    format_price,
    extract_bought_value,
)
from database.database_handler import DatabaseHandler
import config

logger = get_logger()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def build_product_id(source_name: str, url: str) -> str:
    """Sinh product_id dựa trên URL (tự động nếu thiếu)."""
    clean_url = (url or "").split("?")[0].rstrip("/")
    slug = clean_url.split("/")[-1] if clean_url else ""
    slug = re.sub(r"[^a-zA-Z0-9\-]+", "-", slug).strip("-")
    if not slug:
        slug = uuid.uuid4().hex[:10]
    product_id = f"{source_name}-{slug}".lower()
    if len(product_id) > 100:
        max_slug_len = max(1, 100 - len(source_name) - 1)
        slug = slug[:max_slug_len].rstrip("-")
        product_id = f"{source_name}-{slug}".lower()
    return product_id


def text_or_empty(element: BeautifulSoup) -> str:
    if not element:
        return ""
    return " ".join(element.stripped_strings)


def extract_lamthaocosmetics_products(soup: BeautifulSoup, brand: str) -> List[Dict[str, Any]]:
    products = []
    for card in soup.select("div.product-inner"):
        title_link = card.select_one("h3.titleproduct a")
        if not title_link:
            continue

        relative_url = title_link.get("href", "").strip()
        url = urljoin(config.WEBSITE_1_BASE, relative_url)
        name = title_link.get_text(strip=True)
        if not url or not name:
            continue

        price_text = text_or_empty(card.select_one("span.price-del"))
        price_sale_text = text_or_empty(card.select_one("span.price"))
        bought_text = text_or_empty(card.select_one("div.bottomloopend21"))

        price_value = int(format_price(price_text)) if price_text else 0
        price_sale_value = int(format_price(price_sale_text)) if price_sale_text else 0
        bought_value = extract_bought_value(bought_text)

        product = {
            "product_id": build_product_id(config.WEBSITE_1_NAME, url),
            "source_name": config.WEBSITE_1_NAME,
            "data": {
                "brand": brand,
                "category": "",
                "name": name,
                "price_text": price_text,
                "price": price_value,
                "price_sale_text": price_sale_text,
                "price_sale": price_sale_value,
                "bought_text": bought_text,
                "bought": bought_value,
                "url": url,
            },
        }
        products.append(product)
    return products


def extract_thegioiskinfood_products(soup: BeautifulSoup, brand: str) -> List[Dict[str, Any]]:
    products = []
    for card in soup.select("div.proLoop"):
        title_link = card.select_one("p.productName a")
        if not title_link:
            continue

        relative_url = title_link.get("href", "").strip()
        url = urljoin(config.WEBSITE_2_BASE, relative_url)
        name = title_link.get_text(strip=True)
        if not url or not name:
            continue

        brand_text = text_or_empty(card.select_one(".loopvendor .fill-vendor")) or brand
        price_text = text_or_empty(card.select_one("p.productPrice del"))
        price_sale_text = text_or_empty(card.select_one("p.productPrice b"))
        bought_text = text_or_empty(card.select_one(".productLoop-sold-qtt"))

        price_value = int(format_price(price_text)) if price_text else 0
        price_sale_value = int(format_price(price_sale_text)) if price_sale_text else 0
        bought_value = extract_bought_value(bought_text)

        product = {
            "product_id": build_product_id(config.WEBSITE_2_NAME, url),
            "source_name": config.WEBSITE_2_NAME,
            "data": {
                "brand": brand_text,
                "category": "",
                "name": name,
                "price_text": price_text,
                "price": price_value,
                "price_sale_text": price_sale_text,
                "price_sale": price_sale_value,
                "bought_text": bought_text,
                "bought": bought_value,
                "url": url,
            },
        }
        products.append(product)
    return products


def crawl_brand_products(brand: str, db_handler: DatabaseHandler, sessions: Dict[str, uuid.UUID]) -> Dict[str, int]:
    stats = {"crawled": 0, "saved": 0}
    brand_normalized = normalize_brand_name(brand)

    # Website 1
    try:
        url_1 = config.WEBSITE_1_PRODUCTS.format(brand=brand_normalized)
        logger.info(f"Crawl {brand} từ {config.WEBSITE_1_NAME}: {url_1}")
        response = make_request(url_1)
        if response:
            soup = parse_html(response.text)
            if soup:
                products = extract_lamthaocosmetics_products(soup, brand)
                logger.info(f"{config.WEBSITE_1_NAME}: tìm thấy {len(products)} sản phẩm")
                for product in products:
                    stats["crawled"] += 1
                    if db_handler.insert_product(sessions[config.WEBSITE_1_NAME], product):
                        stats["saved"] += 1
        delay_request()
    except Exception as exc:
        logger.error(f"Lỗi crawl {brand} từ {config.WEBSITE_1_NAME}: {exc}")

    # Website 2
    try:
        url_2 = config.WEBSITE_2_PRODUCTS.format(brand=brand_normalized)
        logger.info(f"Crawl {brand} từ {config.WEBSITE_2_NAME}: {url_2}")
        response = make_request(url_2)
        if response:
            soup = parse_html(response.text)
            if soup:
                products = extract_thegioiskinfood_products(soup, brand)
                logger.info(f"{config.WEBSITE_2_NAME}: tìm thấy {len(products)} sản phẩm")
                for product in products:
                    stats["crawled"] += 1
                    if db_handler.insert_product(sessions[config.WEBSITE_2_NAME], product):
                        stats["saved"] += 1
        delay_request()
    except Exception as exc:
        logger.error(f"Lỗi crawl {brand} từ {config.WEBSITE_2_NAME}: {exc}")

    return stats


def run_pipeline():
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("BẮT ĐẦU CRAWL PIPELINE - MỸ PHẨM")
    logger.info("=" * 80)

    brands = read_brands_from_file()
    if not brands:
        logger.error("Không có brand nào để crawl")
        return

    logger.info(f"Sẽ crawl {len(brands)} brands: {', '.join(brands)}")

    db = DatabaseHandler()
    sessions = {}
    pipeline_failed = False

    try:
        sessions[config.WEBSITE_1_NAME] = db.create_session(config.WEBSITE_1_NAME)
        sessions[config.WEBSITE_2_NAME] = db.create_session(config.WEBSITE_2_NAME)
    except Exception:
        logger.error("Không tạo được crawl session")
        raise

    total_crawled = 0
    total_saved = 0
    failed_brands = []

    try:
        for idx, brand in enumerate(brands, 1):
            logger.info(f"\n{'=' * 80}")
            logger.info(f"[{idx}/{len(brands)}] Brand: {brand}")
            logger.info(f"{'=' * 80}")
            try:
                stats = crawl_brand_products(brand, db, sessions)
                total_crawled += stats["crawled"]
                total_saved += stats["saved"]
                logger.success(f"{brand}: Crawl {stats['crawled']}, lưu {stats['saved']} sản phẩm mới")
            except Exception as exc:
                failed_brands.append(brand)
                logger.error(f"Lỗi xử lý brand {brand}: {exc}")

            if idx < len(brands):
                delay_request(config.REQUEST_DELAY * 2)

    except KeyboardInterrupt:
        pipeline_failed = True
        logger.warning("\nNgười dùng dừng pipeline (Ctrl+C)")
        raise
    except Exception as exc:
        pipeline_failed = True
        logger.error(f"Lỗi nghiêm trọng: {exc}")
        raise
    finally:
        status = "failed" if pipeline_failed else "completed"
        for source_name, session_id in sessions.items():
            db.complete_session(session_id, status)

    duration = datetime.now() - start_time
    print("\n" + "=" * 80)
    print("BÁO CÁO TỔNG KẾT")
    print("=" * 80)
    print(f"Thời gian: {duration}")
    print(f"Brands xử lý: {len(brands)}")
    print(f"Brands thất bại: {len(failed_brands)}")
    print(f"Sản phẩm crawl: {total_crawled}")
    print(f"Sản phẩm lưu mới: {total_saved}")
    print(f"Duplicate bỏ qua: {total_crawled - total_saved}")
    if failed_brands:
        print(f"\nBrands thất bại: {', '.join(failed_brands)}")
    print("=" * 80 + "\n")
    logger.success("HOÀN THÀNH PIPELINE")


if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        sys.exit(1)
