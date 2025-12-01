"""
Crawler cho product detail pages - BƯỚC 3
Parse HTML và transform JSON theo format mong muốn
CHỈ thêm fields NẾU HTML có data - không tạo field giả
"""
import re
import json
from typing import Dict, Any, Optional
from uuid import UUID
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from utils.logger import get_logger
from utils.helpers import make_request, parse_html
import config

logger = get_logger()


def format_price_vnd(price: int, source: str = "lamthaocosmetics") -> str:
    """Format price thành string VND"""
    if source == "lamthaocosmetics":
        return f"{int(price/1000):,}".replace(",", ".") + ".000 ₫"
    else:
        return f"{int(price/1000):,}".replace(",", ".") + ".000₫"


def calculate_discount_percent(price: int, market_price: int) -> int:
    """Calculate discount percentage"""
    if not market_price or market_price == 0 or price >= market_price:
        return 0
    return int(((price - market_price) / market_price) * 100)


def transform_lamthao_json(raw_json: Dict, bought_count: int = 0) -> Dict:
    """
    Transform Haravan JSON từ lamthaocosmetics sang format mong muốn
    Chỉ giữ các fields có trong json.txt
    """
    # Price từ Haravan format (VND * 100)
    price = int(raw_json.get("price_min", 0) / 100)
    compare_price = int(raw_json.get("compare_at_price_min", 0) / 100)
    
    result = {
        "id": raw_json.get("id"),
        "sku": None,
        "name": raw_json.get("title", ""),
        "url": raw_json.get("handle", ""),
        "brand": {"name": raw_json.get("vendor", "")},
        "category_name": raw_json.get("type", ""),
        "price": price,
        "final_price": format_price_vnd(price * 100, "lamthaocosmetics"),
        "market_price": format_price_vnd(compare_price * 100, "lamthaocosmetics"),
        "discount_market_percent": calculate_discount_percent(price, compare_price),
        "bought": bought_count,
        "can_buy": raw_json.get("available", False),
        "is_saleable": raw_json.get("available", False),
    }
    
    # Variants processing
    variants = raw_json.get("variants", [])
    if variants and len(variants) > 1:
        # Sản phẩm có nhiều variants
        transformed_variants = []
        for v in variants:
            v_price = int(v.get("price", 0) / 100)
            v_compare = int(v.get("compare_at_price", 0) / 100)
            
            variant = {
                "id": v.get("id"),
                "title": v.get("title", ""),
                "sku": v.get("sku"),
                "barcode": v.get("barcode"),
                "available": v.get("available", False),
                "price": v_price,
                "final_price": format_price_vnd(v_price * 100, "lamthaocosmetics"),
                "market_price": format_price_vnd(v_compare * 100, "lamthaocosmetics"),
                "qty": int(v.get("inventory_quantity", 0)),
                "old_qty": int(v.get("old_inventory_quantity", 0))
            }
            transformed_variants.append(variant)
        
        result["variant"] = {
            "has_variants": True,
            "options": raw_json.get("options", []),
            "variants": transformed_variants
        }
    elif variants and len(variants) == 1:
        # Sản phẩm đơn (chỉ 1 variant): hoist variant fields lên product-level
        single_variant = variants[0]
        result["sku"] = single_variant.get("sku")
        result["qty"] = int(single_variant.get("inventory_quantity", 0))
        result["old_qty"] = int(single_variant.get("old_inventory_quantity", 0))
    
    return result


def parse_thegioiskinfood_html(soup: BeautifulSoup, product_id: str, product_url: str) -> Dict:
    """
    Parse HTML thực tế từ thegioiskinfood
    CHỈ thêm field NẾU HTML có data
    """
    result = {}
    
    # Product ID - extract numeric part from formatted string "thegioiskinfood-1234567890"
    if "-" in product_id:
        numeric_part = product_id.split("-")[-1]
        result["id"] = int(numeric_part) if numeric_part.isdigit() else None
    else:
        result["id"] = int(product_id) if product_id.isdigit() else None
    result["sku"] = None
    
    # Name
    name_elem = soup.select_one("h1.page-product-info-title")
    result["name"] = name_elem.get_text(strip=True) if name_elem else ""
    
    # URL
    result["url"] = product_url.lstrip("/products/") if product_url.startswith("/products/") else product_url
    
    # Brand
    brand_elem = soup.select_one("a.fill-vendor span")
    result["brand"] = {"name": brand_elem.get_text(strip=True) if brand_elem else ""}
    
    # Price - parse từ HTML
    old_price_elem = soup.select_one(".page-product-info-oldprice span")
    new_price_elem = soup.select_one(".page-product-info-newprice span")
    
    old_price_text = old_price_elem.get_text(strip=True) if old_price_elem else ""
    new_price_text = new_price_elem.get_text(strip=True) if new_price_elem else ""
    
    # Remove non-digits to get price
    market_price = int(re.sub(r'[^\d]', '', old_price_text)) if old_price_text else 0
    price = int(re.sub(r'[^\d]', '', new_price_text)) if new_price_text else 0
    
    result["price"] = price
    result["final_price"] = new_price_text if new_price_text else format_price_vnd(price, "thegioiskinfood")
    result["market_price"] = old_price_text if old_price_text else format_price_vnd(market_price, "thegioiskinfood")
    result["discount_market_percent"] = calculate_discount_percent(price, market_price)
    
    # Bought - parse số
    bought_elem = soup.select_one(".sold-qtt strong")
    result["bought"] = int(bought_elem.get_text(strip=True)) if bought_elem and bought_elem.get_text(strip=True).isdigit() else 0
    
    # Can buy & is saleable - assume true nếu có giá
    result["can_buy"] = True
    result["is_saleable"] = True
    
    # Variants - parse từ select options
    variant_select = soup.select("#product-select option")
    if variant_select and len(variant_select) > 1:
        # Sản phẩm có nhiều variants
        options_raw = soup.select(".single-option-selector option")
        option_name = "Tiêu đề"  # Default
        
        variants_list = []
        for option in variant_select:
            variant_id = option.get("value")
            variant_title = option.get("data-title", "")
            variant_sku = option.get("data-sku", "")
            variant_price_str = option.get("data-price", "0")
            variant_max_order = option.get("data-max-order", "0")
            variant_max = option.get("data-max", "0")
            
            # Price format: "36900000" = 369000.00 VND (price * 100)
            variant_price = int(variant_price_str) // 100 if variant_price_str.isdigit() else price
            
            # Check if available (max_order > 0 usually means available)
            is_available = int(variant_max_order) > 0 if variant_max_order.isdigit() else True
            
            variant = {
                "id": int(variant_id) if variant_id and variant_id.isdigit() else None,
                "title": variant_title,
                "sku": variant_sku,
                "barcode": variant_sku,  # Usually same as SKU
                "available": is_available,
                "price": variant_price,
                "final_price": format_price_vnd(variant_price, "thegioiskinfood"),
                "qty": int(variant_max) if variant_max.isdigit() else 999,
                "max_order": int(variant_max_order) if variant_max_order.isdigit() else 0
            }
            variants_list.append(variant)
        
        result["variant"] = {
            "has_variants": True,
            "options": [option_name],
            "variants": variants_list
        }
    elif variant_select and len(variant_select) == 1:
        # Sản phẩm đơn (chỉ 1 option): hoist variant fields lên product-level
        single_option = variant_select[0]
        result["sku"] = single_option.get("data-sku", "")
        result["qty"] = int(single_option.get("data-max", 0)) if single_option.get("data-max", "0").isdigit() else 0
        result["max_order"] = int(single_option.get("data-max-order", 0)) if single_option.get("data-max-order", "0").isdigit() else 0
    
    return result


def crawl_product_detail_lamthaocosmetics(listing: Dict[str, Any], session_id: UUID, db) -> Optional[Dict[str, Any]]:
    """
    Crawl và transform product detail từ lamthaocosmetics
    """
    product_url = listing['product_url']
    product_id = listing['product_id']
    
    full_url = urljoin(config.WEBSITE_1_BASE, product_url)
    logger.info(f"[PRODUCT] Crawl detail: {full_url}")
    
    try:
        response = make_request(full_url)
        if not response:
            return None
        
        soup = parse_html(response.text)
        if not soup:
            return None
        
        # Extract JSON từ window.F1GENZ_vars.product.data
        raw_json = None
        for script in soup.find_all('script'):
            if not script.string or 'window.F1GENZ_vars' not in script.string:
                continue
            
            text = script.string
            data_start = text.find('product:')
            if data_start == -1:
                continue
            
            data_key_pos = text.find('data:', data_start)
            if data_key_pos == -1:
                continue
            
            brace_start = text.find('{', data_key_pos)
            if brace_start == -1:
                continue
            
            # Count braces
            depth = 0
            i = brace_start
            while i < len(text):
                if text[i] == '{':
                    depth += 1
                elif text[i] == '}':
                    depth -= 1
                    if depth == 0:
                        json_str = text[brace_start:i+1]
                        try:
                            raw_json = json.loads(json_str)
                            break
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON parse error: {e}")
                            break
                i += 1
            
            if raw_json:
                break
        
        if not raw_json:
            logger.warning(f"[PRODUCT] Không tìm thấy product JSON")
            return None
        
        # Parse bought count
        bought_count = 0
        bought_elem = soup.select_one(".bottomloopend21")
        if bought_elem:
            bought_text = bought_elem.get_text(strip=True)
            match = re.search(r'(\d+)', bought_text)
            if match:
                bought_count = int(match.group(1))
        
        # Transform
        transformed_json = transform_lamthao_json(raw_json, bought_count)
        
        # Save
        product_data = {
            "product_id": product_id,
            "source_name": config.WEBSITE_1_NAME,
            "data": transformed_json
        }
        
        if db.insert_product(session_id, product_data):
            logger.success(f"[PRODUCT] Lưu thành công: {transformed_json.get('name', '')[:50]}")
            return transformed_json
        else:
            logger.info(f"[PRODUCT] Duplicate")
            return None
        
    except Exception as exc:
        logger.error(f"[PRODUCT] Lỗi: {exc}")
        return None


def crawl_product_detail_thegioiskinfood(listing: Dict[str, Any], session_id: UUID, db) -> Optional[Dict[str, Any]]:
    """
    Crawl và parse HTML từ thegioiskinfood
    """
    product_url = listing['product_url']
    product_id = listing['product_id']
    
    full_url = urljoin(config.WEBSITE_2_BASE, product_url)
    logger.info(f"[PRODUCT] Crawl detail: {full_url}")
    
    try:
        response = make_request(full_url)
        if not response:
            return None
        
        soup = parse_html(response.text)
        if not soup:
            return None
        
        # Parse HTML
        transformed_json = parse_thegioiskinfood_html(soup, product_id, product_url)
        
        # Save
        product_data = {
            "product_id": product_id,
            "source_name": config.WEBSITE_2_NAME,
            "data": transformed_json
        }
        
        if db.insert_product(session_id, product_data):
            logger.success(f"[PRODUCT] Lưu thành công: {transformed_json.get('name', '')[:50]}")
            return {
                "id": transformed_json.get('id'),
                "product_id": product_id,
                "name": transformed_json.get('name', '')
            }
        else:
            logger.info(f"[PRODUCT] Duplicate")
            return None
        
    except Exception as exc:
        logger.error(f"[PRODUCT] Lỗi: {exc}")
        return None
