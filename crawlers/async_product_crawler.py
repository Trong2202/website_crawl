"""
Async product crawler for concurrent processing
"""
import asyncio
from typing import Dict, Any, Optional
from uuid import UUID
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from utils.logger import get_logger
from utils.async_helpers import make_request_with_semaphore
from utils.helpers import parse_html
from crawlers.product_crawler import (
    parse_thegioiskinfood_html,
    transform_lamthao_json
)
import config
import json
import re

logger = get_logger()


async def crawl_product_detail_thegioiskinfood_async(
    listing: Dict[str, Any], 
    session_id: UUID, 
    db,
    semaphore: asyncio.Semaphore
) -> Optional[Dict[str, Any]]:
    """
    Async version of crawl_product_detail_thegioiskinfood
    
    Args:
        listing: Listing data
        session_id: Session UUID
        db: Database handler (thread-safe)
        semaphore: Semaphore for concurrency control
        
    Returns:
        Product data dict or None
    """
    product_url = listing['product_url']
    product_id = listing['product_id']
    
    full_url = urljoin(config.WEBSITE_2_BASE, product_url)
    logger.info(f"[ASYNC PRODUCT] Crawl detail: {full_url}")
    
    try:
        # Async request with site-specific delay
        html_content = await make_request_with_semaphore(
            full_url, 
            semaphore, 
            delay=config.WEBSITE_2_DELAY
        )
        
        if not html_content:
            return None
        
        # Parse HTML (sync operation, but fast)
        soup = parse_html(html_content)
        if not soup:
            return None
        
        # Parse data
        transformed_json = parse_thegioiskinfood_html(soup, product_id, product_url)
        
        # Save to database (sync operation - database is thread-safe)
        product_data = {
            "product_id": product_id,
            "source_name": config.WEBSITE_2_NAME,
            "data": transformed_json
        }
        
        
        if db.insert_product(session_id, product_data):
            logger.success(f"[ASYNC PRODUCT] Saved: {transformed_json.get('name', '')[:50]}")
        else:
            logger.info(f"[ASYNC PRODUCT] Duplicate (already in DB): {product_id}")
        
        # Return product info regardless of duplicate status for review crawling
        return {
            "id": transformed_json.get('id'),
            "product_id": product_id,
            "name": transformed_json.get('name', '')
        }
            
    except Exception as exc:
        logger.error(f"[ASYNC PRODUCT] Error {product_id}: {exc}")
        return None


async def crawl_product_detail_lamthaocosmetics_async(
    listing: Dict[str, Any],
    session_id: UUID,
    db,
    semaphore: asyncio.Semaphore
) -> Optional[Dict[str, Any]]:
    """
    Async version of crawl_product_detail_lamthaocosmetics
    
    Args:
        listing: Listing data
        session_id: Session UUID
        db: Database handler (thread-safe)
        semaphore: Semaphore for concurrency control
        
    Returns:
        Product data dict or None
    """
    product_url = listing['product_url']
    product_id = listing['product_id']
    
    full_url = urljoin(config.WEBSITE_1_BASE, product_url)
    logger.info(f"[ASYNC PRODUCT] Crawl detail: {full_url}")
    
    try:
        # Async request with site-specific delay
        html_content = await make_request_with_semaphore(
            full_url,
            semaphore,
            delay=config.WEBSITE_1_DELAY
        )
        
        if not html_content:
            return None
        
        soup = parse_html(html_content)
        if not soup:
            return None
        
        # Extract JSON from window.F1GENZ_vars.product.data
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
                        except json.JSONDecodeError:
                            break
                i += 1
            
            if raw_json:
                break
        
        if not raw_json:
            logger.warning(f"[ASYNC PRODUCT] No JSON found: {product_id}")
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
            logger.success(f"[ASYNC PRODUCT] Saved: {transformed_json.get('name', '')[:50]}")
            return transformed_json
        else:
            logger.info(f"[ASYNC PRODUCT] Duplicate: {product_id}")
            return None
            
    except Exception as exc:
        logger.error(f"[ASYNC PRODUCT] Error {product_id}: {exc}")
        return None


async def crawl_products_concurrent(
    listings: list,
    session_id: UUID,
    db,
    source_name: str
) -> Dict[str, int]:
    """
    Crawl multiple products concurrently
    
    Args:
        listings: List of listing dicts
        session_id: Session UUID
        db: Database handler
        source_name: Source name (lamthaocosmetics/thegioiskinfood)
        
    Returns:
        Stats dict with counts
    """
    if not listings:
        return {"products": 0, "reviews": 0}
    
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
    
    # Choose crawler based on source
    if source_name == config.WEBSITE_1_NAME:
        crawler = crawl_product_detail_lamthaocosmetics_async
    else:
        crawler = crawl_product_detail_thegioiskinfood_async
    
    # Create tasks
    tasks = [
        crawler(listing, session_id, db, semaphore)
        for listing in listings
    ]
    
    # Execute concurrently
    logger.info(f"[CONCURRENT] Processing {len(tasks)} products for {source_name}")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Count successful products
    products_count = sum(1 for r in results if r and not isinstance(r, Exception))
    errors_count = sum(1 for r in results if isinstance(r, Exception))
    
    logger.success(f"[CONCURRENT] {source_name}: {products_count} products, {errors_count} errors")
    
    return {"products": products_count, "results": [r for r in results if r and not isinstance(r, Exception)]}
