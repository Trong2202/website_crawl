"""
OPTIMIZED Async review crawler for BIG DATA
Fetches multiple review pages concurrently for maximum speed
"""
import asyncio
from typing import Optional, List, Dict, Any
from uuid import UUID

from utils.logger import get_logger
from utils.async_helpers import make_request_with_semaphore
import config
import json

logger = get_logger()


async def fetch_review_page(
    product_numeric_id: int,
    page: int,
    semaphore: asyncio.Semaphore
) -> Optional[Dict[str, Any]]:
    """
    Fetch a single review page
    
    Args:
        product_numeric_id: Numeric product ID
        page: Page number
        semaphore: Semaphore for rate limiting
        
    Returns:
        Review data dict or None
    """
    api_url = (
        f"{config.REVIEW_API_BASE}"
        f"?org_id={config.REVIEW_API_ORG_ID}"
        f"&product_id={product_numeric_id}"
        f"&page={page}"
        f"&limit={config.REVIEW_API_LIMIT}"
        f"&source_cr_at=desc"
    )
    
    try:
        response_text = await make_request_with_semaphore(
            api_url,
            semaphore,
            delay=config.REVIEW_DELAY  # Faster delay for reviews
        )
        
        if not response_text:
            return None
        
        try:
            data = json.loads(response_text)
            return {"page": page, "data": data}
        except json.JSONDecodeError as e:
            logger.error(f"[REVIEW] JSON error page {page}: {e}")
            return None
            
    except Exception as exc:
        logger.error(f"[REVIEW] Error fetching page {page}: {exc}")
        return None


async def crawl_reviews_thegioiskinfood_async(
    product_numeric_id: int,
    product_id: str,
    session_id: UUID,
    db,
    semaphore: asyncio.Semaphore
) -> int:
    """
    OPTIMIZED: Fetch all review pages concurrently
    
    Args:
        product_numeric_id: Numeric product ID
        product_id: String product ID
        session_id: Session UUID
        db: Database handler
        semaphore: Semaphore for concurrency control
        
    Returns:
        Number of pages saved
    """
    if not product_numeric_id:
        logger.warning(f"[REVIEW] No product_numeric_id for {product_id}")
        return 0
    
    # Get product snapshot ID
    product_snapshot_id = db.get_latest_product_snapshot_id(product_id)
    if not product_snapshot_id:
        logger.warning(f"[REVIEW] No product snapshot for {product_id}")
        return 0
    
    # Resume capability
    latest_page = db.get_latest_review_page(product_id)
    start_page = latest_page + 1 if latest_page > 0 else 1
    
    logger.info(f"[REVIEW] Start concurrent crawl for product_id={product_numeric_id}, from page {start_page}")
    
    # STEP 1: Fetch first page to get total count
    first_page_result = await fetch_review_page(product_numeric_id, start_page, semaphore)
    
    if not first_page_result:
        logger.warning(f"[REVIEW] No data on first page {start_page}")
        return 0
    
    first_data = first_page_result["data"]
    reviews = first_data.get("list_ratings", [])
    
    if not reviews or len(reviews) == 0:
        logger.info(f"[REVIEW] No reviews found")
        return 0
    
    # Save first page
    review_data = {
        "product_id": product_id,
        "product_snapshot_id": product_snapshot_id,
        "session_id": session_id,
        "pages": start_page,
        "data": first_data
    }
    
    total_saved = 1 if db.insert_review(review_data) else 0
    
    # Calculate total pages
    total_reviews = first_data.get("total", 0)
    total_pages = (total_reviews + config.REVIEW_API_LIMIT - 1) // config.REVIEW_API_LIMIT
    
    logger.info(f"[REVIEW] Product {product_id} has {total_reviews} reviews ({total_pages} pages)")
    
    # Smart early stopping: Check if already crawled all pages
    if latest_page >= total_pages:
        logger.info(f"[REVIEW] Product {product_id}: Already crawled all {total_pages} pages (latest={latest_page})")
        return 0
    
    if total_pages <= start_page:
        logger.success(f"[REVIEW] Only 1 page for {product_id}")
        return total_saved
    
    # STEP 2: Fetch remaining pages CONCURRENTLY
    remaining_pages = list(range(start_page + 1, total_pages + 1))
    
    if not remaining_pages:
        return total_saved
    
    logger.info(f"[REVIEW] Fetching {len(remaining_pages)} pages concurrently for {product_id}")
    
    # Create semaphore for review pages
    review_semaphore = asyncio.Semaphore(config.MAX_REVIEW_CONCURRENT_PAGES)
    
    # Fetch all pages concurrently
    tasks = [
        fetch_review_page(product_numeric_id, page, review_semaphore)
        for page in remaining_pages
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # STEP 3: Save all successful pages
    for result in results:
        if result and not isinstance(result, Exception) and result.get("data"):
            page_num = result["page"]
            page_data = result["data"]
            
            # Check if has reviews
            page_reviews = page_data.get("list_ratings", [])
            if not page_reviews:
                continue
            
            review_data = {
                "product_id": product_id,
                "product_snapshot_id": product_snapshot_id,
                "session_id": session_id,
                "pages": page_num,
                "data": page_data
            }
            
            if db.insert_review(review_data):
                total_saved += 1
    
    logger.success(f"[REVIEW] Saved {total_saved}/{total_pages} pages for {product_id}")
    return total_saved
