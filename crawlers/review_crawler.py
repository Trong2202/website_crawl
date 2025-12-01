"""
Crawler cho reviews - BƯỚC 4
Thu thập reviews từ API (chỉ thegioiskinfood) và lưu vào review_api
"""
from typing import Optional
from uuid import UUID
import time

from utils.logger import get_logger
from utils.helpers import make_request
import config

logger = get_logger()


def crawl_reviews_thegioiskinfood(product_numeric_id: int, product_id: str, session_id: UUID, db) -> int:
    """
    Crawl reviews từ review API (thegioiskinfood)
    Loop qua các trang cho đến khi hết data
    
    Args:
        product_numeric_id: Product ID dạng số (e.g. 1043504950)
        product_id: Product ID string (e.g. "thegioiskinfood-1043504950")
        session_id: UUID của crawl session
        db: DatabaseHandler instance
        
    Returns:
        Số reviews (pages) đã lưu thành công
    """
    if not product_numeric_id:
        logger.warning(f"[REVIEW] Không có product_numeric_id cho {product_id}, skip reviews")
        return 0
    
    # Get product snapshot ID từ database
    product_snapshot_id = db.get_latest_product_snapshot_id(product_id)
    if not product_snapshot_id:
        logger.warning(f"[REVIEW] Không tìm thấy product snapshot cho {product_id}, skip reviews")
        return 0
    
    # Resume capability: Get latest page from DB
    latest_page = db.get_latest_review_page(product_id)
    start_page = latest_page + 1 if latest_page > 0 else 1
    
    total_saved = 0
    max_pages = 100  # Safety limit
    
    logger.info(f"[REVIEW] Bắt đầu crawl reviews cho product_id={product_numeric_id}. Resume from page {start_page} (Latest: {latest_page})")
    
    page = start_page
    while page <= max_pages:
        # Build API URL
        api_url = (
            f"{config.REVIEW_API_BASE}"
            f"?org_id={config.REVIEW_API_ORG_ID}"
            f"&product_id={product_numeric_id}"
            f"&page={page}"
            f"&limit={config.REVIEW_API_LIMIT}"
            f"&source_cr_at=desc"
        )
        
        logger.info(f"[REVIEW] Crawl page {page}: product_id={product_numeric_id}")
        
        try:
            response = make_request(api_url)
            if not response:
                logger.warning(f"[REVIEW] Không nhận được response từ API (page {page})")
                break
            
            try:
                data = response.json()
            except Exception as json_err:
                logger.error(f"[REVIEW] Không parse được JSON response (page {page}): {json_err}")
                break
            
            # Check xem có data không - API trả về "list_ratings"
            reviews = data.get("list_ratings", [])
            if not reviews or len(reviews) == 0:
                logger.info(f"[REVIEW] Hết reviews ở page {page}")
                break
            
            # Insert review page vào database
            review_data = {
                "product_id": product_id,
                "product_snapshot_id": product_snapshot_id,
                "session_id": session_id,
                "pages": page,
                "data": data
            }
            
            if db.insert_review(review_data):
                total_saved += 1
                logger.success(f"[REVIEW] Lưu page {page}: {len(reviews)} reviews")
            else:
                logger.info(f"[REVIEW] Page {page} đã tồn tại (duplicate) - Tiếp tục check page sau")
                # Không break ở đây để đảm bảo crawl hết các trang còn thiếu (nếu có gap)
                # Hoặc đơn giản là tiếp tục vì chúng ta đang resume
            
            # Check xem còn page tiếp theo không
            total_reviews = data.get("total", 0)
            current_count = page * config.REVIEW_API_LIMIT
            
            if current_count >= total_reviews:
                logger.info(f"[REVIEW] Đã crawl hết {total_reviews} reviews")
                break
            
            page += 1
            time.sleep(0.5)  # Delay nhẹ giữa các page
            
        except Exception as exc:
            logger.error(f"[REVIEW] Lỗi crawl review page {page}: {exc}")
            break
    
    logger.success(f"[REVIEW] Hoàn thành: Lưu {total_saved} pages cho product {product_id}")
    return total_saved
