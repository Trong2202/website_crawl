"""
Database handler để lưu dữ liệu vào Supabase

IMPORTANT: listing_api lưu full JSON data
"""
from typing import Dict, Any, Optional
import uuid
from supabase import create_client, Client
from utils.logger import get_logger
import config

logger = get_logger()


class DatabaseHandler:
    """Handler để tương tác với Supabase database"""
    
    def __init__(self):
        """Khởi tạo Supabase client"""
        try:
            self.client: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
            self.schema = config.SUPABASE_SCHEMA
            logger.info("Kết nối Supabase thành công")
        except Exception as e:
            logger.error(f"Lỗi kết nối Supabase: {str(e)}")
            raise
    
    def create_session(self, source_name: str) -> uuid.UUID:
        """
        Tạo crawl session mới
        """
        try:
            result = self.client.schema('raw').rpc(
                'create_crawl_session',
                {'p_source_name': source_name}
            ).execute()
            
            session_id = uuid.UUID(result.data)
            logger.success(f"Tạo session mới: {session_id} cho nguồn {source_name}")
            return session_id
            
        except Exception as e:
            logger.error(f"Lỗi tạo session: {str(e)}")
            raise
    
    def complete_session(self, session_id: uuid.UUID, status: str = 'completed'):
        """
        Đánh dấu session hoàn thành
        """
        try:
            self.client.schema('raw').rpc(
                'complete_crawl_session',
                {
                    'p_session_id': str(session_id),
                    'p_status': status
                }
            ).execute()
            
            logger.success(f"Hoàn thành session {session_id} với trạng thái {status}")
            
        except Exception as e:
            logger.error(f"Lỗi complete session: {str(e)}")
    
    def insert_listing(self, session_id: uuid.UUID, source_name: str, listing_data: Dict[str, Any]) -> bool:
        """
        Insert listing vào raw.listing_api
        Sử dụng direct insert thay vì RPC để tránh lỗi signature mismatch
        
        Args:
            session_id: UUID của crawl session
            source_name: Source name (lamthaocosmetics/thegioiskinfood)
            listing_data: Dictionary chứa thông tin listing (bao gồm product_id)
        
        Returns:
            True nếu insert thành công hoặc duplicate, False nếu lỗi
        """
        try:
            product_id = str(listing_data.get("id"))
            if not product_id:
                logger.error("Listing data missing 'id'")
                return False

            # Direct insert into table
            data_to_insert = {
                "session_id": str(session_id),
                "source_name": source_name,
                "product_id": product_id,
                "data": listing_data
            }
            
            # Sử dụng upsert với ignore_duplicates=True để mô phỏng ON CONFLICT DO NOTHING
            result = self.client.schema('raw').table('listing_api').upsert(
                data_to_insert, 
                on_conflict='product_id', 
                ignore_duplicates=True
            ).execute()
            
            return True
        except Exception as exc:
            logger.error(f"Lỗi insert listing {listing_data.get('id')}: {exc}")
            return False
    
    def insert_product(self, session_id: uuid.UUID, product_data: Dict[str, Any]) -> Optional[int]:
        """
        Insert sản phẩm vào bảng product_api
        Lưu full JSON trong data column
        """
        try:
            # Gọi function từ schema raw
            result = self.client.schema('raw').rpc(
                'safe_insert_product_api',
                {
                    'p_session_id': str(session_id),
                    'p_source_name': product_data['source_name'],
                    'p_product_id': product_data['product_id'],
                    'p_data': product_data['data']
                }
            ).execute()
            
            if result.data:
                logger.debug(f"Đã lưu sản phẩm {product_data['product_id']}")
                return result.data
            else:
                logger.debug(f"Sản phẩm {product_data['product_id']} đã tồn tại (duplicate)")
                return None
                
        except Exception as exc:
            logger.error(f"Lỗi insert product {product_data.get('product_id', 'unknown')}: {exc}")
            return None

    def insert_review(self, review_data: Dict[str, Any]) -> bool:
        """
        Insert review vào raw.review_api
        """
        try:
            result = self.client.schema('raw').rpc(
                "safe_insert_review_api",
                {
                    "p_data": review_data["data"],
                    "p_product_id": review_data["product_id"],
                    "p_product_snapshot_id": review_data["product_snapshot_id"],
                    "p_session_id": str(review_data["session_id"]),
                    "p_total": review_data["pages"],  # Database function uses p_total not p_pages
                }
            ).execute()
            
            if result.data and result.data != "null":
                return True
            return False
        except Exception as exc:
            logger.error(f"Lỗi insert review page {review_data.get('pages', 'unknown')}: {exc}")
            return False

    def get_latest_product_snapshot_id(self, product_id: str) -> Optional[int]:
        """
        Lấy product snapshot ID mới nhất cho một product_id
        """
        try:
            result = self.client.schema('raw').rpc(
                "get_latest_product_snapshot_id",
                {"p_product_id": product_id}
            ).execute()
            
            if result.data:
                return result.data
            return None
        except Exception as exc:
            logger.error(f"Lỗi get snapshot ID cho {product_id}: {exc}")
            return None
            
    def get_latest_review_page(self, product_id: str) -> int:
        """
        Lấy số trang review lớn nhất đã crawl cho product_id
        """
        try:
            # Query trực tiếp bảng review_api
            result = self.client.schema('raw').table('review_api') \
                .select('pages') \
                .eq('product_id', product_id) \
                .order('pages', desc=True) \
                .limit(1) \
                .execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]['pages']
            return 0
        except Exception as exc:
            logger.error(f"Lỗi get latest review page cho {product_id}: {exc}")
            return 0

    def get_listings_by_brand(self, source_name: str, brand_name: str) -> list:
        """
        Lấy danh sách listings từ database theo brand
        """
        try:
            # Note: Filtering by JSON field in Supabase-py can be tricky
            # We'll fetch by source_name and filter in Python for flexibility
            # Or use text search if possible. 
            # For now, let's try to fetch all for source and filter, 
            # assuming the number of listings per source isn't massive yet.
            # Optimization: If dataset grows, add a dedicated brand_id column or index.
            
            # Actually, let's try to use the 'contains' filter on the JSON column if possible,
            # but standard Supabase-py might not support deep JSON filtering easily without RPC.
            # Let's use a simple RPC or just fetch all for the source (if not too many) or 
            # better, create a specific RPC for this.
            
            # Let's create a simple RPC for this to be efficient
            # But I cannot modify database.sql easily right now without user permission/migration.
            # So I will fetch listings for the source and filter in Python.
            # To avoid fetching EVERYTHING, maybe we can rely on the fact that 
            # we are processing specific brands.
            
            # Let's try to fetch with a limit, or just fetch all for the source.
            # Given the user mentioned "Big Data", fetching all might be bad.
            # But for now, let's assume we can filter by brand in the query if we can.
            
            # Using Supabase 'contains' for JSONB:
            # .eq('data->brand->name', brand_name) might not work directly.
            
            # Let's try to use the text search on the data column if it's castable, 
            # or just fetch all. 
            # Wait, listing_api has `brand_id` column! 
            # Let's check database.sql again.
            # "brand_id VARCHAR(100) NULL"
            # And "listing_api" table definition.
            
            # If brand_id is populated, we can use it.
            # In listing_crawler.py, we didn't explicitly populate `brand_id` column in the insert.
            # We passed `listing_data` which has `brand` dict.
            # The `insert_listing` method:
            # data_to_insert = { ..., "data": listing_data }
            # It does NOT populate `brand_id` column explicitly in the `data_to_insert` dict 
            # unless `listing_data` has it? No, `listing_api` table has `brand_id` column.
            # The `insert_listing` method in `database_handler.py` (lines 84-89) 
            # ONLY inserts `session_id`, `source_name`, `product_id`, `data`.
            # It does NOT insert `brand_id`.
            
            # So `brand_id` column is likely NULL.
            # So we must filter by `data`.
            
            # Let's use a raw SQL query via RPC if possible, or just fetch all for source.
            # Since we want to be safe and not fetch millions of rows, 
            # let's try to use a filter on the JSON column if the client supports it.
            # client.table('listing_api').select('product_id, data').eq('source_name', source_name).execute()
            
            # If we have to fetch all, it might be slow.
            # But wait, the user wants "main_pipeline.py" to be fast.
            # If we fetch all listings every time, it might be slow.
            
            # Let's try to implement a smart filter.
            # Since I cannot add RPC right now, I will fetch all for the source 
            # BUT only select necessary columns to reduce bandwidth.
            
            result = self.client.schema('raw').table('listing_api') \
                .select('product_id, data') \
                .eq('source_name', source_name) \
                .execute()
            
            listings = []
            if result.data:
                normalized_target_brand = brand_name.lower()
                for item in result.data:
                    data = item.get('data', {})
                    item_brand = data.get('brand', {}).get('name', '').lower()
                    
                    # Loose matching
                    if normalized_target_brand in item_brand or item_brand in normalized_target_brand:
                        listings.append({
                            "product_id": item['product_id'],
                            "product_url": data.get('url', ''),
                        })
            
            return listings
            
        except Exception as exc:
            logger.error(f"Lỗi get listings by brand {brand_name}: {exc}")
            return []
