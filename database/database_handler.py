"""
Database handler để lưu dữ liệu vào Supabase
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
        
        Args:
            source_name: Tên nguồn crawl (VD: lamthaocosmetics)
            
        Returns:
            Session ID (UUID)
        """
        try:
            # Gọi function từ schema raw
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
        
        Args:
            session_id: Session ID cần complete
            status: Trạng thái (completed/failed)
        """
        try:
            # Gọi function từ schema raw
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
    
    def insert_product(self, session_id: uuid.UUID, product_data: Dict[str, Any]) -> Optional[int]:
        """
        Insert sản phẩm vào bảng product_api
        
        Args:
            session_id: Session đang chạy tương ứng với source
            product_data: Dictionary chứa thông tin sản phẩm
            Format: {
                'product_id': str,
                'source_name': str,
                'data': dict (JSONB)
            }
            
        Returns:
            Product ID (bigint) hoặc None nếu thất bại/duplicate
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
                
        except Exception as e:
            logger.error(f"Lỗi insert product {product_data.get('product_id')}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Lỗi insert product {product_data.get('product_id')}: {str(e)}")
            return None

