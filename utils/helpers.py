"""
Helper functions cho pipeline crawl
"""
import re
import time
import requests
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.logger import get_logger
import config

logger = get_logger()


def delay_request(delay: float = None):
    """
    Delay giữa các request để tránh bị block
    
    Args:
        delay: Thời gian delay (seconds). Nếu None sẽ dùng config.REQUEST_DELAY
    """
    delay_time = delay if delay is not None else config.REQUEST_DELAY
    time.sleep(delay_time)


@retry(
    stop=stop_after_attempt(config.MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout)),
    reraise=True
)
def make_request(url: str, headers: Dict[str, str] = None, timeout: int = None) -> Optional[requests.Response]:
    """
    Thực hiện HTTP request với retry logic
    
    Args:
        url: URL cần crawl
        headers: HTTP headers
        timeout: Timeout cho request
        
    Returns:
        Response object hoặc None nếu thất bại
    """
    try:
        headers = headers or config.HEADERS
        timeout = timeout or config.TIMEOUT
        
        logger.info(f"Đang request: {url}")
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        logger.success(f"Request thành công: {url}")
        return response
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error {e.response.status_code}: {url}")
        raise
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout khi request: {url}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {url} - {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {url} - {str(e)}")
        return None


def parse_html(html_content: str) -> Optional[BeautifulSoup]:
    """
    Parse HTML content thành BeautifulSoup object
    
    Args:
        html_content: HTML content string
        
    Returns:
        BeautifulSoup object hoặc None nếu thất bại
    """
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        return soup
    except Exception as e:
        logger.error(f"Error parsing HTML: {str(e)}")
        return None


def read_brands_from_file(file_path: str = config.BRANDS_FILE) -> list:
    """
    Đọc danh sách brands từ file brands.txt
    
    Args:
        file_path: Đường dẫn đến file brands
        
    Returns:
        List các brand names
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            brands = []
            for line in f:
                line = line.strip()
                # Bỏ qua dòng trống và comment
                if line and not line.startswith('#'):
                    brands.append(line)
            
        # Loại bỏ duplicate
        brands = list(dict.fromkeys(brands))
        logger.info(f"Đọc được {len(brands)} brands từ {file_path}")
        return brands
        
    except FileNotFoundError:
        logger.error(f"Không tìm thấy file: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error đọc brands file: {str(e)}")
        return []


def normalize_brand_name(brand: str) -> str:
    """
    Chuẩn hóa tên brand để sử dụng trong URL
    
    Args:
        brand: Tên brand
        
    Returns:
        Tên brand đã chuẩn hóa
    """
    # Chuyển về lowercase và thay space bằng dash
    normalized = brand.lower().strip()
    normalized = normalized.replace(" ", "-")
    normalized = normalized.replace("'", "")
    return normalized


def format_price(price_str: str) -> float:
    """
    Giữ tương thích cũ - trả về số từ chuỗi giá.
    Sử dụng giá trị đầu tiên nếu có nhiều mức (vd: "150k - 200k").
    """
    return extract_price_value(price_str)


def calculate_discount_percent(original_price: float, sale_price: float) -> float:
    """
    Tính phần trăm giảm giá
    
    Args:
        original_price: Giá gốc
        sale_price: Giá sale
        
    Returns:
        Phần trăm giảm giá
    """
    if original_price > 0 and sale_price > 0:
        return round(((original_price - sale_price) / original_price) * 100, 2)
    return 0.0


def extract_price_value(price_str: str) -> float:
    """
    Lấy giá trị số đầu tiên trong chuỗi giá, loại bỏ ký tự tiền tệ/khoảng.
    """
    if not price_str:
        return 0.0
    first_part = price_str.split("-")[0]
    digits = re.sub(r"[^\d]", "", first_part)
    return float(digits) if digits else 0.0


def extract_bought_value(bought_str: str) -> int:
    """
    Chuyển đổi chuỗi số đã bán (vd: '2,1k') thành integer.
    """
    if not bought_str:
        return 0
    text = bought_str.lower().replace(",", ".")
    match = re.search(r"([\d\.]+)\s*([km]?)", text)
    if match:
        value = float(match.group(1))
        suffix = match.group(2)
        if suffix == "k":
            value *= 1_000
        elif suffix == "m":
            value *= 1_000_000
        return int(value)
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0

