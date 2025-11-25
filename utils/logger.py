"""
Logger configuration cho pipeline crawl
"""
from loguru import logger
import sys

# Xóa default logger
logger.remove()

# Thêm logger với format tiếng Việt
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level="INFO"
)

# Thêm file logger
logger.add(
    "logs/crawl_{time:YYYY-MM-DD}.log",
    rotation="00:00",  # Tạo file mới mỗi ngày
    retention="30 days",  # Giữ log 30 ngày
    compression="zip",  # Nén log cũ
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
    level="DEBUG"
)

def get_logger():
    """Lấy logger instance"""
    return logger

