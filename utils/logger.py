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

def get_logger():
    """Lấy logger instance"""
    return logger

