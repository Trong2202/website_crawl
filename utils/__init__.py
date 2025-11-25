"""
Utils package - Các utility functions
"""
from .logger import get_logger
from .helpers import (
    make_request,
    parse_html,
    delay_request,
    read_brands_from_file,
    normalize_brand_name,
    format_price,
    calculate_discount_percent
)

__all__ = [
    'get_logger',
    'make_request',
    'parse_html',
    'delay_request',
    'read_brands_from_file',
    'normalize_brand_name',
    'format_price',
    'calculate_discount_percent'
]
