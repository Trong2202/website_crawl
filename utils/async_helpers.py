"""
Async helpers for concurrent crawling with rate limiting
"""
import asyncio
import random
from typing import Optional, Dict, Any
from aiohttp import ClientSession, TCPConnector, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.logger import get_logger
import config

logger = get_logger()

# Global session for connection reuse
_session: Optional[ClientSession] = None
_session_lock = asyncio.Lock()


async def get_session() -> ClientSession:
    """
    Get or create global aiohttp session with connection pooling
    
    Returns:
        ClientSession with configured connector
    """
    global _session
    
    async with _session_lock:
        if _session is None or _session.closed:
            connector = TCPConnector(
                limit=200,  # Total connections (increased for aggressive crawling)
                limit_per_host=50,  # Per host limit (increased for review API)
                ttl_dns_cache=300,  # DNS cache
                enable_cleanup_closed=True
            )
            timeout = ClientTimeout(total=config.TIMEOUT)
            _session = ClientSession(
                connector=connector,
                timeout=timeout,
                headers=config.HEADERS
            )
            logger.info("Created new aiohttp session with connection pooling")
        
        return _session


async def close_session():
    """Close global session"""
    global _session
    if _session and not _session.closed:
        await _session.close()
        logger.info("Closed aiohttp session")


@retry(
    stop=stop_after_attempt(config.MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
async def make_request_async(url: str, delay: float = None) -> Optional[str]:
    """
    Make async HTTP request with retry logic and anti-block measures
    
    Args:
        url: URL to fetch
        delay: Delay before request (with jitter)
        
    Returns:
        Response text or None
    """
    try:
        # Add random jitter to delay (±20%)
        if delay is None:
            delay = config.REQUEST_DELAY
        
        jitter = delay * random.uniform(-0.2, 0.2)
        await asyncio.sleep(delay + jitter)
        
        session = await get_session()
        logger.info(f"[ASYNC] Requesting: {url}")
        
        async with session.get(url) as response:
            response.raise_for_status()
            text = await response.text()
            logger.success(f"[ASYNC] Success: {url}")
            return text
            
    except Exception as e:
        logger.error(f"[ASYNC] Error {url}: {str(e)}")
        raise


async def make_request_with_semaphore(url: str, semaphore: asyncio.Semaphore, delay: float = None) -> Optional[str]:
    """
    Make async request with semaphore for concurrency control
    
    Args:
        url: URL to fetch
        semaphore: Semaphore to limit concurrency
        delay: Delay before request
        
    Returns:
        Response text or None
    """
    async with semaphore:
        return await make_request_async(url, delay)
