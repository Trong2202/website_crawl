"""
AGGRESSIVE Listing Crawler - Weekly URL Refresh
Maximum speed with high concurrency (independent run)
"""
import sys
import asyncio
from datetime import datetime

from utils.logger import get_logger
from utils.helpers import read_brands_from_file
from utils.async_helpers import close_session
from database.database_handler import DatabaseHandler
from crawlers import (
    crawl_listing_lamthaocosmetics,
    crawl_listing_thegioiskinfood,
)
import config

logger = get_logger()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


async def crawl_brand_listings(brand: str, db: DatabaseHandler, sessions: dict) -> dict:
    """Crawl listings for one brand"""
    stats = {"listings_1": 0, "listings_2": 0}
    
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Brand: {brand}")
    logger.info(f"{'=' * 60}")
    
    # W1
    try:
        listings_1 = crawl_listing_lamthaocosmetics(brand, sessions[config.WEBSITE_1_NAME], db)
        stats["listings_1"] = len(listings_1)
        logger.success(f"✓ W1: {len(listings_1)} NEW")
    except Exception as exc:
        logger.error(f"W1 error: {exc}")
    
    # W2
    try:
        listings_2 = crawl_listing_thegioiskinfood(brand, sessions[config.WEBSITE_2_NAME], db)
        stats["listings_2"] = len(listings_2)
        logger.success(f"✓ W2: {len(listings_2)} NEW")
    except Exception as exc:
        logger.error(f"W2 error: {exc}")
    
    return stats


async def run_listing_crawler_async():
    """Async listing crawler with brand parallelization"""
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("AGGRESSIVE LISTING CRAWLER - Weekly Run (Maximum Speed)")
    logger.info(f"Concurrency: 8 | Delay: 0.4s")
    logger.info("=" * 80)
    
    brands = read_brands_from_file()
    if not brands:
        logger.error("No brands")
        return
    
    logger.info(f"Processing {len(brands)} brands\n")
    
    db = DatabaseHandler()
    sessions = {}
    
    try:
        sessions[config.WEBSITE_1_NAME] = db.create_session(config.WEBSITE_1_NAME)
        sessions[config.WEBSITE_2_NAME] = db.create_session(config.WEBSITE_2_NAME)
    except Exception:
        logger.error("Cannot create sessions")
        raise
    
    total_stats = {"listings_1": 0, "listings_2": 0}
    
    try:
        # Process multiple brands in parallel (aggressive)
        batch_size = 4  # 4 brands at once
        
        for i in range(0, len(brands), batch_size):
            batch = brands[i:i + batch_size]
            logger.info(f"\n{'#' * 80}")
            logger.info(f"Batch: {', '.join(batch)}")
            logger.info(f"{'#' * 80}")
            
            # Process batch concurrently
            tasks = [crawl_brand_listings(brand, db, sessions) for brand in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for brand, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"✗ {brand}: {result}")
                else:
                    total_stats["listings_1"] += result["listings_1"]
                    total_stats["listings_2"] += result["listings_2"]
    
    except KeyboardInterrupt:
        logger.warning("\nStopped")
        raise
    finally:
        await close_session()
        for source_name, session_id in sessions.items():
            db.complete_session(session_id, 'completed')
    
    duration = datetime.now() - start_time
    print("\n" + "=" * 80)
    print("AGGRESSIVE LISTING CRAWLER - SUMMARY")
    print("=" * 80)
    print(f"Duration: {duration}")
    print(f"Brands: {len(brands)}")
    print(f"\n📋 NEW LISTINGS INSERTED:")
    print(f"  - Website 1: {total_stats['listings_1']}")
    print(f"  - Website 2: {total_stats['listings_2']}")
    print(f"  - Total: {total_stats['listings_1'] + total_stats['listings_2']}")
    print("=" * 80 + "\n")
    logger.success("LISTING CRAWLER COMPLETED")


def run_listing_crawler():
    """Entry point"""
    try:
        asyncio.run(run_listing_crawler_async())
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    run_listing_crawler()
