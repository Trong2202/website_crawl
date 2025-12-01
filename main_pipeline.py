"""
Optimized Main Pipeline with Async/Concurrent Processing
HYBRID: Quick wins + Async concurrent crawling
"""
import sys
import asyncio
from datetime import datetime
from typing import Dict
import uuid

from utils.logger import get_logger
from utils.helpers import read_brands_from_file
from utils.async_helpers import close_session
from database.database_handler import DatabaseHandler
from crawlers import (
    crawl_listing_lamthaocosmetics,
    crawl_listing_thegioiskinfood,
    crawl_reviews_thegioiskinfood,
)
from crawlers.async_product_crawler import crawl_products_concurrent
from crawlers.async_review_crawler import crawl_reviews_thegioiskinfood_async
import config

logger = get_logger()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


async def crawl_brand_all_steps_async(brand: str, db: DatabaseHandler, sessions: Dict[str, uuid.UUID]) -> Dict[str, int]:
    """
    Async version: Crawl all steps for one brand with concurrent product processing
    
    Args:
        brand: Brand name
        db: Database handler
        sessions: Session IDs dict
        
    Returns:
        Statistics dict
    """
    stats = {
        "listings_1": 0,
        "listings_2": 0,
        "products_1": 0,
        "products_2": 0,
        "reviews": 0,
    }
    
    logger.info(f"\n{'=' * 80}")
    logger.info(f"Brand: {brand}")
    logger.info(f"{'=' * 80}")
    
    # ========================================
    # STEP 2: Get Listings from DB (Skip Crawl)
    # ========================================
    logger.info(f"\n[STEP 2] Get Listings from DB: {brand}")
    
    # Website 1
    try:
        # listings_1 = crawl_listing_lamthaocosmetics(brand, sessions[config.WEBSITE_1_NAME], db)
        listings_1 = db.get_listings_by_brand(config.WEBSITE_1_NAME, brand)
        stats["listings_1"] = len(listings_1)
        logger.info(f"Found {len(listings_1)} listings for W1 in DB")
    except Exception as exc:
        logger.error(f"Error getting listings W1: {exc}")
        listings_1 = []
    
    # Website 2
    try:
        # listings_2 = crawl_listing_thegioiskinfood(brand, sessions[config.WEBSITE_2_NAME], db)
        listings_2 = db.get_listings_by_brand(config.WEBSITE_2_NAME, brand)
        stats["listings_2"] = len(listings_2)
        logger.info(f"Found {len(listings_2)} listings for W2 in DB")
    except Exception as exc:
        logger.error(f"Error getting listings W2: {exc}")
        listings_2 = []
    
    logger.success(f"[STEP 2] Total listings from DB: {stats['listings_1']} (W1) + {stats['listings_2']} (W2)")
    
    # ========================================
    # STEP 3: Crawl Products CONCURRENTLY
    # ========================================
    logger.info(f"\n[STEP 3] Crawl Products (CONCURRENT)")
    
    # Process both websites concurrently
    tasks = []
    
    if listings_1:
        tasks.append(crawl_products_concurrent(
            listings_1,
            sessions[config.WEBSITE_1_NAME],
            db,
            config.WEBSITE_1_NAME
        ))
    
    if listings_2:
        tasks.append(crawl_products_concurrent(
            listings_2,
            sessions[config.WEBSITE_2_NAME],
            db,
            config.WEBSITE_2_NAME
        ))
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Product crawl error: {result}")
            else:
                if i == 0 and listings_1:  # W1
                    stats["products_1"] = result.get("products", 0)
                elif i == (1 if listings_1 else 0) and listings_2:  # W2
                    stats["products_2"] = result.get("products", 0)
                    
                    # STEP 4: Crawl Reviews for W2 products
                    if "results" in result:
                        semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
                        review_tasks = []
                        
                        for product_result in result["results"]:
                            if product_result and product_result.get('id'):
                                review_tasks.append(
                                    crawl_reviews_thegioiskinfood_async(
                                        product_result['id'],
                                        product_result['product_id'],
                                        sessions[config.WEBSITE_2_NAME],
                                        db,
                                        semaphore
                                    )
                                )
                        
                        if review_tasks:
                            logger.info(f"\n[STEP 4] Crawl Reviews (CONCURRENT)")
                            review_results = await asyncio.gather(*review_tasks, return_exceptions=True)
                            stats["reviews"] = sum(r for r in review_results if isinstance(r, int))
    
    logger.success(
        f"[COMPLETE] {brand}: "
        f"Products={stats['products_1']+stats['products_2']}, "
        f"Reviews={stats['reviews']}"
    )
    
    return stats


async def run_pipeline_async():
    """Main async pipeline - Process brands with concurrency"""
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("OPTIMIZED ASYNC CRAWL PIPELINE - MỸ PHẨM")
    logger.info("Features: Concurrent processing + Session reuse + Anti-block")
    logger.info("=" * 80)
    
    # Read brands
    brands = read_brands_from_file()
    if not brands:
        logger.error("No brands to crawl")
        return
    
    logger.info(f"Processing {len(brands)} brands with {config.MAX_CONCURRENT_REQUESTS} concurrent requests\n")
    
    # Initialize database
    db = DatabaseHandler()
    sessions = {}
    pipeline_failed = False
    
    # Create sessions
    try:
        sessions[config.WEBSITE_1_NAME] = db.create_session(config.WEBSITE_1_NAME)
        sessions[config.WEBSITE_2_NAME] = db.create_session(config.WEBSITE_2_NAME)
    except Exception:
        logger.error("Cannot create sessions")
        raise
    
    # Statistics
    total_stats = {
        "listings_1": 0,
        "listings_2": 0,
        "products_1": 0,
        "products_2": 0,
        "reviews": 0,
    }
    failed_brands = []
    
    # Crawl brands (with brand-level concurrency)
    try:
        # Process brands in batches
        brand_batch_size = config.MAX_CONCURRENT_BRANDS
        
        for batch_start in range(0, len(brands), brand_batch_size):
            batch_brands = brands[batch_start:batch_start + brand_batch_size]
            
            logger.info(f"\n{'#' * 80}")
            logger.info(f"Processing batch: {', '.join(batch_brands)}")
            logger.info(f"{'#' * 80}")
            
            # Process brands in this batch concurrently
            batch_tasks = [
                crawl_brand_all_steps_async(brand, db, sessions)
                for brand in batch_brands
            ]
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Collect stats
            for brand, result in zip(batch_brands, batch_results):
                if isinstance(result, Exception):
                    failed_brands.append(brand)
                    logger.error(f"✗ {brand}: {result}")
                else:
                    for key in total_stats:
                        total_stats[key] += result[key]
                    
                    logger.success(
                        f"✓ {brand}: "
                        f"Listings={result['listings_1']+result['listings_2']}, "
                        f"Products={result['products_1']+result['products_2']}, "
                        f"Reviews={result['reviews']}"
                    )
    
    except KeyboardInterrupt:
        pipeline_failed = True
        logger.warning("\nUser stopped pipeline (Ctrl+C)")
        raise
    except Exception as exc:
        pipeline_failed = True
        logger.error(f"Critical error: {exc}")
        raise
    finally:
        # Close async session
        await close_session()
        
        # Complete sessions
        status = "failed" if pipeline_failed else "completed"
        for source_name, session_id in sessions.items():
            db.complete_session(session_id, status)
    
    # Report
    duration = datetime.now() - start_time
    print("\n" + "=" * 80)
    print("SUMMARY REPORT")
    print("=" * 80)
    print(f"Duration: {duration}")
    print(f"Brands processed: {len(brands)}")
    print(f"Brands failed: {len(failed_brands)}")
    print(f"\n📋 LISTINGS:")
    print(f"  - Website 1: {total_stats['listings_1']}")
    print(f"  - Website 2: {total_stats['listings_2']}")
    print(f"  - Total: {total_stats['listings_1'] + total_stats['listings_2']}")
    print(f"\n📦 PRODUCTS:")
    print(f"  - Website 1: {total_stats['products_1']}")
    print(f"  - Website 2: {total_stats['products_2']}")
    print(f"  - Total: {total_stats['products_1'] + total_stats['products_2']}")
    print(f"\n⭐ REVIEWS:")
    print(f"  - Pages saved: {total_stats['reviews']}")
    
    if failed_brands:
        print(f"\n⚠️  Failed brands: {', '.join(failed_brands)}")
    
    print("=" * 80 + "\n")
    logger.success("ASYNC PIPELINE COMPLETED")


def run_pipeline():
    """Entry point - runs async pipeline"""
    try:
        asyncio.run(run_pipeline_async())
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
