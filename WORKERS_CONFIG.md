# Worker Configuration - Optimized Pipeline (v2.0)

## Current Settings (OPTIMIZED)

### Concurrent Processing
- **Product Crawling**: 25 concurrent requests (3x increase from 8)
- **Review Pages**: 25 concurrent pages per product (25% increase from 20)
- **Brand Processing**: 3 brands in parallel (50% increase from 2)
- **Total Concurrent Capacity**: Up to 75 simultaneous operations (25 × 3)

### Delays (Anti-Block) - OPTIMIZED
- Website 1 (lamthaocosmetics): 0.3s with ±20% jitter (reduced from 0.5s)
- Website 2 (thegioiskinfood): 0.5s with ±20% jitter (reduced from 1.0s)
- Review API: 0.1s with ±20% jitter (reduced from 0.3s - API chậm, tăng concurrency)
- Random jitter prevents pattern detection

### Connection Pooling (OPTIMIZED)
- Total connection limit: 300 (increased from 200)
- Per-host limit: 100 (increased from 50 - review API needs high concurrency)
- DNS cache: 600s (increased from 300s)
- Connection reuse: Enabled (force_close=False)
- Session lifetime: Until pipeline completion

### Retry Strategy
- Max retries: 3
- Exponential backoff: 2s to 10s
- Timeout: 30s per request

---

## How to Adjust Workers

Edit `config.py`:

```python
# Current optimized settings
MAX_CONCURRENT_REQUESTS = 25  # Products (was 8)
MAX_CONCURRENT_BRANDS = 3     # Brands (was 2)
MAX_REVIEW_CONCURRENT_PAGES = 25  # Review pages (was 20)

# Adjust delays if hitting rate limits
WEBSITE_1_DELAY = 0.3  # Increase if needed (was 0.5s)
WEBSITE_2_DELAY = 0.5  # Increase if needed (was 1.0s)
REVIEW_DELAY = 0.1     # Increase if needed (was 0.3s)
```

---

## Performance vs Safety Trade-offs

| Setting | Old | Optimized (v2.0) | Aggressive | Conservative |
|---------|-----|------------------|------------|--------------|
| Concurrent Requests | 8 | **25** | 30-35 | 15-20 |
| Brand Parallelism | 2 | **3** | 4-5 | 2 |
| Review Pages | 20 | **25** | 30 | 15 |
| W1 Delay | 0.5s | **0.3s** | 0.2s | 0.5s |
| W2 Delay | 1.0s | **0.5s** | 0.3s | 0.8s |
| Review Delay | 0.3s | **0.1s** | 0.05s | 0.2s |
| Risk Level | Low | **Medium** | High | Low |

**Current**: Optimized (v2.0) - Balanced for 15 brands with slow review API

**Recommended**: Monitor first run for 429/503 errors. If stable, can try Aggressive settings.

---

## Expected Performance Gains

| Metric | Before | After (v2.0) | Improvement |
|--------|--------|--------------|-------------|
| **Products/s** | ~8-10 | ~20-25 | **2.5x** ↑ |
| **Review pages/s** | ~10-15 | ~25-30 | **2x** ↑ |
| **Time/brand** | ~5-7 min | ~2-3 min | **2.3x** ↓ |
| **Total (15 brands)** | ~75-105 min | ~30-45 min | **2.5x** ↓ |
