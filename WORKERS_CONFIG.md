"""
Configuration Info - Optimized Pipeline

## Worker Configuration

### Concurrent Processing Settings
- **Product Crawling**: 8 concurrent requests
- **Brand Processing**: 2 brands in parallel
- **Total Concurrent Capacity**: Up to 16 simultaneous operations

### Delays (Anti-Block)
- Website 1 (lamthaocosmetics): 0.5s with ±20% jitter
- Website 2 (thegioiskinfood): 1.0s with ±20% jitter
- Random jitter prevents pattern detection

### Connection Pooling
- Total connection limit: 100
- Per-host limit: 20
- DNS cache: 300s
- Session reuse: Enabled

### Retry Strategy
- Max retries: 3
- Exponential backoff: 2s to 10s
- Timeout: 30s per request

## How to Adjust Workers

Edit `config.py`:

```python
# Increase concurrent requests (careful: may trigger rate limits)
MAX_CONCURRENT_REQUESTS = 10  # Currently 8

# Process more brands in parallel
MAX_CONCURRENT_BRANDS = 3  # Currently 2

# Adjust delays if hitting rate limits
WEBSITE_1_DELAY = 0.7  # Increase if needed
WEBSITE_2_DELAY = 1.2  # Increase if needed
```

## Performance vs Safety Trade-offs

| Setting | Current | Aggressive | Conservative |
|---------|---------|------------|--------------|
| Concurrent Requests | 8 | 12-15 | 4-6 |
| Brand Parallelism | 2 | 3-4 | 1 |
| W1 Delay | 0.5s | 0.3s | 0.8s |
| W2 Delay | 1.0s | 0.6s | 1.5s |
| Risk Level | Medium | High | Low |

**Recommended**: Start with current settings and monitor for 429 errors or blocks.
"""
<parameter name="Complexity">4
