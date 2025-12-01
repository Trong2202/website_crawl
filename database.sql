-- =====================================================
-- SUPABASE RAW DATA SCHEMA - COMPLETE WITH AUTO-DEDUPLICATION
-- Schema: raw (Raw Data Storage with Incremental Snapshot)
-- Logic: Insert only when data changes, auto-cleanup duplicates
-- Timezone: Asia/Ho_Chi_Minh (UTC+7)
-- =====================================================

ALTER DATABASE postgres SET timezone = 'Asia/Ho_Chi_Minh';

CREATE SCHEMA IF NOT EXISTS raw;

-- =====================================================
-- TABLES
-- =====================================================

CREATE TABLE raw.crawl_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name VARCHAR(100) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    time INTERVAL NULL,
    total_products INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'running' 
        CHECK (status IN ('running', 'completed', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.home_api (
    home_id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    source_name VARCHAR(100) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.listing_api (
    listing_id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    source_name VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    brand_id VARCHAR(100) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(product_id)
);

CREATE TABLE raw.product_api (
    id BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    source_name VARCHAR(100) NOT NULL,
    data JSONB NOT NULL,
    price NUMERIC,
    bought INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_product_price_bought UNIQUE (product_id, price, bought)
);

CREATE TABLE raw.review_api (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    product_snapshot_id BIGINT NOT NULL REFERENCES raw.product_api(id) ON DELETE CASCADE,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    pages INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_review_product_pages UNIQUE (product_id, pages)
);

-- =====================================================
-- INDEXES
-- =====================================================

CREATE INDEX idx_sessions_source_status ON raw.crawl_sessions(source_name, status);
CREATE INDEX idx_sessions_created_at ON raw.crawl_sessions(created_at DESC);

CREATE INDEX idx_home_session ON raw.home_api(session_id);
CREATE INDEX idx_home_source ON raw.home_api(source_name);
CREATE INDEX idx_home_data_gin ON raw.home_api USING gin(data);

CREATE INDEX idx_listing_session ON raw.listing_api(session_id);
CREATE INDEX idx_listing_product ON raw.listing_api(product_id, source_name);
CREATE INDEX idx_listing_brand ON raw.listing_api(brand_id) WHERE brand_id IS NOT NULL;

CREATE INDEX idx_product_id ON raw.product_api(product_id);
CREATE INDEX idx_product_session ON raw.product_api(session_id);
CREATE INDEX idx_product_source_id ON raw.product_api(source_name, product_id);
CREATE INDEX idx_product_created_at ON raw.product_api(created_at DESC);
CREATE INDEX idx_product_data_gin ON raw.product_api USING gin(data);
CREATE INDEX idx_product_id_created ON raw.product_api(product_id, created_at DESC);
CREATE INDEX idx_product_price ON raw.product_api(product_id, price);
CREATE INDEX idx_product_bought ON raw.product_api(product_id, bought);
CREATE INDEX idx_product_price_bought ON raw.product_api(product_id, price, bought);

CREATE INDEX idx_review_product ON raw.review_api(product_id);
CREATE INDEX idx_review_snapshot ON raw.review_api(product_snapshot_id);
CREATE INDEX idx_review_created_at ON raw.review_api(created_at DESC);
CREATE INDEX idx_review_data_gin ON raw.review_api USING gin(data);
CREATE INDEX idx_review_product_pages ON raw.review_api(product_id, pages);

-- =====================================================
-- TRIGGER: AUTO UPDATE SESSION TIME
-- =====================================================

CREATE OR REPLACE FUNCTION raw.update_crawl_session_time()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.finished_at IS NOT NULL AND NEW.started_at IS NOT NULL THEN
        NEW.time = NEW.finished_at - NEW.started_at;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_session_time
BEFORE UPDATE ON raw.crawl_sessions
FOR EACH ROW
WHEN (NEW.finished_at IS NOT NULL AND OLD.finished_at IS NULL)
EXECUTE FUNCTION raw.update_crawl_session_time();

-- =====================================================
-- TRIGGER: PREVENT HOME_API DUPLICATES (JSONB COMPARISON)
-- =====================================================

CREATE OR REPLACE FUNCTION raw.check_home_api_duplicate()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM raw.home_api 
        WHERE data = NEW.data
    ) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_home_api_dedup
BEFORE INSERT ON raw.home_api
FOR EACH ROW
EXECUTE FUNCTION raw.check_home_api_duplicate();

-- =====================================================
-- TRIGGER: PREVENT PRODUCT_API DUPLICATES (PRICE/BOUGHT)
-- =====================================================

CREATE OR REPLACE FUNCTION raw.auto_extract_price_bought()
RETURNS TRIGGER AS $$
DECLARE
    v_price NUMERIC;
    v_bought INTEGER;
BEGIN
    -- Extract price và bought từ JSONB
    v_price := (NEW.data->>'price')::NUMERIC;
    v_bought := (NEW.data->>'bought')::INTEGER;
    
    NEW.price := v_price;
    NEW.bought := v_bought;
    
    -- Check duplicate (product_id, price, bought)
    IF EXISTS (
        SELECT 1 FROM raw.product_api
        WHERE product_id = NEW.product_id
        AND price = NEW.price
        AND bought = NEW.bought
        LIMIT 1
    ) THEN
        RETURN NULL;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_auto_extract_price_bought
BEFORE INSERT ON raw.product_api
FOR EACH ROW
EXECUTE FUNCTION raw.auto_extract_price_bought();

-- =====================================================
-- TRIGGER: PREVENT REVIEW_API DUPLICATES (PRODUCT_ID + PAGES)
-- =====================================================

CREATE OR REPLACE FUNCTION raw.check_review_duplicate()
RETURNS TRIGGER AS $$
BEGIN
    -- Check duplicate chỉ dựa trên (product_id, pages)
    IF EXISTS (
        SELECT 1 FROM raw.review_api 
        WHERE product_id = NEW.product_id
        AND pages = NEW.pages
    ) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_review_dedup
BEFORE INSERT ON raw.review_api
FOR EACH ROW
EXECUTE FUNCTION raw.check_review_duplicate();

-- =====================================================
-- TRIGGER: VALIDATE PRODUCT EXISTS BEFORE INSERT REVIEW
-- =====================================================

CREATE OR REPLACE FUNCTION raw.validate_review_product()
RETURNS TRIGGER AS $$
BEGIN
    -- Kiểm tra product_snapshot_id phải tồn tại và match với product_id
    IF NOT EXISTS (
        SELECT 1 FROM raw.product_api
        WHERE id = NEW.product_snapshot_id
        AND product_id = NEW.product_id
    ) THEN
        RAISE EXCEPTION 'Invalid review: product_snapshot_id=% does not exist or does not match product_id=%', 
                        NEW.product_snapshot_id, NEW.product_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_validate_review_product
BEFORE INSERT ON raw.review_api
FOR EACH ROW
EXECUTE FUNCTION raw.validate_review_product();

-- =====================================================
-- TRIGGER: AUTO UPDATE TOTAL_PRODUCTS
-- =====================================================

CREATE OR REPLACE FUNCTION raw.update_session_total_products()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE raw.crawl_sessions
    SET total_products = total_products + 1
    WHERE session_id = NEW.session_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_count_listings
AFTER INSERT ON raw.listing_api
FOR EACH ROW
EXECUTE FUNCTION raw.update_session_total_products();

CREATE TRIGGER trigger_count_products
AFTER INSERT ON raw.product_api
FOR EACH ROW
EXECUTE FUNCTION raw.update_session_total_products();

-- =====================================================
-- HELPER FUNCTIONS: SESSION MANAGEMENT
-- =====================================================

CREATE OR REPLACE FUNCTION raw.create_crawl_session(p_source_name VARCHAR)
RETURNS UUID AS $$
DECLARE
    v_session_id UUID;
BEGIN
    INSERT INTO raw.crawl_sessions (source_name)
    VALUES (p_source_name)
    RETURNING session_id INTO v_session_id;
    RETURN v_session_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.complete_crawl_session(p_session_id UUID, p_status VARCHAR DEFAULT 'completed')
RETURNS VOID AS $$
BEGIN
    UPDATE raw.crawl_sessions
    SET finished_at = NOW(), status = p_status
    WHERE session_id = p_session_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.get_latest_product_snapshot_id(p_product_id VARCHAR)
RETURNS BIGINT AS $$
DECLARE
    v_snapshot_id BIGINT;
BEGIN
    SELECT id INTO v_snapshot_id
    FROM raw.product_api
    WHERE product_id = p_product_id
    ORDER BY created_at DESC
    LIMIT 1;
    RETURN v_snapshot_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- HELPER FUNCTIONS: SAFE INSERT
-- =====================================================

CREATE OR REPLACE FUNCTION raw.safe_insert_home_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_data JSONB
)
RETURNS BIGINT AS $$
DECLARE
    v_home_id BIGINT;
BEGIN
    INSERT INTO raw.home_api (session_id, source_name, data)
    VALUES (p_session_id, p_source_name, p_data)
    RETURNING home_id INTO v_home_id;
    RETURN v_home_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.safe_insert_listing_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_product_id VARCHAR,
    p_brand_id VARCHAR DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_listing_id BIGINT;
BEGIN
    INSERT INTO raw.listing_api (session_id, source_name, product_id, brand_id)
    VALUES (p_session_id, p_source_name, p_product_id, p_brand_id)
    ON CONFLICT (product_id) DO NOTHING
    RETURNING listing_id INTO v_listing_id;
    RETURN v_listing_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.safe_insert_product_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_product_id VARCHAR,
    p_data JSONB
)
RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
    v_price NUMERIC;
    v_bought INTEGER;
BEGIN
    v_price := (p_data->>'price')::NUMERIC;
    v_bought := (p_data->>'bought')::INTEGER;
    
    IF EXISTS (
        SELECT 1 FROM raw.product_api 
        WHERE product_id = p_product_id 
        AND price = v_price
        AND bought = v_bought
    ) THEN
        RETURN NULL;
    END IF;
    
    INSERT INTO raw.product_api (product_id, session_id, source_name, data, price, bought)
    VALUES (p_product_id, p_session_id, p_source_name, p_data, v_price, v_bought)
    ON CONFLICT (product_id, price, bought) DO NOTHING
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.safe_insert_review_api(
    p_data JSONB,
    p_product_id VARCHAR,
    p_product_snapshot_id BIGINT,
    p_session_id UUID,
    p_pages INTEGER
)
RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
BEGIN
    -- Validate product_snapshot_id exists and matches product_id
    IF NOT EXISTS (
        SELECT 1 FROM raw.product_api
        WHERE id = p_product_snapshot_id
        AND product_id = p_product_id
    ) THEN
        RAISE EXCEPTION 'Product snapshot % does not exist for product_id=%', 
                        p_product_snapshot_id, p_product_id;
    END IF;
    
    INSERT INTO raw.review_api (data, product_id, product_snapshot_id, session_id, pages)
    VALUES (p_data, p_product_id, p_product_snapshot_id, p_session_id, p_pages)
    ON CONFLICT (product_id, pages) DO NOTHING
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.batch_insert_listing_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_products JSONB
)
RETURNS INTEGER AS $$
DECLARE
    v_product JSONB;
    v_inserted INTEGER := 0;
    v_result BIGINT;
BEGIN
    FOR v_product IN SELECT * FROM jsonb_array_elements(p_products)
    LOOP
        INSERT INTO raw.listing_api (session_id, source_name, product_id, brand_id)
        VALUES (
            p_session_id,
            p_source_name,
            v_product->>'product_id',
            v_product->>'brand_id'
        )
        ON CONFLICT (product_id) DO NOTHING
        RETURNING listing_id INTO v_result;
        
        IF v_result IS NOT NULL THEN
            v_inserted := v_inserted + 1;
        END IF;
    END LOOP;
    
    RETURN v_inserted;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- DIAGNOSTIC FUNCTIONS
-- =====================================================

CREATE OR REPLACE FUNCTION raw.get_product_price_history(p_product_id VARCHAR)
RETURNS TABLE (
    snapshot_id BIGINT,
    price NUMERIC,
    bought INTEGER,
    created_at TIMESTAMPTZ,
    price_change NUMERIC,
    bought_change INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH price_history AS (
        SELECT 
            id,
            pa.price,
            pa.bought,
            pa.created_at,
            LAG(pa.price) OVER (ORDER BY pa.created_at) as prev_price,
            LAG(pa.bought) OVER (ORDER BY pa.created_at) as prev_bought
        FROM raw.product_api pa
        WHERE product_id = p_product_id
        ORDER BY created_at
    )
    SELECT 
        id as snapshot_id,
        ph.price,
        ph.bought,
        ph.created_at,
        CASE WHEN prev_price IS NOT NULL THEN ph.price - prev_price ELSE NULL END as price_change,
        CASE WHEN prev_bought IS NOT NULL THEN ph.bought - prev_bought ELSE NULL END as bought_change
    FROM price_history ph;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.find_duplicate_home_api()
RETURNS TABLE (
    data_hash TEXT,
    duplicate_count BIGINT,
    home_ids BIGINT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        md5(data::TEXT) as data_hash,
        COUNT(*) as duplicate_count,
        array_agg(home_id ORDER BY created_at) as home_ids
    FROM raw.home_api
    GROUP BY md5(data::TEXT)
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.find_duplicate_product_api()
RETURNS TABLE (
    product_id VARCHAR,
    price NUMERIC,
    bought INTEGER,
    duplicate_count BIGINT,
    snapshot_ids BIGINT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pa.product_id,
        pa.price,
        pa.bought,
        COUNT(*) as duplicate_count,
        array_agg(pa.id ORDER BY pa.created_at) as snapshot_ids
    FROM raw.product_api pa
    GROUP BY pa.product_id, pa.price, pa.bought
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.find_duplicate_review_api()
RETURNS TABLE (
    product_id VARCHAR,
    pages INTEGER,
    duplicate_count BIGINT,
    review_ids BIGINT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ra.product_id,
        ra.pages,
        COUNT(*) as duplicate_count,
        array_agg(ra.id ORDER BY ra.created_at) as review_ids
    FROM raw.review_api ra
    GROUP BY ra.product_id, ra.pages
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC;
END;
$$ LANGUAGE plpgsql;

-- Kiểm tra orphan reviews (review không có product)
CREATE OR REPLACE FUNCTION raw.find_orphan_reviews()
RETURNS TABLE (
    review_id BIGINT,
    product_id VARCHAR,
    product_snapshot_id BIGINT,
    created_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        r.id as review_id,
        r.product_id,
        r.product_snapshot_id,
        r.created_at
    FROM raw.review_api r
    LEFT JOIN raw.product_api p ON r.product_snapshot_id = p.id
    WHERE p.id IS NULL
    ORDER BY r.created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- AUTO CLEANUP DUPLICATES (RUN AT END)
-- =====================================================

CREATE OR REPLACE FUNCTION raw.cleanup_all_duplicates()
RETURNS TABLE (
    table_name TEXT,
    deleted_count BIGINT
) AS $$
DECLARE
    v_deleted_home BIGINT := 0;
    v_deleted_product BIGINT := 0;
    v_deleted_review BIGINT := 0;
BEGIN
    -- Cleanup home_api: giữ record đầu tiên theo created_at
    WITH duplicates AS (
        SELECT home_id,
               ROW_NUMBER() OVER (PARTITION BY md5(data::TEXT) ORDER BY created_at ASC) as rn
        FROM raw.home_api
    )
    DELETE FROM raw.home_api
    WHERE home_id IN (SELECT home_id FROM duplicates WHERE rn > 1);
    
    GET DIAGNOSTICS v_deleted_home = ROW_COUNT;
    
    -- Cleanup product_api: giữ record đầu tiên theo created_at
    WITH duplicates AS (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY product_id, price, bought ORDER BY created_at ASC) as rn
        FROM raw.product_api
    )
    DELETE FROM raw.product_api
    WHERE id IN (SELECT id FROM duplicates WHERE rn > 1);
    
    GET DIAGNOSTICS v_deleted_product = ROW_COUNT;
    
    -- Cleanup review_api: giữ record đầu tiên theo created_at
    WITH duplicates AS (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY product_id, pages ORDER BY created_at ASC) as rn
        FROM raw.review_api
    )
    DELETE FROM raw.review_api
    WHERE id IN (SELECT id FROM duplicates WHERE rn > 1);
    
    GET DIAGNOSTICS v_deleted_review = ROW_COUNT;
    
    -- Return results
    RETURN QUERY
    SELECT 'home_api'::TEXT, v_deleted_home
    UNION ALL
    SELECT 'product_api'::TEXT, v_deleted_product
    UNION ALL
    SELECT 'review_api'::TEXT, v_deleted_review;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- VERIFICATION
-- =====================================================

SELECT 'Schema raw with direct JSONB comparison created successfully!' as status;
SELECT tablename FROM pg_tables WHERE schemaname = 'raw' ORDER BY tablename;

ALTER TABLE raw.listing_api
ADD COLUMN product_url TEXT;
ADD COLUMN data JSONB;

-- 1. Xóa trigger/function cũ nếu tồn tại
DROP TRIGGER IF EXISTS trg_sync_product_url ON raw.listing_api;
DROP FUNCTION IF EXISTS raw.sync_product_url_from_jsonb();

-- 2. Tạo function trigger để tách data->>'url' vào product_url
CREATE OR REPLACE FUNCTION raw.sync_product_url_from_jsonb()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Nếu tồn tại key 'url' trong jsonb cột data
  IF NEW.data IS NOT NULL AND NEW.data ? 'url' THEN
    NEW.product_url := NULLIF(NEW.data->>'url', '');
  ELSE
    NEW.product_url := NULL;
  END IF;

  RETURN NEW;
END;
$$;

-- 3. Tạo trigger chạy BEFORE INSERT/UPDATE
CREATE TRIGGER trg_sync_product_url
BEFORE INSERT OR UPDATE
ON raw.listing_api
FOR EACH ROW
EXECUTE FUNCTION raw.sync_product_url_from_jsonb();

-- 4. Cập nhật toàn bộ dữ liệu hiện có (nếu đã có data->'url')
UPDATE raw.listing_api
SET product_url = NULLIF(data->>'url', '')
WHERE data IS NOT NULL
  AND data ? 'url';

-- 5. (Tùy) Tạo index để tìm kiếm nhanh product_url
CREATE INDEX IF NOT EXISTS idx_raw_listing_api_product_url
ON raw.listing_api (product_url);
