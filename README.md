# Pipeline Crawl Dữ Liệu Mỹ Phẩm

Pipeline chuyên nghiệp để crawl dữ liệu sản phẩm mỹ phẩm từ 2 website và lưu vào Supabase.

## Tính năng

- ✅ Crawl sản phẩm từ 2 website: `lamthaocosmetics.vn` và `thegioiskinfood.com`
- ✅ Trích xuất: brand, danh mục, tên sản phẩm, giá gốc, giá sale, số đã bán, link sản phẩm
- ✅ Lưu dữ liệu JSONB vào Supabase (bảng `raw.product_api`)
- ✅ Auto-retry khi request fail
- ✅ Smart delay để tránh bị block
- ✅ Logging chi tiết
- ✅ Auto-deduplication trong database

## Cài đặt

Dự án sử dụng **UV package manager**:

```bash
uv sync
```

## Cấu hình

1. **Tạo file `.env` từ `.envexample`:**

```bash
cp .envexample .env
```

2. **Điền thông tin Supabase vào `.env`:**

```env
SUPABASE_URL=your_supabase_url_here
SUPABASE_KEY=your_supabase_anon_key_here
SUPABASE_SCHEMA=raw
```

3. **Setup database:**
   - Truy cập Supabase Dashboard
   - Vào SQL Editor
   - Chạy file `database.sql`

4. **Cấu hình brands cần crawl:**
   - Mở file `brands.txt`
   - Thêm/xoá brands (mỗi dòng một brand)
   - Comment dòng bằng `#` để bỏ qua

## Sử dụng

### 1. Tra cứu danh sách brands

Chạy file riêng để xem tất cả brands có sẵn:

```bash
uv run python crawl_brands.py
```

Kết quả lưu vào `all_brands.txt`

### 2. Crawl sản phẩm

Chạy pipeline chính:

```bash
uv run python main_pipeline.py
```

Pipeline sẽ:
1. Đọc brands từ `brands.txt`
2. Tạo crawl session
3. Crawl sản phẩm từng brand từ cả 2 website
4. Lưu vào Supabase tự động
5. In báo cáo tổng kết

## Cấu trúc dữ liệu

Dữ liệu lưu vào bảng `raw.product_api` với format JSONB:

```json
{
  "brand": "CeraVe",
  "category": "Chăm sóc da mặt",
  "name": "Sữa Rửa Mặt CeraVe...",
  "price": 300000,
  "price_sale": 250000,
  "bought": 150,
  "url": "https://..."
}
```

PostgreSQL tự động extract `price` và `bought` từ JSONB.

## Cấu trúc dự án

```
website_crawl/
├── main_pipeline.py          # File chính - crawl và lưu sản phẩm
├── crawl_brands.py           # Tra cứu danh sách brands
├── config.py                 # Cấu hình
├── brands.txt                # Brands cần crawl
├── database.sql              # Schema database
│
├── utils/
│   ├── logger.py            # Logging
│   └── helpers.py           # Utilities
│
└── database/
    └── database_handler.py  # Supabase handler
```

## Logs

Logs được lưu tự động vào thư mục `logs/`:
- Format: `crawl_YYYY-MM-DD.log`
- Rotate daily, giữ 30 ngày

## Lưu ý

- Đảm bảo đã setup database schema trước khi chạy
- Kiểm tra file `.env` có thông tin đúng
- Không chạy quá nhiều lần liên tục để tránh bị block
- Check logs nếu có lỗi

## Hỗ trợ

Đọc thêm chi tiết:
- `QUICK_START.txt` - Hướng dẫn nhanh 3 bước
- `HUONG_DAN.txt` - Hướng dẫn chi tiết
- `TONG_QUAN_DU_AN.txt` - Kiến trúc kỹ thuật

---

**Chúc bạn crawl thành công!** 🎉

