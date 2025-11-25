"""
Crawl danh sách tên các brands/thương hiệu từ 2 website
Chạy độc lập để tra cứu: uv run python crawl_brands.py
Kết quả lưu vào file TXT
"""
from typing import List
import sys
from bs4 import BeautifulSoup
from utils.helpers import make_request, parse_html, delay_request
import config

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

print("Bắt đầu crawl danh sách brands...")


def crawl_lamthaocosmetics_brands() -> List[str]:
    """Crawl brands từ lamthaocosmetics.vn"""
    try:
        print(f"\nĐang crawl brands từ {config.WEBSITE_1_NAME}...")
        
        response = make_request(config.WEBSITE_1_BRANDS)
        if not response:
            return []
        
        soup = parse_html(response.text)
        if not soup:
            return []
        
        brands = []
        
        # Tìm các checkbox input có data-filter chứa vendor
        brand_inputs = soup.find_all('input', {'data-filter': True})
        
        for input_tag in brand_inputs:
            data_filter = input_tag.get('data-filter', '')
            if 'vendor:product' in data_filter or 'vendor' in data_filter:
                brand_name = data_filter.split('=')[-1].strip().strip(')')
                if brand_name:
                    brands.append(brand_name)
        
        # Loại bỏ duplicate và sort
        brands = sorted(list(set(brands)))
        
        print(f"Tìm thấy {len(brands)} brands")
        return brands
        
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        return []


def crawl_thegioiskinfood_brands() -> List[str]:
    """Crawl brands từ thegioiskinfood.com"""
    try:
        print(f"\nĐang crawl brands từ {config.WEBSITE_2_NAME}...")
        
        response = make_request(config.WEBSITE_2_BRANDS)
        if not response:
            return []
        
        soup = parse_html(response.text)
        if not soup:
            return []
        
        brands = []
        
        # Các brand nằm trong khối boxlistbrand với class brand-title
        for brand_item in soup.select("div.boxlistbrand span.brand-title"):
            name = brand_item.get_text(strip=True)
            if name:
                brands.append(name)
        
        # Loại bỏ duplicate và sort
        brands = sorted(list(set(brands)))
        
        print(f"Tìm thấy {len(brands)} brands")
        return brands
        
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        return []


def save_brands_to_txt(brands_1: List[str], brands_2: List[str], output_file: str = "all_brands.txt"):
    """Lưu danh sách brands vào file TXT, chia theo 2 trang"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("DANH SÁCH TẤT CẢ BRANDS/THƯƠNG HIỆU\n")
            f.write("="*80 + "\n\n")
            
            # Website 1
            f.write(f"{config.WEBSITE_1_NAME.upper()} ({len(brands_1)} brands)\n")
            f.write("-"*80 + "\n")
            for i, brand in enumerate(brands_1, 1):
                f.write(f"{i:3d}. {brand}\n")
            
            f.write("\n" + "="*80 + "\n\n")
            
            # Website 2
            f.write(f"{config.WEBSITE_2_NAME.upper()} ({len(brands_2)} brands)\n")
            f.write("-"*80 + "\n")
            for i, brand in enumerate(brands_2, 1):
                f.write(f"{i:3d}. {brand}\n")
            
            f.write("\n" + "="*80 + "\n")
            
            # Thống kê
            all_unique = set(brands_1 + brands_2)
            f.write(f"TỔNG CỘ: {len(all_unique)} brands unique\n")
            f.write("="*80 + "\n")
        
        print(f"\nĐã lưu vào {output_file}")
        
    except Exception as e:
        print(f"Lỗi lưu file: {str(e)}")


def main():
    """Main function"""
    # Crawl từ website 1
    brands_1 = crawl_lamthaocosmetics_brands()
    delay_request()
    
    # Crawl từ website 2
    brands_2 = crawl_thegioiskinfood_brands()
    
    # Lưu vào file TXT
    save_brands_to_txt(brands_1, brands_2)
    
    # Thống kê
    all_unique = set(brands_1 + brands_2)
    print("\n" + "="*80)
    print(f"THỐNG KÊ:")
    print(f"  - {config.WEBSITE_1_NAME}: {len(brands_1)} brands")
    print(f"  - {config.WEBSITE_2_NAME}: {len(brands_2)} brands")
    print(f"  - Tổng unique: {len(all_unique)} brands")
    print("="*80)
    print("\nHoàn thành!\n")


if __name__ == "__main__":
    main()

