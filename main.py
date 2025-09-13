import concurrent.futures
import json
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import urllib3
import time
import os
from crawl_url_pdf import download_pdf
from config import (BASE_URL, BASE_DOMAIN, DATASET_DIR, CHECKPOINT_DIR,
                    DEFAULT_MAX_PAGES, DEFAULT_BATCH_SIZE, MIN_BATCH_SIZE, MAX_BATCH_SIZE,
                    DROP_LEVELS_OPTIONS, DEFAULT_DROP_LEVELS, SEARCH_KEYWORD)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



from config import DEFAULT_MAX_PAGES
from checkpoint_utils import list_all_checkpoints
from crawl_utils import get_hidden_fields, initialize_session
from main_batch import main as run_batches

def get_user_configuration():
    print("\n" + "="*60)
    print("⚙️  CẤU HÌNH CRAWL DATA")
    print("="*60)
    while True:
        try:
            max_pages_input = input(f"📄 Nhập số pages tối đa để crawl (mặc định {DEFAULT_MAX_PAGES}): ").strip()
            if not max_pages_input:
                max_pages = DEFAULT_MAX_PAGES
            else:
                max_pages = int(max_pages_input)
                if max_pages <= 0:
                    print("❌ Số pages phải lớn hơn 0")
                    continue
            break
        except ValueError:
            print("❌ Vui lòng nhập số nguyên hợp lệ")

    while True:
        try:
            total_batches_input = input(f"🔢 Nhập tổng số batch muốn tạo (ví dụ 100) (mặc định 5): ").strip()
            if not total_batches_input:
                total_batches = 5
            else:
                total_batches = int(total_batches_input)
                if total_batches < 1:
                    print("❌ Tổng số batch phải >= 1")
                    continue
            break
        except ValueError:
            print("❌ Vui lòng nhập số nguyên hợp lệ")

    while True:
        try:
            num_threads_input = input(f"🧵 Nhập số luồng (threads) muốn chạy song song (mặc định 5): ").strip()
            if not num_threads_input:
                num_threads = 5
            else:
                num_threads = int(num_threads_input)
                if num_threads < 1:
                    print("❌ Số luồng phải >= 1")
                    continue
            break
        except ValueError:
            print("❌ Vui lòng nhập số nguyên hợp lệ")

    # Compute batch size from pages and total batches
    batch_size = (max_pages + total_batches - 1) // total_batches
    print(f"\n✅ Cấu hình đã chọn:")
    print(f"   📄 Max pages: {max_pages}")
    print(f"   🔢 Tổng batches: {total_batches}")
    print(f"   🧵 Luồng (threads): {num_threads}")
    print(f"   📦 Batch size: {batch_size}")
    print(f"   📊 Pages trong batch cuối: {max_pages - batch_size * (total_batches-1)}")
    return max_pages, batch_size, total_batches, num_threads

def display_checkpoint_status_and_choose(max_pages, batch_size, total_batches):
    print("\n" + "="*80)
    print("📊 HỆ THỐNG CHECKPOINT THEO DROP_LEVELS + BATCH")
    print("="*80)
    print(f"📄 Configuration: {max_pages} pages, batch size {batch_size}, {total_batches} batches")
    from config import DROP_LEVELS_OPTIONS, DEFAULT_DROP_LEVELS
    print("\n🎯 Các cấp tòa án:")
    for key, name in DROP_LEVELS_OPTIONS.items():
        print(f"  [{key}] {name}")
    checkpoints = list_all_checkpoints()
    if checkpoints:
        print(f"\n📋 Checkpoint hiện có ({len(checkpoints)} files):")
        for ckpt in checkpoints:
            print(f"  {ckpt['filename']}")
    else:
        print("\n📋 Chưa có checkpoint nào.")
    print("\n" + "-"*50)
    drop_levels = input(f"Chọn cấp tòa án (TW/CW/T/H hoặc Enter cho {DEFAULT_DROP_LEVELS}): ").strip().upper()
    if not drop_levels:
        drop_levels = DEFAULT_DROP_LEVELS
    if drop_levels not in DROP_LEVELS_OPTIONS:
        print(f"❌ Cấp '{drop_levels}' không hợp lệ, sử dụng mặc định '{DEFAULT_DROP_LEVELS}'")
        drop_levels = DEFAULT_DROP_LEVELS
    print(f"\n✅ Sẽ tạo batch đa luồng cho cấp tòa án: {drop_levels} ({DROP_LEVELS_OPTIONS[drop_levels]})")
    return drop_levels

import shutil
import os

if __name__ == "__main__":
    try:
        print("🚀 CRAWL DỮ LIỆU BẢN ÁN - ĐA LUỒNG THEO BATCH")
        # Hỏi user có muốn xóa toàn bộ checkpoint không
        reset = input("\nBạn có muốn tải lại từ đầu và xóa toàn bộ checkpoint? (y/N): ").strip().lower()
        if reset == 'y':
            from config import CHECKPOINT_DIR
            if os.path.exists(CHECKPOINT_DIR):
                shutil.rmtree(CHECKPOINT_DIR)
                print(f"✅ Đã xóa toàn bộ checkpoint trong {CHECKPOINT_DIR}")
            else:
                print("Không có thư mục checkpoint để xóa.")
        max_pages, batch_size, total_batches, num_threads = get_user_configuration()
        drop_levels = display_checkpoint_status_and_choose(max_pages, batch_size, total_batches)
        run_batches(max_pages, batch_size, total_batches, num_threads, drop_levels)
    except KeyboardInterrupt:
        print("\n⏹️ Đã dừng chương trình (Ctrl+C)")
