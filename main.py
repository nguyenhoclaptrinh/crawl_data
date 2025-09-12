import json
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import urllib3
import time
import os
from crawl_url_pdf import download_pdf
from config import (BASE_URL, BASE_DOMAIN, DATASET_DIR, CHECKPOINT_DIR, BATCH_SIZE, 
                   NUM_BATCHES, DROP_LEVELS_OPTIONS, DEFAULT_DROP_LEVELS, SEARCH_KEYWORD)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_hidden_fields(response):
    try:
        soup = BeautifulSoup(response, "html.parser")

        hidden = {}

        for input_tag in soup.find_all("input", type="hidden"):
            if input_tag.get("name") and input_tag.get("value"):
                hidden[input_tag["name"]] = input_tag["value"]

        return hidden
    except RequestException as e:
        print(f"Error fetching hidden fields: {e}")
        return {}


def initialize_session():
    """Khởi tạo session và lấy hidden fields ban đầu"""
    session = requests.Session()
    response = session.get(BASE_URL, verify=False)
    response.raise_for_status()
    hidden_fields = get_hidden_fields(response.text)
    return session, hidden_fields


def get_checkpoint_filename(drop_levels, batch_num):
    """Tạo tên file checkpoint theo DROP_LEVELS và batch number"""
    # Xử lý trường hợp DROP_LEVELS rỗng
    level_code = drop_levels if drop_levels else "ALL"
    return f"checkpoint_{level_code}_{batch_num}.json"


def get_checkpoint_filepath(drop_levels, batch_num):
    """Lấy đường dẫn đầy đủ của file checkpoint"""
    if not os.path.exists(CHECKPOINT_DIR):
        os.makedirs(CHECKPOINT_DIR)
    
    filename = get_checkpoint_filename(drop_levels, batch_num)
    return os.path.join(CHECKPOINT_DIR, filename)


def create_checkpoint_structure(drop_levels, batch_num, start_page, end_page):
    """Tạo cấu trúc checkpoint mới"""
    return {
        "drop_levels": drop_levels,
        "drop_levels_name": DROP_LEVELS_OPTIONS.get(drop_levels, f"Cấp {drop_levels}"),
        "batch_number": batch_num,
        "start_page": start_page,
        "end_page": end_page,
        "last_processed_page": 0,  # Page cuối cùng đã xử lý thành công
        "total_links_found": 0,
        "total_pdfs_downloaded": 0,
        "failed_pages": [],
        "completed_pages": [],
        "created_at": time.time(),
        "last_updated": time.time(),
        "is_completed": False
    }


def load_checkpoint(drop_levels, batch_num):
    """Tải checkpoint từ file cụ thể"""
    filepath = get_checkpoint_filepath(drop_levels, batch_num)
    
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            checkpoint_data = json.load(f)
            print(f"✅ Loaded checkpoint: {get_checkpoint_filename(drop_levels, batch_num)}")
            print(f"   📄 Pages: {checkpoint_data['start_page']}-{checkpoint_data['end_page']}")
            print(f"   ✏️  Last processed: {checkpoint_data['last_processed_page']}")
            print(f"   🔗 Links found: {checkpoint_data['total_links_found']}")
            print(f"   📥 PDFs downloaded: {checkpoint_data['total_pdfs_downloaded']}")
            return checkpoint_data
    else:
        print(f"❌ Không tìm thấy checkpoint: {get_checkpoint_filename(drop_levels, batch_num)}")
        return None


def save_checkpoint(checkpoint_data):
    """Lưu checkpoint vào file"""
    checkpoint_data["last_updated"] = time.time()
    
    filepath = get_checkpoint_filepath(
        checkpoint_data["drop_levels"], 
        checkpoint_data["batch_number"]
    )
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
    
    filename = get_checkpoint_filename(
        checkpoint_data["drop_levels"], 
        checkpoint_data["batch_number"]
    )
    print(f"💾 Saved checkpoint: {filename}")


def update_checkpoint_progress(checkpoint_data, page_num, links_found, success=True):
    """Cập nhật tiến độ xử lý page"""
    if success:
        if page_num not in checkpoint_data["completed_pages"]:
            checkpoint_data["completed_pages"].append(page_num)
            checkpoint_data["total_links_found"] += links_found
        
        # Cập nhật last_processed_page
        if page_num > checkpoint_data["last_processed_page"]:
            checkpoint_data["last_processed_page"] = page_num
        
        # Xóa khỏi failed_pages nếu có
        if page_num in checkpoint_data["failed_pages"]:
            checkpoint_data["failed_pages"].remove(page_num)
    else:
        if page_num not in checkpoint_data["failed_pages"]:
            checkpoint_data["failed_pages"].append(page_num)
    
    # Kiểm tra xem batch đã hoàn thành chưa
    if checkpoint_data["last_processed_page"] >= checkpoint_data["end_page"]:
        checkpoint_data["is_completed"] = True
    
    return checkpoint_data


def list_all_checkpoints():
    """Liệt kê tất cả checkpoint files có sẵn"""
    if not os.path.exists(CHECKPOINT_DIR):
        return []
    
    checkpoint_files = []
    for filename in os.listdir(CHECKPOINT_DIR):
        if filename.startswith("checkpoint_") and filename.endswith(".json"):
            # Parse filename: checkpoint_{drop_levels}_{batch}.json
            parts = filename.replace("checkpoint_", "").replace(".json", "").split("_")
            if len(parts) >= 2:
                drop_levels = parts[0] if parts[0] != "ALL" else ""
                try:
                    batch_num = int(parts[1])
                    checkpoint_files.append({
                        "filename": filename,
                        "drop_levels": drop_levels,
                        "batch_number": batch_num,
                        "filepath": os.path.join(CHECKPOINT_DIR, filename)
                    })
                except ValueError:
                    continue
    
    return sorted(checkpoint_files, key=lambda x: (x["drop_levels"], x["batch_number"]))


def display_checkpoint_status_and_choose():
    """Hiển thị trạng thái tất cả checkpoint và cho phép user chọn"""
    print("\n" + "="*80)
    print("📊 HỆ THỐNG CHECKPOINT THEO DROP_LEVELS + BATCH")
    print("="*80)
    
    # Hiển thị các DROP_LEVELS có sẵn
    print("\n🎯 Các cấp tòa án:")
    for key, name in DROP_LEVELS_OPTIONS.items():
        display_key = key if key else "ALL"
        print(f"  [{display_key}] {name}")
    
    # Liệt kê tất cả checkpoint hiện có
    checkpoints = list_all_checkpoints()
    if checkpoints:
        print(f"\n📋 Checkpoint hiện có ({len(checkpoints)} files):")
        for ckpt in checkpoints:
            # Load chi tiết checkpoint
            data = load_checkpoint(ckpt["drop_levels"], ckpt["batch_number"])
            if data:
                status_icon = "✅" if data["is_completed"] else "⏳"
                progress = f"{data['last_processed_page']}/{data['end_page']}"
                print(f"  {status_icon} {ckpt['filename']}: "
                      f"Pages {progress}, "
                      f"{data['total_links_found']} links, "
                      f"{data['total_pdfs_downloaded']} PDFs")
                
                if data["failed_pages"]:
                    print(f"      ❌ Failed pages: {data['failed_pages']}")
    else:
        print("\n📋 Chưa có checkpoint nào.")
    
    print("\n" + "-"*50)
    print("🎯 Chọn công việc:")
    print("  1. Tạo batch mới")
    print("  2. Tiếp tục batch đã có")
    
    choice = input("Nhập lựa chọn (1/2): ").strip()
    
    if choice == "1":
        return choose_new_batch()
    elif choice == "2":
        return choose_existing_batch(checkpoints)
    else:
        print("❌ Lựa chọn không hợp lệ, sử dụng mặc định: tạo batch mới")
        return choose_new_batch()


def choose_new_batch():
    """Cho phép user chọn DROP_LEVELS và batch để tạo mới"""
    print("\n🆕 TẠO BATCH MỚI")
    print("-" * 30)
    
    # Chọn DROP_LEVELS
    drop_levels = input(f"Chọn cấp tòa án (T/H/X hoặc Enter cho {DEFAULT_DROP_LEVELS}): ").strip().upper()
    if not drop_levels:
        drop_levels = DEFAULT_DROP_LEVELS
    
    if drop_levels not in DROP_LEVELS_OPTIONS:
        print(f"❌ Cấp '{drop_levels}' không hợp lệ, sử dụng mặc định '{DEFAULT_DROP_LEVELS}'")
        drop_levels = DEFAULT_DROP_LEVELS
    
    # Chọn batch
    batch_num = int(input(f"Chọn số batch (1-{NUM_BATCHES}): "))
    if batch_num < 1 or batch_num > NUM_BATCHES:
        print(f"❌ Batch không hợp lệ, sử dụng mặc định: 1")
        batch_num = 1
    
    # Tính toán start_page và end_page
    start_page = (batch_num - 1) * BATCH_SIZE + 1
    end_page = batch_num * BATCH_SIZE
    
    print(f"\n✅ Sẽ tạo batch mới:")
    print(f"   📂 DROP_LEVELS: {drop_levels} ({DROP_LEVELS_OPTIONS[drop_levels]})")
    print(f"   📦 Batch: {batch_num}")
    print(f"   📄 Pages: {start_page} - {end_page}")
    
    return drop_levels, batch_num, start_page, end_page, None


def choose_existing_batch(checkpoints):
    """Cho phép user chọn batch đã có để tiếp tục"""
    if not checkpoints:
        print("❌ Không có checkpoint nào để tiếp tục")
        return choose_new_batch()
    
    print(f"\n🔄 TIẾP TỤC BATCH ĐÃ CÓ")
    print("-" * 30)
    
    for i, ckpt in enumerate(checkpoints):
        data = load_checkpoint(ckpt["drop_levels"], ckpt["batch_number"])
        if data and not data["is_completed"]:
            print(f"  [{i+1}] {ckpt['filename']}: Pages {data['last_processed_page']}/{data['end_page']}")
    
    try:
        choice_idx = int(input("Chọn checkpoint để tiếp tục (số thứ tự): ")) - 1
        if 0 <= choice_idx < len(checkpoints):
            ckpt = checkpoints[choice_idx]
            data = load_checkpoint(ckpt["drop_levels"], ckpt["batch_number"])
            
            return (ckpt["drop_levels"], ckpt["batch_number"], 
                   data["start_page"], data["end_page"], data)
        else:
            print("❌ Lựa chọn không hợp lệ")
            return choose_new_batch()
    except ValueError:
        print("❌ Vui lòng nhập số")
        return choose_new_batch()


def create_payload(hidden_fields, page, drop_levels):
    """Tạo payload cho request tùy theo page số và drop_levels"""
    if page == 1:
        # Lần đầu: bấm nút "Tìm kiếm"
        return {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": SEARCH_KEYWORD,
            "ctl00$Content_home_Public$ctl00$Drop_Levels": drop_levels,
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$cmd_search_banner": "Tìm kiếm"
        }
    else:
        return {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": SEARCH_KEYWORD,
            "ctl00$Content_home_Public$ctl00$Drop_Levels": drop_levels,
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$DropPages": str(page),
            "__EVENTTARGET": "ctl00$Content_home_Public$ctl00$DropPages",
            "__EVENTARGUMENT": ""
        }


def crawl_page(session, page, hidden_fields, drop_levels):
    """Crawl một page và trả về danh sách links + hidden_fields mới"""
    payload = create_payload(hidden_fields, page, drop_levels)
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = session.post(BASE_URL, data=payload, headers=headers, verify=False)
        response.raise_for_status()
        
        print(f"📄 Page {page} (DROP_LEVELS={drop_levels}) fetched successfully.")
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        links = [a["href"] for a in soup.find_all("a", href=True)]
        page_links = []
        
        for link in links:
            text_split = link.split("/")
            if len(text_split) > 2 and text_split[2] == "chi-tiet-ban-an":
                full_link = BASE_DOMAIN + link
                page_links.append(full_link)
        
        new_hidden_fields = get_hidden_fields(response.text)
        print(f"✅ Found {len(page_links)} detail links on page {page}")
        return page_links, new_hidden_fields, True
        
    except RequestException as e:
        print(f"❌ Error on page {page}: {e}")
        return [], hidden_fields, False


def process_and_deduplicate_links(all_links):
    """Xử lý và loại bỏ duplicate links"""
    set_all_links = set(all_links)
    list_all_links = list(set_all_links)
    print(f"Total unique links collected: {len(list_all_links)}")
    return list_all_links


def download_all_pdfs(links, session):
    """Download tất cả PDF từ danh sách links"""
    if not os.path.exists(DATASET_DIR):
        os.makedirs(DATASET_DIR)

    for i, link in enumerate(links):
        print(f"{i+1}: {link}")
        download_pdf(link, session)


def main():
    """Hàm chính điều phối toàn bộ quá trình crawl với checkpoint theo DROP_LEVELS + batch"""
    print("🚀 BẮT ĐẦU CRAWL DỮ LIỆU BẢN ÁN")
    
    # Khởi tạo session và hidden fields
    session, hidden_fields = initialize_session()
    
    # Hiển thị trạng thái checkpoint và cho user chọn
    drop_levels, batch_num, start_page, end_page, existing_checkpoint = display_checkpoint_status_and_choose()
    
    # Tạo hoặc load checkpoint
    if existing_checkpoint:
        checkpoint_data = existing_checkpoint
        print(f"\n🔄 Tiếp tục từ page {checkpoint_data['last_processed_page'] + 1}")
        start_from_page = checkpoint_data['last_processed_page'] + 1
    else:
        checkpoint_data = create_checkpoint_structure(drop_levels, batch_num, start_page, end_page)
        save_checkpoint(checkpoint_data)
        print(f"\n🆕 Tạo checkpoint mới: {get_checkpoint_filename(drop_levels, batch_num)}")
        start_from_page = start_page
    
    # Crawl các pages
    all_links = []
    print(f"\n📄 Bắt đầu crawl pages {start_from_page} đến {end_page}...")
    
    for page in range(start_from_page, end_page + 1):
        print(f"\n--- Processing Page {page} ---")
        
        page_links, hidden_fields, success = crawl_page(session, page, hidden_fields, drop_levels)
        
        # Cập nhật checkpoint progress
        checkpoint_data = update_checkpoint_progress(checkpoint_data, page, len(page_links), success)
        
        if success:
            all_links.extend(page_links)
            print(f"✅ Page {page} hoàn thành: {len(page_links)} links")
        else:
            print(f"❌ Page {page} thất bại")
        
        # Lưu checkpoint sau mỗi page
        save_checkpoint(checkpoint_data)
        
        # Nghỉ ngắn để tránh spam server
        time.sleep(1)
    
    # Xử lý và loại bỏ duplicate links
    unique_links = process_and_deduplicate_links(all_links)
    
    # Download tất cả PDF
    print(f"\n📥 Bắt đầu download {len(unique_links)} PDFs...")
    pdf_count = 0
    for i, link in enumerate(unique_links):
        print(f"📄 {i+1}/{len(unique_links)}: {link}")
        try:
            download_pdf(link, session)
            pdf_count += 1
        except Exception as e:
            print(f"❌ Lỗi download: {e}")
    
    # Cập nhật số PDF đã download
    checkpoint_data["total_pdfs_downloaded"] = pdf_count
    checkpoint_data["is_completed"] = True
    save_checkpoint(checkpoint_data)
    
    print(f"\n🎉 HOÀN THÀNH BATCH!")
    print(f"   📂 DROP_LEVELS: {drop_levels}")
    print(f"   📦 Batch: {batch_num}")
    print(f"   📄 Pages: {start_page}-{end_page}")
    print(f"   🔗 Total links: {checkpoint_data['total_links_found']}")
    print(f"   📥 PDFs downloaded: {pdf_count}")
    print(f"   💾 Checkpoint: {get_checkpoint_filename(drop_levels, batch_num)}")


if __name__ == "__main__":
    main()
