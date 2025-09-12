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


def get_user_configuration():
    """Cho phép user nhập maxpages và tự động tính toán batch configuration"""
    print("\n" + "="*60)
    print("⚙️  CẤU HÌNH CRAWL DATA")
    print("="*60)

    # Nhập số pages tối đa
    while True:
        try:
            max_pages_input = input(
                f"📄 Nhập số pages tối đa để crawl (mặc định {DEFAULT_MAX_PAGES}): ").strip()
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

    # Tự động tính toán batch size tối ưu hoặc cho user chọn
    print(f"\n📊 Với {max_pages} pages, các tùy chọn batch size:")

    # Tính toán các tùy chọn batch size hợp lý
    batch_options = calculate_batch_options(max_pages)

    for i, option in enumerate(batch_options):
        batch_size = option["batch_size"]
        num_batches = option["num_batches"]
        efficiency = option["efficiency"]
        print(
            f"  [{i+1}] Batch size {batch_size}: {num_batches} batches (hiệu quả: {efficiency:.1f}%)")

    print(f"  [0] Tự nhập batch size")

    # User chọn batch size
    while True:
        try:
            choice = input(
                f"\nChọn tùy chọn (1-{len(batch_options)} hoặc 0): ").strip()
            if not choice:
                # Mặc định chọn tùy chọn đầu tiên (tối ưu nhất)
                chosen_batch_size = batch_options[0]["batch_size"]
                break

            choice_num = int(choice)
            if choice_num == 0:
                chosen_batch_size = get_custom_batch_size(max_pages)
                break
            elif 1 <= choice_num <= len(batch_options):
                chosen_batch_size = batch_options[choice_num - 1]["batch_size"]
                break
            else:
                print(f"❌ Vui lòng chọn từ 0 đến {len(batch_options)}")
        except ValueError:
            print("❌ Vui lòng nhập số nguyên hợp lệ")

    # Tính toán số batches
    num_batches = calculate_num_batches(max_pages, chosen_batch_size)

    print(f"\n✅ Cấu hình đã chọn:")
    print(f"   📄 Max pages: {max_pages}")
    print(f"   📦 Batch size: {chosen_batch_size}")
    print(f"   🔢 Số batches: {num_batches}")
    print(
        f"   📊 Pages trong batch cuối: {max_pages % chosen_batch_size if max_pages % chosen_batch_size != 0 else chosen_batch_size}")

    return max_pages, chosen_batch_size, num_batches


def calculate_batch_options(max_pages):
    """Tính toán các tùy chọn batch size hợp lý"""
    options = []

    # Thử các batch size từ MIN đến MAX
    for batch_size in range(MIN_BATCH_SIZE, min(MAX_BATCH_SIZE, max_pages) + 1):
        num_batches = calculate_num_batches(max_pages, batch_size)

        # Tính hiệu quả (% pages được sử dụng đầy đủ)
        full_batches = max_pages // batch_size
        remaining_pages = max_pages % batch_size
        if remaining_pages == 0:
            efficiency = 100.0
        else:
            efficiency = (full_batches * batch_size +
                          remaining_pages) / (num_batches * batch_size) * 100

        options.append({
            "batch_size": batch_size,
            "num_batches": num_batches,
            "efficiency": efficiency
        })

    # Sắp xếp theo hiệu quả giảm dần, rồi theo batch size tăng dần
    options.sort(key=lambda x: (-x["efficiency"], x["batch_size"]))

    # Chỉ trả về 5 tùy chọn tốt nhất
    return options[:5]


def get_custom_batch_size(max_pages):
    """Cho phép user nhập batch size tùy chỉnh"""
    while True:
        try:
            batch_size = int(input(
                f"📦 Nhập batch size ({MIN_BATCH_SIZE}-{min(MAX_BATCH_SIZE, max_pages)}): "))
            if MIN_BATCH_SIZE <= batch_size <= min(MAX_BATCH_SIZE, max_pages):
                return batch_size
            else:
                print(
                    f"❌ Batch size phải từ {MIN_BATCH_SIZE} đến {min(MAX_BATCH_SIZE, max_pages)}")
        except ValueError:
            print("❌ Vui lòng nhập số nguyên hợp lệ")


def calculate_num_batches(max_pages, batch_size):
    """Tính số batches cần thiết"""
    return (max_pages + batch_size - 1) // batch_size  # Ceiling division


def get_batch_page_range(batch_num, batch_size, max_pages):
    """Tính toán start_page và end_page cho một batch cụ thể"""
    start_page = (batch_num - 1) * batch_size + 1
    end_page = min(batch_num * batch_size, max_pages)
    return start_page, end_page


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


def create_checkpoint_structure(drop_levels, batch_num, start_page, end_page, max_pages, batch_size):
    """Tạo cấu trúc checkpoint mới với thông tin batch configuration"""
    return {
        "drop_levels": drop_levels,
        "drop_levels_name": DROP_LEVELS_OPTIONS.get(drop_levels, f"Cấp {drop_levels}"),
        "batch_number": batch_num,
        "batch_size": batch_size,
        "max_pages": max_pages,
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
            print(
                f"✅ Loaded checkpoint: {get_checkpoint_filename(drop_levels, batch_num)}")
            print(
                f"   📄 Pages: {checkpoint_data['start_page']}-{checkpoint_data['end_page']}")
            print(
                f"   ✏️  Last processed: {checkpoint_data['last_processed_page']}")
            print(f"   🔗 Links found: {checkpoint_data['total_links_found']}")
            print(
                f"   📥 PDFs downloaded: {checkpoint_data['total_pdfs_downloaded']}")
            return checkpoint_data
    else:
        print(
            f"❌ Không tìm thấy checkpoint: {get_checkpoint_filename(drop_levels, batch_num)}")
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
            parts = filename.replace("checkpoint_", "").replace(
                ".json", "").split("_")
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


def display_checkpoint_status_and_choose(max_pages, batch_size, num_batches):
    """Hiển thị trạng thái tất cả checkpoint và cho phép user chọn"""
    print("\n" + "="*80)
    print("📊 HỆ THỐNG CHECKPOINT THEO DROP_LEVELS + BATCH")
    print("="*80)
    print(
        f"📄 Configuration: {max_pages} pages, batch size {batch_size}, {num_batches} batches")

    # Hiển thị các DROP_LEVELS có sẵn
    print("\n🎯 Các cấp tòa án:")
    for key, name in DROP_LEVELS_OPTIONS.items():
        print(f"  [{key}] {name}")

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
                batch_info = f"Batch {data['batch_number']}"
                if "batch_size" in data:
                    batch_info += f" (size {data['batch_size']})"

                print(f"  {status_icon} {ckpt['filename']}: "
                      f"{batch_info}, Pages {progress}, "
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
        return choose_new_batch(max_pages, batch_size, num_batches)
    elif choice == "2":
        result = choose_existing_batch(checkpoints)
        if result is None:
            print("🔄 Chuyển sang tạo batch mới...")
            return choose_new_batch(max_pages, batch_size, num_batches)
        return result
    else:
        print("❌ Lựa chọn không hợp lệ, sử dụng mặc định: tạo batch mới")
        return choose_new_batch(max_pages, batch_size, num_batches)


def choose_new_batch(max_pages, batch_size, num_batches):
    """Cho phép user chọn DROP_LEVELS và batch để tạo mới"""
    print("\n🆕 TẠO BATCH MỚI")
    print("-" * 30)
    # Chỉ cho phép user chọn DROP_LEVELS
    drop_levels = input(
        f"Chọn cấp tòa án (TW/CW/T/H hoặc Enter cho {DEFAULT_DROP_LEVELS}): ").strip().upper()
    if not drop_levels:
        drop_levels = DEFAULT_DROP_LEVELS
    if drop_levels not in DROP_LEVELS_OPTIONS:
        print(f"❌ Cấp '{drop_levels}' không hợp lệ, sử dụng mặc định '{DEFAULT_DROP_LEVELS}'")
        drop_levels = DEFAULT_DROP_LEVELS
    print(f"\n✅ Sẽ tạo batch đa luồng cho cấp tòa án: {drop_levels} ({DROP_LEVELS_OPTIONS[drop_levels]})")
    return drop_levels


def choose_existing_batch(checkpoints):
    """Cho phép user chọn batch đã có để tiếp tục"""
    if not checkpoints:
        print("❌ Không có checkpoint nào để tiếp tục")
        # Không thể gọi choose_new_batch() vì thiếu tham số, return None để main xử lý
        return None

    print(f"\n🔄 TIẾP TỤC BATCH ĐÃ CÓ")
    print("-" * 30)

    incomplete_checkpoints = []
    for i, ckpt in enumerate(checkpoints):
        data = load_checkpoint(ckpt["drop_levels"], ckpt["batch_number"])
        if data and not data["is_completed"]:
            incomplete_checkpoints.append((i, ckpt, data))
            print(
                f"  [{len(incomplete_checkpoints)}] {ckpt['filename']}: Pages {data['last_processed_page']}/{data['end_page']}")

    if not incomplete_checkpoints:
        print("❌ Không có checkpoint nào chưa hoàn thành")
        return None

    try:
        choice_idx = int(
            input("Chọn checkpoint để tiếp tục (số thứ tự): ")) - 1
        if 0 <= choice_idx < len(incomplete_checkpoints):
            original_idx, ckpt, data = incomplete_checkpoints[choice_idx]

            return (ckpt["drop_levels"], ckpt["batch_number"],
                    data["start_page"], data["end_page"], data)
        else:
            print("❌ Lựa chọn không hợp lệ")
            return None
    except ValueError:
        print("❌ Vui lòng nhập số")
        return None


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
        response = session.post(BASE_URL, data=payload,
                                headers=headers, verify=False)
        response.raise_for_status()

        print(
            f"📄 Page {page} (DROP_LEVELS={drop_levels}) fetched successfully.")

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


# --- ĐA LUỒNG: mỗi batch là 1 luồng độc lập ---


def batch_worker(drop_levels, batch_num, start_page, end_page, max_pages, batch_size, existing_checkpoint=None):
    print(
        f"\n🚀 [BATCH {batch_num}] Bắt đầu từ page {start_page} đến {end_page} (DROP_LEVELS={drop_levels})")
    session, hidden_fields = initialize_session()
    if existing_checkpoint:
        checkpoint_data = existing_checkpoint
        print(
            f"[BATCH {batch_num}] 🔄 Tiếp tục từ page {checkpoint_data['last_processed_page'] + 1}")
        start_from_page = checkpoint_data['last_processed_page'] + 1
    else:
        checkpoint_data = create_checkpoint_structure(
            drop_levels, batch_num, start_page, end_page, max_pages, batch_size)
        save_checkpoint(checkpoint_data)
        print(
            f"[BATCH {batch_num}] 🆕 Tạo checkpoint mới: {get_checkpoint_filename(drop_levels, batch_num)}")
        start_from_page = start_page

    for page in range(start_from_page, end_page + 1):
        print(f"[BATCH {batch_num}] --- Processing Page {page}/{end_page} ---")
        page_links, hidden_fields, success = crawl_page(
            session, page, hidden_fields, drop_levels)
        checkpoint_data = update_checkpoint_progress(
            checkpoint_data, page, len(page_links), success)
        if success:
            print(
                f"[BATCH {batch_num}] ✅ Page {page} hoàn thành: {len(page_links)} links")
            for i, link in enumerate(page_links):
                print(
                    f"[BATCH {batch_num}]    📄 Downloading PDF {i+1}/{len(page_links)}: {link}")
                try:
                    download_pdf(link, session)
                    checkpoint_data["total_pdfs_downloaded"] += 1
                except Exception as e:
                    print(f"[BATCH {batch_num}]    ❌ Lỗi download: {e}")
        else:
            print(f"[BATCH {batch_num}] ❌ Page {page} thất bại")
        save_checkpoint(checkpoint_data)
        progress_percent = ((page - start_page + 1) /
                            (end_page - start_page + 1)) * 100
        print(
            f"[BATCH {batch_num}] 📈 Progress: {progress_percent:.1f}% ({page - start_page + 1}/{end_page - start_page + 1} pages)")
        time.sleep(1)
    checkpoint_data["is_completed"] = True
    save_checkpoint(checkpoint_data)
    print(f"[BATCH {batch_num}] 🎉 HOÀN THÀNH!")


def main():
    print("🚀 CRAWL DỮ LIỆU BẢN ÁN - ĐA LUỒNG THEO BATCH")
    max_pages, batch_size, num_batches = get_user_configuration()

    # Gom các batch thành danh sách
    batch_jobs = []
    for batch_num in range(1, num_batches + 1):
        start_page, end_page = get_batch_page_range(
            batch_num, batch_size, max_pages)
        batch_jobs.append({
            "batch_num": batch_num,
            "start_page": start_page,
            "end_page": end_page
        })

    # Hiển thị trạng thái checkpoint và cho user chọn DROP_LEVELS
    drop_levels = display_checkpoint_status_and_choose(
        max_pages, batch_size, num_batches)

    # Chuẩn bị checkpoint cho từng batch (nếu có)
    checkpoints = list_all_checkpoints()
    batch_ckpt_map = {}
    for ckpt in checkpoints:
        if ckpt["drop_levels"] == drop_levels:
            batch_ckpt_map[ckpt["batch_number"]] = load_checkpoint(
                drop_levels, ckpt["batch_number"])

    # Số luồng tối đa là 10 hoặc số batch
    max_workers = min(10, num_batches)
    print(f"\n🧵 Sử dụng tối đa {max_workers} luồng song song!")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for job in batch_jobs:
            batch_num = job["batch_num"]
            start_page = job["start_page"]
            end_page = job["end_page"]
            existing_ckpt = batch_ckpt_map.get(batch_num)
            futures.append(executor.submit(
                batch_worker,
                drop_levels,
                batch_num,
                start_page,
                end_page,
                max_pages,
                batch_size,
                existing_ckpt
            ))
        # Đợi tất cả batch hoàn thành
        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("\n🎉 TẤT CẢ BATCH ĐÃ HOÀN THÀNH!")


if __name__ == "__main__":
    main()
