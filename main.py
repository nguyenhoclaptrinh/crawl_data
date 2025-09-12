import json
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import urllib3
import time
import os
from crawl_url_pdf import download_pdf
from config import BASE_URL, BASE_DOMAIN, DATASET_DIR, CHECKPOINT_FILE, BATCH_SIZE, NUM_BATCHES, DROP_LEVELS, SEARCH_KEYWORD
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


def setup_batch_configuration():
    """Thiết lập cấu hình batch và checkpoint"""
    pair_list = []
    checkpoint_dict = {}
    for i in range(1, NUM_BATCHES + 1):
        start = (i - 1) * BATCH_SIZE + 1
        end = i * BATCH_SIZE
        pair_list.append((start, end))
        checkpoint_dict[i] = 0
    return pair_list, checkpoint_dict


def load_checkpoint():
    """Tải checkpoint từ file"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            checkpoint_dict = json.load(f)
            print("Loaded checkpoint:", checkpoint_dict)
            return checkpoint_dict
    else:
        return None


def save_checkpoint(checkpoint_dict):
    """Lưu checkpoint vào file"""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint_dict, f)


def display_checkpoint_status(checkpoint_dict):
    """Hiển thị trạng thái checkpoint và cho phép user chọn batch"""
    for i in range(1, NUM_BATCHES + 1):
        if checkpoint_dict[str(i)] != 0:
            print(f"Batch {i} already completed up to page {checkpoint_dict[str(i)]}. Skipping.")
        else:
            print(f"Batch {i} not started yet.")
    
    input_str = input(f"Chọn số tiếp tục batch: ")
    return int(input_str)


def create_payload(hidden_fields, page):
    """Tạo payload cho request tùy theo page số"""
    if page == 1:
        # Lần đầu: bấm nút "Tìm kiếm"
        return {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": "Nhập tên vụ/việc hoặc số bản án, quyết định",
            "ctl00$Content_home_Public$ctl00$Drop_Levels": DROP_LEVELS,
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$cmd_search_banner": "Tìm kiếm"
        }
    else:
        return {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": "Nhập tên vụ/việc hoặc số bản án, quyết định",
            "ctl00$Content_home_Public$ctl00$Drop_Levels": DROP_LEVELS,
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$DropPages": str(page),
            "__EVENTTARGET": "ctl00$Content_home_Public$ctl00$DropPages",
            "__EVENTARGUMENT": ""
        }


def crawl_page(session, page, hidden_fields):
    """Crawl một page và trả về danh sách links + hidden_fields mới"""
    payload = create_payload(hidden_fields, page)
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = session.post(BASE_URL, data=payload, headers=headers, verify=False)
        response.raise_for_status()
        
        print(f"Page {page} fetched successfully.")
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        links = [a["href"] for a in soup.find_all("a", href=True)]
        page_links = []
        
        for link in links:
            text_split = link.split("/")
            if len(text_split) > 2 and text_split[2] == "chi-tiet-ban-an":
                full_link = BASE_DOMAIN + link
                page_links.append(full_link)
        
        new_hidden_fields = get_hidden_fields(response.text)
        return page_links, new_hidden_fields
        
    except RequestException as e:
        print(f"Error on page {page}: {e}")
        return [], hidden_fields


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
    """Hàm chính điều phối toàn bộ quá trình crawl"""
    # Khởi tạo session và hidden fields
    session, hidden_fields = initialize_session()
    
    # Thiết lập cấu hình batch
    pair_list, checkpoint_dict = setup_batch_configuration()
    
    # Tải checkpoint nếu có
    loaded_checkpoint = load_checkpoint()
    if loaded_checkpoint:
        checkpoint_dict = loaded_checkpoint
    else:
        save_checkpoint(checkpoint_dict)
    
    # Hiển thị trạng thái và cho user chọn batch
    batch_choice = display_checkpoint_status(checkpoint_dict)
    
    # Crawl các pages trong batch đã chọn
    all_links = []
    for page in range(pair_list[batch_choice - 1][0], pair_list[batch_choice - 1][1] + 1):
        page_links, hidden_fields = crawl_page(session, page, hidden_fields)
        all_links.extend(page_links)
    
    # Xử lý và loại bỏ duplicate links
    unique_links = process_and_deduplicate_links(all_links)
    
    # Download tất cả PDF
    download_all_pdfs(unique_links, session)
    
    # Cập nhật checkpoint
    checkpoint_dict[str(batch_choice)] = 1
    save_checkpoint(checkpoint_dict)


if __name__ == "__main__":
    main()
