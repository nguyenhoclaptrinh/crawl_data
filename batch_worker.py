import time
from crawl_url_pdf import download_pdf
from crawl_utils import crawl_page
from checkpoint_utils import (
    create_checkpoint_structure, save_checkpoint, update_checkpoint_progress, load_checkpoint, get_checkpoint_filename
)

def batch_worker(drop_levels, batch_num, start_page, end_page, max_pages, batch_size, session, hidden_fields, existing_checkpoint=None, BASE_DOMAIN=None, SEARCH_KEYWORD=None):
    print(f"\n🚀 [BATCH {batch_num}] Bắt đầu từ page {start_page} đến {end_page} (DROP_LEVELS={drop_levels})")
    if existing_checkpoint:
        checkpoint_data = existing_checkpoint
        print(f"[BATCH {batch_num}] 🔄 Tiếp tục từ page {checkpoint_data['last_processed_page'] + 1}")
        start_from_page = checkpoint_data['last_processed_page'] + 1
    else:
        checkpoint_data = create_checkpoint_structure(
            drop_levels, batch_num, start_page, end_page, max_pages, batch_size)
        save_checkpoint(checkpoint_data)
        print(f"[BATCH {batch_num}] 🆕 Tạo checkpoint mới: {get_checkpoint_filename(drop_levels, batch_num)}")
        start_from_page = start_page

    for page in range(start_from_page, end_page + 1):
        print(f"[BATCH {batch_num}] --- Processing Page {page}/{end_page} ---")
        page_links, hidden_fields, success = crawl_page(
            session, page, hidden_fields, drop_levels, BASE_DOMAIN, SEARCH_KEYWORD)
        checkpoint_data = update_checkpoint_progress(
            checkpoint_data, page, len(page_links), success)
        if success:
            print(f"[BATCH {batch_num}] ✅ Page {page} hoàn thành: {len(page_links)} links")
            for i, link in enumerate(page_links):
                print(f"[BATCH {batch_num}]    📄 Downloading PDF {i+1}/{len(page_links)}: {link}")
                try:
                    download_pdf(link, session, drop_levels, page, i+1)
                    checkpoint_data["total_pdfs_downloaded"] += 1
                except Exception as e:
                    print(f"[BATCH {batch_num}]    ❌ Lỗi download: {e}")
        else:
            print(f"[BATCH {batch_num}] ❌ Page {page} thất bại")
        save_checkpoint(checkpoint_data)
        progress_percent = ((page - start_page + 1) / (end_page - start_page + 1)) * 100
        print(f"[BATCH {batch_num}] 📈 Progress: {progress_percent:.1f}% ({page - start_page + 1}/{end_page - start_page + 1} pages)")
        time.sleep(1)
    checkpoint_data["is_completed"] = True
    save_checkpoint(checkpoint_data)
    print(f"[BATCH {batch_num}] 🎉 HOÀN THÀNH!")
