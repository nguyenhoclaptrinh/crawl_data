import time
from crawl_url_pdf import download_pdf
from crawl_utils import crawl_page
from checkpoint_utils import (
    create_checkpoint_structure, save_checkpoint, update_checkpoint_progress, load_checkpoint, get_checkpoint_filename
)
from config import PAGE_RETRY_LIMIT, RETRY_DELAY_SECONDS
from retry_utils import retry_page

def batch_worker(drop_levels, batch_num, start_page, end_page, max_pages, batch_size, session, hidden_fields, existing_checkpoint=None, BASE_DOMAIN=None, SEARCH_KEYWORD=None):
    print(f"\n🚀 [BATCH {batch_num}] Bắt đầu từ page {start_page} đến {end_page} (DROP_LEVELS={drop_levels})")
    if existing_checkpoint:
        checkpoint_data = existing_checkpoint
        if checkpoint_data['last_processed_page'] == 0:
            start_from_page = start_page
        else:
            start_from_page = checkpoint_data['last_processed_page'] + 1
        print(f"[BATCH {batch_num}] 🔄 Tiếp tục từ page {start_from_page}")
    else:
        checkpoint_data = create_checkpoint_structure(
            drop_levels, batch_num, start_page, end_page, max_pages, batch_size)
        save_checkpoint(checkpoint_data)
        print(f"[BATCH {batch_num}] 🆕 Tạo checkpoint mới: {get_checkpoint_filename(drop_levels, batch_num)}")
        start_from_page = start_page

    # First, attempt to retry any previously failed pages up to the retry limit
    failed_pages = list(checkpoint_data.get("failed_pages", []))
    if failed_pages:
        print(f"[BATCH {batch_num}] 🔁 Found failed pages to retry: {failed_pages}")
    for fp in failed_pages:
        # Skip if already completed
        if fp in checkpoint_data.get("completed_pages", []):
            continue
        # Use common retry helper
        checkpoint_data, hidden_fields = retry_page(
            page=fp,
            session=session,
            hidden_fields=hidden_fields,
            drop_levels=drop_levels,
            BASE_DOMAIN=BASE_DOMAIN,
            SEARCH_KEYWORD=SEARCH_KEYWORD,
            crawl_fn=crawl_page,
            download_fn=download_pdf,
            checkpoint_data=checkpoint_data,
            max_retries=PAGE_RETRY_LIMIT,
            retry_delay=RETRY_DELAY_SECONDS
        )
        save_checkpoint(checkpoint_data)

    for page in range(start_from_page, end_page + 1):
        # skip pages already completed (from checkpoint)
        if page in checkpoint_data.get("completed_pages", []):
            print(f"[BATCH {batch_num}] ⏭️ Skipping already completed page {page}")
            continue
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
