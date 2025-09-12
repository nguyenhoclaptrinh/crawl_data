import concurrent.futures
from config import (BASE_DOMAIN, SEARCH_KEYWORD)
from crawl_utils import initialize_session
from checkpoint_utils import list_all_checkpoints, load_checkpoint
from batch_worker import batch_worker

def main(max_pages, batch_size, num_batches, drop_levels):
    batch_jobs = []
    for batch_num in range(1, num_batches + 1):
        start_page = (batch_num - 1) * batch_size + 1
        end_page = min(batch_num * batch_size, max_pages)
        batch_jobs.append({
            "batch_num": batch_num,
            "start_page": start_page,
            "end_page": end_page
        })
    checkpoints = list_all_checkpoints()
    batch_ckpt_map = {}
    for ckpt in checkpoints:
        if ckpt["drop_levels"] == drop_levels:
            batch_ckpt_map[ckpt["batch_number"]] = load_checkpoint(drop_levels, ckpt["batch_number"])
    max_workers = num_batches
    print(f"\n🧵 Sử dụng {max_workers} luồng song song!")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for job in batch_jobs:
            batch_num = job["batch_num"]
            start_page = job["start_page"]
            end_page = job["end_page"]
            session, hidden_fields = initialize_session()
            existing_ckpt = batch_ckpt_map.get(batch_num)
            futures.append(executor.submit(
                batch_worker,
                drop_levels,
                batch_num,
                start_page,
                end_page,
                max_pages,
                batch_size,
                session,
                hidden_fields,
                existing_ckpt,
                BASE_DOMAIN,
                SEARCH_KEYWORD
            ))
        for future in concurrent.futures.as_completed(futures):
            future.result()
    print("\n🎉 TẤT CẢ BATCH ĐÃ HOÀN THÀNH!")
