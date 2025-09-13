import concurrent.futures
from config import (BASE_DOMAIN, SEARCH_KEYWORD)
from crawl_utils import initialize_session
from checkpoint_utils import list_all_checkpoints, load_checkpoint
from batch_worker import batch_worker
def main(max_pages, batch_size, total_batches, num_threads, drop_levels):
    checkpoints = list_all_checkpoints()
    batch_ckpt_map = {}
    for ckpt in checkpoints:
        if ckpt["drop_levels"] == drop_levels:
            batch_ckpt_map[ckpt["batch_number"]] = load_checkpoint(drop_levels, ckpt["batch_number"])
    # create list of batch numbers
    batch_numbers = list(range(1, total_batches + 1))

    # Map batches round-robin to threads: thread i gets batches i, i+num_threads, i+2*num_threads, ...
    assignments = {i: [] for i in range(1, num_threads + 1)}
    for idx, batch_num in enumerate(batch_numbers):
        thread_id = (idx % num_threads) + 1
        assignments[thread_id].append(batch_num)

    print(f"\n🧵 Sử dụng {num_threads} luồng; tổng batches: {total_batches}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for thread_id in range(1, num_threads + 1):
            assigned = assignments.get(thread_id, [])
            if not assigned:
                continue
            futures.append(executor.submit(worker_thread_runner, thread_id, assigned, max_pages, batch_size, drop_levels))
        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("\n🎉 TẤT CẢ BATCH ĐÃ HOÀN THÀNH!")

def worker_thread_runner(thread_id: int, assigned_batches: list, max_pages: int, batch_size: int, drop_levels: str):
    """Run assigned batch numbers sequentially for this thread."""
    for batch_num in assigned_batches:
        start_page = (batch_num - 1) * batch_size + 1
        end_page = min(batch_num * batch_size, max_pages)
        session, hidden_fields = initialize_session()
        existing_ckpt = load_checkpoint(drop_levels, batch_num)
        print(f"\n[THREAD {thread_id}] Starting assigned batch {batch_num} (pages {start_page}-{end_page})")
        batch_worker(
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
        )
