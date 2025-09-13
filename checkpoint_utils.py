import os
import time
import json
from config import CHECKPOINT_DIR, DROP_LEVELS_OPTIONS

def get_checkpoint_filename(drop_levels, batch_num):
    level_code = drop_levels if drop_levels else "ALL"
    return f"checkpoint_{level_code}_{batch_num}.json"

def get_checkpoint_filepath(drop_levels, batch_num):
    if not os.path.exists(CHECKPOINT_DIR):
        os.makedirs(CHECKPOINT_DIR)
    filename = get_checkpoint_filename(drop_levels, batch_num)
    return os.path.join(CHECKPOINT_DIR, filename)

def create_checkpoint_structure(drop_levels, batch_num, start_page, end_page, max_pages, batch_size):
    return {
        "drop_levels": drop_levels,
        "drop_levels_name": DROP_LEVELS_OPTIONS.get(drop_levels, f"Cấp {drop_levels}"),
        "batch_number": batch_num,
        "batch_size": batch_size,
        "max_pages": max_pages,
        "start_page": start_page,
        "end_page": end_page,
        "last_processed_page": 0,
        "total_links_found": 0,
        "total_pdfs_downloaded": 0,
        "failed_pages": [],
        # Track retry counts per page as strings -> int (json-safe keys)
        "page_retry_counts": {},
        "completed_pages": [],
        "created_at": time.time(),
        "last_updated": time.time(),
        "is_completed": False
    }

def load_checkpoint(drop_levels, batch_num):
    filepath = get_checkpoint_filepath(drop_levels, batch_num)
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            checkpoint_data = json.load(f)
            print(f"✅ Loaded checkpoint: {get_checkpoint_filename(drop_levels, batch_num)}")
            print(f"   📄 Pages: {checkpoint_data['start_page']}-{checkpoint_data['end_page']}")
            print(f"   ✏️  Last processed: {checkpoint_data['last_processed_page']}")
            print(f"   🔗 Links found: {checkpoint_data['total_links_found']}")
            print(f"   📥 PDFs downloaded: {checkpoint_data['total_pdfs_downloaded']}")
            # If retry information exists, show a compact summary
            if "page_retry_counts" in checkpoint_data and checkpoint_data["page_retry_counts"]:
                print(f"   🔁 Page retry counts: {checkpoint_data['page_retry_counts']}")
            return checkpoint_data
    else:
        print(f"❌ Không tìm thấy checkpoint: {get_checkpoint_filename(drop_levels, batch_num)}")
        return None

def save_checkpoint(checkpoint_data):
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
    if success:
        if page_num not in checkpoint_data["completed_pages"]:
            checkpoint_data["completed_pages"].append(page_num)
            checkpoint_data["total_links_found"] += links_found
        if page_num > checkpoint_data["last_processed_page"]:
            checkpoint_data["last_processed_page"] = page_num
        if page_num in checkpoint_data["failed_pages"]:
            checkpoint_data["failed_pages"].remove(page_num)
    else:
        if page_num not in checkpoint_data["failed_pages"]:
            checkpoint_data["failed_pages"].append(page_num)
        # increment retry count for this page (store as string key to be json-safe)
        if "page_retry_counts" not in checkpoint_data:
            checkpoint_data["page_retry_counts"] = {}
        key = str(page_num)
        checkpoint_data["page_retry_counts"][key] = checkpoint_data["page_retry_counts"].get(key, 0) + 1
    if checkpoint_data["last_processed_page"] >= checkpoint_data["end_page"]:
        checkpoint_data["is_completed"] = True
    return checkpoint_data

def list_all_checkpoints():
    if not os.path.exists(CHECKPOINT_DIR):
        return []
    checkpoint_files = []
    for filename in os.listdir(CHECKPOINT_DIR):
        if filename.startswith("checkpoint_") and filename.endswith(".json"):
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
