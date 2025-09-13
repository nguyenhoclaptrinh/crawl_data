import time
from typing import Callable, Tuple, Dict, Any


def retry_page(page: int,
               session,
               hidden_fields: dict,
               drop_levels: str,
               BASE_DOMAIN: str,
               SEARCH_KEYWORD: str,
               crawl_fn: Callable,
               download_fn: Callable,
               checkpoint_data: dict,
               max_retries: int = 3,
               retry_delay: int = 5) -> Tuple[dict, dict]:
    """Retry logic for a single page.

    Args:
        page: page number to fetch
        session: requests session
        hidden_fields: current hidden fields dict
        drop_levels, BASE_DOMAIN, SEARCH_KEYWORD: params forwarded to crawl_fn
        crawl_fn: function(session, page, hidden_fields, drop_levels, BASE_DOMAIN, SEARCH_KEYWORD) -> (links, new_hidden, success)
        download_fn: function(link, session, drop_levels, page, index) -> None
        checkpoint_data: checkpoint dict to be updated
        max_retries: maximum attempts (total) allowed
        retry_delay: seconds to wait between attempts

    Returns:
        (checkpoint_data, hidden_fields)
    """
    # Determine current retries
    retry_counts = checkpoint_data.get("page_retry_counts", {})
    key = str(page)
    current = retry_counts.get(key, 0)

    # If already exceeded, return immediately
    if current >= max_retries:
        print(f"⚠️ Page {page} already reached retry limit ({current}/{max_retries})")
        return checkpoint_data, hidden_fields

    attempt = current + 1
    print(f"🔁 Retrying page {page} (attempt {attempt}/{max_retries})")
    links, new_hidden, success = crawl_fn(session, page, hidden_fields, drop_levels, BASE_DOMAIN, SEARCH_KEYWORD)

    # Update checkpoint via the helper (assumes external function update_checkpoint_progress exists)
    # We don't import checkpoint_utils here to keep this module generic; caller should call update_checkpoint_progress
    if success:
        print(f"✅ Retry success for page {page}: {len(links)} links")
        # Download the found links
        for i, link in enumerate(links):
            print(f"   📄 Downloading PDF {i+1}/{len(links)}: {link}")
            try:
                download_fn(link, session, drop_levels, page, i+1)
                checkpoint_data["total_pdfs_downloaded"] = checkpoint_data.get("total_pdfs_downloaded", 0) + 1
            except Exception as e:
                print(f"   ❌ Lỗi download: {e}")
        # Reset retry count on success
        if "page_retry_counts" in checkpoint_data and key in checkpoint_data["page_retry_counts"]:
            checkpoint_data["page_retry_counts"].pop(key, None)
        # Mark as completed
        if page not in checkpoint_data.get("completed_pages", []):
            checkpoint_data.setdefault("completed_pages", []).append(page)
        # Update last processed page if applicable
        if page > checkpoint_data.get("last_processed_page", 0):
            checkpoint_data["last_processed_page"] = page
    else:
        print(f"❌ Retry failed for page {page}")
        # increment retry count
        checkpoint_data.setdefault("page_retry_counts", {})[key] = checkpoint_data.get("page_retry_counts", {}).get(key, 0) + 1
        checkpoint_data.setdefault("failed_pages", [])
        if page not in checkpoint_data["failed_pages"]:
            checkpoint_data["failed_pages"].append(page)

    # Return updated checkpoint and new hidden fields
    # If crawl_fn returned new_hidden, update it
    if new_hidden:
        hidden_fields = new_hidden

    # Sleep between retries
    time.sleep(retry_delay)

    return checkpoint_data, hidden_fields
