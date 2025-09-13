# config.py
# Cấu hình chung cho dự án

BASE_URL = "https://congbobanan.toaan.gov.vn/0tat1cvn/ban-an-quyet-dinh"
BASE_DOMAIN = "https://congbobanan.toaan.gov.vn"
DATASET_DIR = "E:/CRAWL_DATA"
CHECKPOINT_DIR = "./checkpoints"

DATASET_CLEANING_DIR = "E:/Dataset_Cleaning"

# Cấu hình batch - sẽ được tính toán tự động
DEFAULT_MAX_PAGES = 100  # Số page tối đa mặc định
DEFAULT_BATCH_SIZE = 10  # Kích thước batch mặc định
MIN_BATCH_SIZE = 1       # Kích thước batch tối thiểu
MAX_BATCH_SIZE = 50      # Kích thước batch tối đa

# Cấu hình form parameters
DROP_LEVELS_OPTIONS = {
    "TW": "Tòa án nhân dân tối cao",
    "CW": "Tòa án cấp cao", 
    "T": "Tòa án nhân dân cấp tỉnh",
    "H": "Tòa án nhân dân cấp huyện"
}
DEFAULT_DROP_LEVELS = "T"
SEARCH_KEYWORD = "Nhập tên vụ/việc hoặc số bản án, quyết định"

# Retry configuration for failed pages
# How many times to retry fetching a page before marking it as permanently failed
PAGE_RETRY_LIMIT = 3
# Seconds to wait between retry attempts
RETRY_DELAY_SECONDS = 5

# Convention đặt tên file checkpoint: checkpoint_{drop_levels}_{batch}.json
# Ví dụ: checkpoint_T_1.json, checkpoint_H_2.json