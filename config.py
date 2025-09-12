# config.py
# Cấu hình chung cho dự án

BASE_URL = "https://congbobanan.toaan.gov.vn/0tat1cvn/ban-an-quyet-dinh"
BASE_DOMAIN = "https://congbobanan.toaan.gov.vn"
DATASET_DIR = "./dataset"
CHECKPOINT_DIR = "./checkpoints"
BATCH_SIZE = 1
NUM_BATCHES = 9

# Cấu hình form parameters
DROP_LEVELS_OPTIONS = {
    "TW": "Tòa án nhân dân tối cao",
    "CW": "Tòa án cấp cao", 
    "T": "Tòa án nhân dân cấp tỉnh",
    "H": "Tòa án nhân dân cấp huyện"
}
DEFAULT_DROP_LEVELS = "T"
SEARCH_KEYWORD = "Nhập tên vụ/việc hoặc số bản án, quyết định"

# Convention đặt tên file checkpoint: checkpoint_{drop_levels}_{batch}.json
# Ví dụ: checkpoint_T_1.json, checkpoint_H_2.json