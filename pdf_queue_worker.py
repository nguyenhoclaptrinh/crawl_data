import os
import threading
import queue
from pdf_to_text import process_file
from config import DATASET_DIR

# Hàng đợi lưu tên file PDF cần convert
pdf_queue = queue.Queue()

# Worker: lấy file từ hàng đợi và convert
def pdf_converter_worker():
    while True:
        file_path = pdf_queue.get()
        if file_path is None:
            break  # Dùng None để báo hiệu dừng thread
        try:
            process_file(file_path)
            print(f"[Converter] Đã xử lý xong: {file_path}")
        except Exception as e:
            print(f"[Converter] Lỗi khi xử lý {file_path}: {e}")
        pdf_queue.task_done()

def start_pdf_converter_workers(num_workers=4):
    threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=pdf_converter_worker)
        t.daemon = True
        t.start()
        threads.append(t)
    return threads

def stop_pdf_converter_workers(threads):
    for _ in threads:
        pdf_queue.put(None)
    for t in threads:
        t.join()

# Hàm này sẽ được gọi khi tải xong 1 file PDF
# Ví dụ: gọi pdf_queue.put(file_path) sau khi download xong
# Trong main, gọi start_pdf_converter_workers() để khởi động các worker
