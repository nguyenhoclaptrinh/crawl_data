# Hệ thống crawl dữ liệu bản án Việt Nam

## Mục đích
Tự động thu thập, tải và lưu trữ các file PDF bản án từ website tòa án Việt Nam, hỗ trợ đa luồng, checkpoint, và dễ dàng tiếp tục hoặc tải lại từ đầu.

## Tính năng nổi bật
- Crawl đa luồng, chia batch linh hoạt, không giới hạn số luồng.
- Checkpoint tự động, có thể dừng/tiếp tục hoặc tải lại từ đầu.
- Đặt tên file PDF theo cấp tòa, số trang, thứ tự.
- Dễ cấu hình, giao diện dòng lệnh thân thiện.
- Có thể dừng bằng Ctrl+C bất cứ lúc nào.

## Hướng dẫn sử dụng
1. **Cài đặt thư viện:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Chạy chương trình:**
   ```bash
   python main.py
   ```
3. **Lựa chọn cấu hình:**
   - Nhập số trang muốn crawl.
   - Nhập số batch (luồng) muốn chạy song song.
   - Chọn cấp tòa án (TW/CW/T/H).
   - Có thể chọn xóa toàn bộ checkpoint để tải lại từ đầu.

4. **Kết quả:**
   - File PDF sẽ được lưu trong thư mục dataset/ với tên dạng: `Droplevel_Page_Index.pdf`.
   - Checkpoint lưu trong thư mục checkpoints/.

## Cấu trúc thư mục
```
├── main.py
├── main_batch.py
├── batch_worker.py
├── crawl_utils.py
├── checkpoint_utils.py
├── crawl_url_pdf.py
├── config.py
├── dataset/
├── checkpoints/
└── ...
```

## Lưu ý
- Số luồng lớn có thể gây tải cao cho CPU và mạng, nên chọn phù hợp với máy.
- Nếu muốn crawl lại từ đầu, hãy chọn xóa checkpoint khi được hỏi.
- Chương trình tự động checkpoint, có thể dừng và chạy lại bất cứ lúc nào.

## Đóng góp & bản quyền
- Tác giả: nguyenhoclaptrinh (cải tiến từ mã nguồn của trungkiet2005)
- Mọi đóng góp, phản hồi xin gửi qua Github hoặc email.
