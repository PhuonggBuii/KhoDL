# Dữ liệu

Bộ dữ liệu **Online Retail II** (Kaggle):
https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset

## Cách lấy dữ liệu

1. Tải file `online_retail_II.xlsx` (hoặc `.csv`) từ trang Kaggle trên.
2. Đặt nguyên file vào thư mục `data/raw/` (không đổi tên, không chỉnh sửa).
3. Chạy `scripts/01_explore_data.py` để khảo sát dữ liệu.

Dữ liệu thô không được commit vào Git (xem `.gitignore`) vì dung lượng lớn và
có thể tải lại từ Kaggle bất cứ lúc nào — đảm bảo pipeline có thể chạy lại
(reproducible) mà không cần lưu trữ dữ liệu trong repo.
