# Báo cáo tiến độ — Khảo sát dữ liệu, RFM & phân cụm K-Means

Ngày: 2026-10-12 (Tuần 1, theo tiến độ dự kiến trong kế hoạch đề tài)

## 1. Dữ liệu đầu vào

- Nguồn: [Online Retail II (Kaggle)](https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset)
- File: `online_retail_II.xlsx`, 2 sheet (`Year 2009-2010`, `Year 2010-2011`)
- Tổng: **1,067,371 dòng giao dịch**, 8 cột: `Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country`
- Khoảng thời gian: 2009-12-01 → 2011-12-09

## 2. Khảo sát chất lượng dữ liệu (thực đo, script [`scripts/01_explore_data.py`](../scripts/01_explore_data.py))

| Vấn đề | Số dòng | Tỷ lệ |
|---|---|---|
| Thiếu `Customer ID` | 243,007 | 22.77% |
| Thiếu `Description` | 4,382 | 0.41% |
| Trùng lặp hoàn toàn | 34,335 | 3.22% |
| Đơn hàng hủy (`Invoice` bắt đầu bằng `C`) | 19,494 | 1.83% |
| `Quantity` ≤ 0 | 22,950 | — |
| `Price` ≤ 0 | 6,207 | — |

Ngoài ra phát hiện các `StockCode` không phải sản phẩm thực (phí/điều chỉnh sổ sách):
`POST` (bưu phí), `DOT` (bưu phí DOTCOM), `D` (giảm giá), `M`/`m` (thủ công), `BANK CHARGES`,
`ADJUST`, `S` (mẫu thử), `B` (nợ xấu), `AMAZONFEE`, `CRUK`, `GIFT`, `DCGS*` (dữ liệu rác, mô tả "update").

Top quốc gia theo số dòng: United Kingdom (981,330), EIRE (17,866), Germany (17,624), France (14,330).

## 3. Làm sạch dữ liệu (script [`scripts/02_clean_data.py`](../scripts/02_clean_data.py))

Quy trình loại bỏ tuần tự, có log số dòng ở mỗi bước:

| Bước | Số dòng còn lại | Loại bỏ |
|---|---|---|
| Dữ liệu thô | 1,067,371 | — |
| Bỏ trùng lặp | 1,033,036 | 34,335 |
| Bỏ thiếu Customer ID | 797,885 | 235,151 |
| Bỏ đơn hủy | 779,495 | 18,390 |
| Bỏ Quantity/Price ≤ 0 | 779,425 | 70 |
| Bỏ mã phí/điều chỉnh | 776,857 | 2,568 |

**Kết quả: 776,857 dòng sạch (72.8% dữ liệu gốc), 5,853 khách hàng duy nhất.**

Giới hạn cần nêu trong báo cáo cuối: loại bỏ 22.77% giao dịch do thiếu `Customer ID` là
đánh đổi cần thiết để phân tích theo khách hàng (RFM/segmentation), nhưng làm giảm độ phủ
khi tính tổng doanh thu toàn hệ thống — nên tách riêng pipeline cho FactSales doanh thu
(giữ cả khách ẩn danh) và pipeline cho RFM/clustering (chỉ khách có ID) nếu cần chính xác
tuyệt đối cho KPI doanh thu.

## 4. Tính RFM (script [`scripts/03_rfm.py`](../scripts/03_rfm.py))

- Ngày tham chiếu (reference date): 2011-12-10 (ngày giao dịch cuối + 1 ngày)
- 5,853 khách hàng

| Chỉ số | Mean | Std | Min | 25% | 50% | 75% | Max |
|---|---|---|---|---|---|---|---|
| Recency (ngày) | 200.2 | 208.5 | 1 | 25 | 95 | 379 | 739 |
| Frequency (số hóa đơn) | 6.26 | 12.76 | 1 | 1 | 3 | 7 | 375 |
| Monetary (£) | 2,918.5 | 14,335.5 | 2.95 | 340.85 | 856.03 | 2,240.90 | 580,987.04 |

Phân phối Frequency/Monetary lệch phải mạnh (đuôi dài) → áp dụng `log1p` trước khi chuẩn hóa
cho bước phân cụm.

## 5. Phân cụm K-Means (script [`scripts/04_kmeans.py`](../scripts/04_kmeans.py))

Đặc trưng: `Recency`, `log1p(Frequency)`, `log1p(Monetary)`, chuẩn hóa bằng `StandardScaler`.
Thử K = 2..10, đánh giá bằng Silhouette Score:

| K | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|
| Silhouette | **0.4182** | 0.4017 | 0.3611 | 0.3652 | 0.3474 | 0.3357 | 0.3154 | 0.3116 | 0.3010 |

Biểu đồ: [`docs/silhouette_scores.png`](silhouette_scores.png)

**Nhận xét quan trọng:** K=2 cho silhouette cao nhất nhưng chỉ tách "khách còn hoạt động"
vs "khách không hoạt động" — ít ý nghĩa kinh doanh để xây dashboard phân khúc. K=4 được
chọn làm phương án tham khảo cho báo cáo vì cho 4 nhóm khách hàng có đặc điểm rõ ràng, dù
silhouette thấp hơn (0.3611). Đây là đánh đổi phổ biến khi áp dụng K-Means cho RFM
segmentation, cần nêu rõ trong báo cáo cuối (mục "đánh giá hạn chế của mô hình").

### Phân khúc với K=2

| Cluster | Số KH | Recency TB | Frequency TB | Monetary TB (£) |
|---|---|---|---|---|
| 1 | 2,684 | 63.7 | 11.5 | 5,760.3 |
| 0 | 3,169 | 315.9 | 1.8 | 511.6 |

### Phân khúc với K=4 (đề xuất dùng cho dashboard)

| Cluster | Số KH | Recency TB | Frequency TB | Monetary TB (£) | Diễn giải đề xuất |
|---|---|---|---|---|---|
| 3 | 947 | 42.5 | 22.2 | 12,819.5 | Khách VIP — mua gần đây, rất thường xuyên, chi tiêu rất cao |
| 0 | 1,875 | 98.3 | 5.5 | 1,871.3 | Khách trung thành — mua đều, chi tiêu khá |
| 2 | 1,397 | 104.1 | 1.7 | 421.3 | Khách mới/ít tương tác — mua gần đây nhưng ít lần, chi tiêu thấp |
| 1 | 1,634 | 490.8 | 1.7 | 517.0 | Khách có nguy cơ rời bỏ — lâu không quay lại |

## 6. Kết quả / đầu ra đã có

- `data/processed/transactions_clean.csv` — 776,857 dòng giao dịch sạch
- `data/processed/rfm.csv` — 5,853 khách hàng với R/F/M
- `data/processed/customer_segments.csv` — nhãn cụm (K=2 và K=4) theo từng khách hàng
- `docs/silhouette_scores.png` — biểu đồ đánh giá K

## 7. Việc tiếp theo

1. Thiết kế Star Schema (FactSales, DimCustomer, DimProduct, DimDate, DimCountry) trong PostgreSQL.
2. Khởi động Docker Compose (Postgres + Airflow + Superset) — Docker Desktop đã cài, cần bật daemon.
3. Chuyển logic 3 script trên thành Airflow DAG (Extract → Transform → Load vào Postgres), lưu cả bảng `customer_segments` vào kho dữ liệu.
4. Xây dashboard Superset: doanh thu theo thời gian, số lượng/tỷ lệ từng nhóm khách hàng, so sánh chi tiêu trung bình giữa các nhóm.
