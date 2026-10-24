# Báo cáo đồ án — Kho dữ liệu và Khai phá dữ liệu: Online Retail II

> Tài liệu tổng hợp để nộp/bảo vệ. Toàn bộ số liệu trong tài liệu này là số liệu **đã chạy
> thật và kiểm thử thật** trên môi trường Docker Compose của repo (không có số liệu minh họa/
> giả định) — nguồn chi tiết từng phần được dẫn link kèm theo.

## 1. Mục tiêu và bài toán

Xây dựng hệ thống phân tích hành vi mua sắm bán lẻ end-to-end: tích hợp dữ liệu đa nguồn (mô
phỏng) → Data Lake → ETL (Airflow) → Data Warehouse (PostgreSQL, Star Schema) → OLAP → Khai
phá dữ liệu (RFM + K-Means) → Dashboard (Superset), trên dataset **Online Retail II** (Kaggle).

Bài toán nghiệp vụ: xác định **phân khúc khách hàng** (ai là khách VIP, khách trung thành,
khách mới, khách có nguy cơ rời bỏ) từ hành vi mua hàng (Recency/Frequency/Monetary), và cung
cấp **báo cáo doanh thu đa chiều** (theo thời gian, quốc gia, sản phẩm) cho việc ra quyết định.

## 2. Nguồn dữ liệu và tiền xử lý

**Dataset gốc:** [Online Retail II (Kaggle)](https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset)
— 1.067.371 dòng giao dịch bán lẻ online 2009–2011, 8 cột (`Invoice, StockCode, Description,
Quantity, InvoiceDate, Price, Customer ID, Country`).

**Ba "nguồn dữ liệu" mô phỏng** (API/File/Database) đều tách từ cùng 1 dataset này — **không
phải 3 nguồn độc lập ngoài đời thực** (đã nêu rõ, xem `README.md` mục A).

**Quy tắc làm sạch đã thống nhất** (áp dụng nhất quán ở cả baseline `scripts/02_clean_data.py`
và pipeline chính thức `scripts/transform/transform.py`):

| Bước | Số dòng còn lại | Loại bỏ |
|---|---|---|
| Dữ liệu thô | 1.067.371 | — |
| Bỏ trùng lặp hoàn toàn | 1.033.036 | 34.335 |
| Bỏ đơn hủy (`Invoice` bắt đầu `C`) | — | 18.390–19.494* |
| Bỏ `Quantity≤0` hoặc `Price≤0` | — | 70 |
| Bỏ `StockCode` phi sản phẩm (16 mã: `POST, DOT, D, M, BANK CHARGES, ADJUST, S, B, AMAZONFEE, CRUK, GIFT, DCGS*`) | — | 2.568 |
| **Kết quả (có Customer ID, dùng cho RFM)** | **776.857** | tổng 290.514 (27,2%) |
| **Kết quả (giữ cả thiếu Customer ID, dùng cho `fact_sales`)** | **1.003.670** | — |

*(\*) 776.857 dòng dùng cho RFM (baseline `02_clean_data.py` loại thiếu Customer ID trước khi
đếm bước này); `fact_sales` giữ nguyên các dòng này nên số liệu từng bước lệch thứ tự nhỏ,
tổng kết quả cuối khớp tuyệt đối giữa 2 pipeline (xem mục 10).*

**Quy tắc xử lý Customer ID thiếu (22,77% dataset gốc, 243.007 dòng):** giữ trong
`fact_sales` (`customer_key=NULL`, đo đủ tổng doanh thu), **loại khỏi RFM/phân khúc theo khách
hàng**. Doanh thu = `Quantity × Price`, giữ nguyên đơn vị GBP (£), không quy đổi tiền tệ.

Chi tiết đầy đủ: [`docs/01_bao_cao_tien_do.md`](01_bao_cao_tien_do.md),
[`docs/03_star_schema.md`](03_star_schema.md) mục 6, `PROJECT_CHECKLIST.md` mục 0.3.

## 3. Data Lake và ETL

**3 nguồn ingest** (Phase 1, đã kiểm thử thật):

| Nguồn mô phỏng | Công nghệ | Kết quả thật |
|---|---|---|
| API | FastAPI `GET /products` (`api/`) | 5.304 StockCode duy nhất |
| File | Đọc Excel trực tiếp (`scripts/ingest/ingest_file.py`) | 1.067.371 dòng, khớp khảo sát |
| Database | PostgreSQL nguồn `postgres-source`, bảng `source_customers` | 5.942 khách hàng duy nhất |

**ETL qua Airflow** (DAG `retail_online_ii_etl`, `airflow/dags/retail_etl_dag.py`): luồng
`start → extract (API/File/Database song song) → transform → load → validate → end`. Toàn bộ
4 nhóm task gọi hàm `run()` thật (không phải stub) — **đã chạy SUCCESS đầu-cuối thật** trong
container.

- **Transform** (`scripts/transform/transform.py`): đọc trực tiếp dataset gốc (không join lại
  3 nguồn raw — xem hạn chế kiến trúc ở mục 7), áp dụng 5 quy tắc nghiệp vụ theo thứ tự, xuất
  `datalake/processed/<ngày>/{dim_*,fact_sales}.csv`.
- **Load** (`scripts/load/load.py`): UPSERT 4 dimension (không xóa dòng cũ), TRUNCATE + INSERT
  lại toàn bộ `fact_sales` (idempotent) — **đã test chạy 4 lần** (host×2 + Airflow×2),
  `fact_sales` luôn đúng **1.003.670 dòng**, không tăng.
- **Validate**: kiểm tra thật orphan FK (4 chiều), đơn hủy lọt vào fact, công thức revenue —
  **PASS toàn bộ**, không báo thành công giả.

## 4. Thiết kế và triển khai Data Warehouse

**Star Schema** (`sql/001_create_star_schema.sql`, thiết kế đầy đủ: [`docs/03_star_schema.md`](03_star_schema.md)):

- **Grain `fact_sales`**: 1 dòng = 1 dòng hàng (line item) trong 1 hóa đơn (đã xác minh 42.638
  cặp `(Invoice, StockCode)` lặp >1 dòng → cần `sale_key` surrogate riêng, không dùng khóa tự
  nhiên).
- 4 dimension (`dim_date, dim_customer, dim_product, dim_country`) + `fact_sales`, PK là
  `BIGINT GENERATED ALWAYS AS IDENTITY`, `revenue` là cột `GENERATED ALWAYS AS
  (ROUND(quantity*unit_price,2)) STORED` (không thể lệch công thức).
- `customer_key` NULLABLE (dòng thiếu Customer ID); `country_key` lấy từ **từng dòng giao
  dịch** (không suy ra từ khách hàng); **không có `dim_time`** (MVP chỉ `dim_date`, giờ/phút giữ
  trong `fact_sales.invoice_datetime`).

**Số liệu thật sau khi nạp:** `dim_date=604`, `dim_customer=5.853`, `dim_product=4.905`,
`dim_country=43`, `fact_sales=1.003.670` dòng, tổng revenue = **£19.659.453,86**. Đã xác thực
đầy đủ: 0 orphan FK (4 chiều), 0 đơn hủy lọt vào fact, công thức revenue không lệch, 226.813
dòng `customer_key=NULL` đúng thiết kế (= 1.003.670 − 776.857).

**OLAP** (`sql/002_olap_views.sql`, 13 view, thiết kế đầy đủ: [`docs/04_olap.md`](04_olap.md)):

- **Roll-up/Drill-down**: Năm → Quý → Tháng → Ngày (`v_olap_sales_yearly/quarterly/monthly/daily`).
- **Slice**: theo quốc gia (`v_olap_sales_by_country`), sản phẩm (`v_olap_product_revenue`,
  `v_olap_top10_products`), khách hàng (`v_olap_customer_revenue`, `v_olap_top10_customers`).
- **Dice**: Quốc gia × Năm (`v_olap_dice_country_year`).
- **Pivot**: ma trận Quốc gia (hàng) × Năm 2009/2010/2011 (cột) (`v_olap_pivot_country_by_year`).
- **KPI tổng quan** (`v_olap_kpi_summary`): revenue=19.659.453,86, fact_row_count=1.003.670,
  invoice_count=39.536, customer_count=5.853, product_count=4.905, country_count=43.

Đã kiểm thử: chạy file view 2 lần liên tiếp (idempotent, `CREATE OR REPLACE VIEW`), số liệu
từng view đối chiếu khớp tuyệt đối với truy vấn trực tiếp trên `fact_sales`.

## 5. Khai phá dữ liệu — RFM + K-Means

**2 pipeline** (giữ cả 2, không xóa baseline):

1. **Baseline** (`scripts/03_rfm.py`, `scripts/04_kmeans.py`) — đọc từ
   `data/processed/transactions_clean.csv`, dùng làm tham chiếu đối chiếu.
2. **Chính thức, đọc từ DW** (`scripts/mining/rfm_dw.py`, `cluster_dw.py`,
   `load_segments_dw.py`) — đọc trực tiếp `fact_sales JOIN dim_customer` (loại
   `customer_key IS NULL`). Chi tiết đầy đủ: [`docs/05_rfm_kmeans_dw.md`](05_rfm_kmeans_dw.md).

**Công thức RFM**: Recency = số ngày từ lần mua gần nhất đến ngày tham chiếu (= ngày giao dịch
lớn nhất trong tập đã lọc + 1 ngày = **2011-12-10**); Frequency = số `invoice_no` duy nhất;
Monetary = tổng `revenue`. Phạm vi: **5.853 khách hàng** có Customer ID.

**Đối chiếu 2 pipeline (đã chạy thật):** khớp tuyệt đối — 0 khách hàng lệch, lệch Monetary tối
đa 0,002 (sai số dấu phẩy động).

**Chuẩn hóa:** `Recency` giữ nguyên, `log1p(Frequency)`, `log1p(Monetary)` (giảm lệch phải),
`StandardScaler`. K-Means `random_state=42`, `n_init=10` (cố định để tái lập).

**Chọn K — Silhouette Score K=2..10:**

| K | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|
| Silhouette | **0,4182** | 0,4017 | 0,3611 | 0,3652 | 0,3474 | 0,3357 | 0,3154 | 0,3116 | 0,3010 |

K=2 có Silhouette cao nhất nhưng chỉ tách "còn hoạt động" vs "không hoạt động", ít ý nghĩa
kinh doanh. **Chọn K=4** vì lý do nghiệp vụ (chấp nhận trade-off Silhouette thấp hơn để đổi lấy
khả năng diễn giải phân khúc) — Silhouette K=4 tính từ DW = 0,3611, **khớp tuyệt đối** giá trị
đã tính từ baseline.

**4 phân khúc (K=4):**

| Cụm | Số KH | Recency TB | Frequency TB | Monetary TB (£) | Diễn giải |
|---|---|---|---|---|---|
| VIP | 947 | 42,5 | 22,2 | 12.819,5 | Mua gần đây, rất thường xuyên, chi tiêu rất cao |
| Trung thành | 1.875 | 98,3 | 5,5 | 1.871,3 | Mua đều, chi tiêu khá |
| Mới/ít tương tác | 1.397 | 104,1 | 1,7 | 421,3 | Mua gần đây nhưng ít lần, chi tiêu thấp |
| Nguy cơ rời bỏ | 1.634 | 490,8 | 1,7 | 517,0 | Lâu không quay lại |

Tổng 947+1.875+1.397+1.634 = **5.853** (khớp số khách hàng).

**Nạp vào DW (Phase 5.5):** bảng `customer_segment` riêng (không `ALTER dim_customer`,
`sql/003_customer_segment.sql`), PK=FK=`customer_key`, UPSERT (`load_segments_dw.py`). Đã kiểm
thử: 5.853 dòng, 0 orphan FK, chạy UPSERT lần 2 vẫn 5.853 dòng (không trùng), DDL chạy lại
idempotent.

## 6. Dashboard Superset và đánh giá kết quả

Cấu hình qua REST API (`scripts/superset/provision_superset.py`), chi tiết:
[`docs/06_superset_dashboard.md`](06_superset_dashboard.md).

- **Kết nối**: database "Retail DW" → `postgres-dw` (đã thêm driver `psycopg2-binary` vào
  container Superset).
- **5 dataset**: `v_olap_sales_daily`, `v_olap_sales_by_country`, `v_olap_top10_products`,
  `customer_segment`, `v_olap_kpi_summary`.
- **6 chart**, tất cả đã kiểm thử bằng cách đọc lại `params` đã lưu và gọi API
  `/api/v1/chart/data` thật (không giả định):
  1. Doanh thu theo tháng (line) — tổng 19.659.453,86, khớp `fact_sales`.
  2. Doanh thu theo quốc gia (bar) — UK cao nhất 16.806.060,61.
  3. Top 10 sản phẩm theo doanh thu (bar).
  4. Số khách hàng theo phân khúc (pie) — 947+1.875+1.397+1.634=5.853.
  5. RFM trung bình theo phân khúc (bar, 2 metric) — khớp bảng mục 5.
  6. KPI tổng quan (table) — khớp toàn bộ số liệu `v_olap_kpi_summary`.
- **Dashboard** "Online Retail II - Tong quan kinh doanh" (`id=1`), 4 hàng, 2 bộ lọc (Khoảng
  thời gian trên `full_date`, Quốc gia trên `country_name`). Idempotent — chạy lại script 2
  lần không tạo object trùng.
- **Giới hạn đã biết**: chưa xác nhận bằng mắt trong trình duyệt (môi trường phát triển không
  có browser) — cần người dùng mở UI kiểm tra trực quan + chụp ảnh minh chứng (xem mục 9, bước
  thủ công).

## 7. Kiểm thử và hạn chế

**Kiểm thử đã chạy thật (tổng hợp từ các phase):**
- Idempotency: Load chạy 4 lần không trùng; `customer_segment` UPSERT 2 lần không trùng; OLAP
  view + Superset provisioning chạy lại không tạo object trùng.
- Đối soát: `fact_sales` (1.003.670/19.659.453,86) không đổi qua toàn bộ các bước Phase 3-6;
  RFM-từ-DW khớp tuyệt đối baseline; Silhouette K=4 khớp tuyệt đối; dashboard chart khớp OLAP.
- Trường hợp biên: thiếu Customer ID (226.813 dòng giữ đúng `NULL`, loại đúng khỏi RFM), đơn
  hủy (0 dòng lọt vào `fact_sales`, kiểm tra bằng `invoice_no LIKE 'C%'`), dữ liệu không hợp lệ
  (0 dòng `Quantity/Price≤0` trong `fact_sales`).

**Hạn chế đã biết (nêu trung thực, không che giấu):**
1. 3 nguồn dữ liệu là mô phỏng, không độc lập thật.
2. Không có khóa chung giữa 3 nguồn raw để join lại — Transform đọc trực tiếp dataset gốc.
3. K=4 không phải Silhouette tối ưu tuyệt đối — đánh đổi để lấy khả năng diễn giải.
4. `segment_name` là heuristic xếp hạng tương đối giữa các cụm, không phải chuẩn ngành.
5. Dashboard Superset chưa xác nhận bằng mắt (môi trường không có browser).
6. Phase 8 (Apriori/Decision Tree) chưa triển khai — là mở rộng tùy chọn theo kế hoạch gốc,
   không bắt buộc cho MVP.

**Hướng phát triển tiếp theo (nếu còn thời gian):**
- Thêm chart Dice/Pivot (`v_olap_dice_country_year`, `v_olap_pivot_country_by_year`) lên
  Superset để minh họa trực tiếp khái niệm OLAP.
- Apriori (luật kết hợp sản phẩm thường mua cùng nhau) hoặc Decision Tree (dự đoán khách có
  nguy cơ rời bỏ) — Phase 8 mở rộng.
- Xác nhận chính thức bằng văn bản của nhóm cho các quyết định nghiệp vụ (hiện đã áp dụng
  nhất quán trong code nhưng cần nhóm ký xác nhận theo Phase 0.3).

## 8. Kịch bản demo (thứ tự thao tác khi bảo vệ)

1. **Giới thiệu kiến trúc** (1 phút): mở README.md mục A, giải thích 3 nguồn mô phỏng + luồng
   dữ liệu.
2. **Chạy hạ tầng** (nếu demo từ đầu): `docker compose up -d --build`, `docker compose ps` —
   cho thấy 7 container `healthy`.
3. **Trigger DAG Airflow**: mở Airflow UI (`localhost:8081`) → `retail_online_ii_etl` → Trigger
   → cho xem Grid view chạy qua 4 bước (extract/transform/load/validate) → SUCCESS.
4. **Xem Data Warehouse**: `docker exec khodl-postgres-dw psql ... -c "SELECT count(*), sum(revenue) FROM fact_sales;"`
   → 1.003.670 / 19.659.453,86.
5. **Xem OLAP**: `SELECT * FROM v_olap_sales_yearly;` (roll-up năm) và
   `SELECT * FROM v_olap_pivot_country_by_year LIMIT 5;` (pivot).
6. **Xem Data Mining**: mở `data/processed/customer_segments_from_dw.csv` hoặc
   `SELECT segment_name, count(*) FROM customer_segment GROUP BY segment_name;` → 4 phân khúc.
7. **Mở Superset Dashboard**: `localhost:8088` → Dashboards → "Online Retail II - Tong quan
   kinh doanh" → trình bày 6 chart, thử đổi bộ lọc Quốc gia/Khoảng thời gian.
8. **Kết luận**: tóm tắt hạn chế (mục 7) và hướng phát triển.

## 9. Câu hỏi giảng viên có thể hỏi — trả lời ngắn

| Câu hỏi | Trả lời ngắn |
|---|---|
| 3 nguồn dữ liệu có thật độc lập không? | Không — mô phỏng từ 1 dataset Online Retail II, tách theo chiều dữ liệu, đã nêu rõ trong README/báo cáo. |
| Vì sao không join lại 3 nguồn raw? | File source thiếu Customer ID/Country, Database source đã khử trùng theo khách hàng (mất liên kết Invoice) — không có khóa chung ở mức dòng giao dịch. |
| Vì sao giữ dòng thiếu Customer ID trong `fact_sales`? | Để không mất doanh thu thật (22,77% dữ liệu) khi tính KPI tổng; loại khỏi RFM vì không gán được giao dịch cho khách hàng cụ thể. |
| Grain của `fact_sales` là gì? | 1 dòng = 1 dòng hàng trong 1 hóa đơn (không phải hóa đơn tổng hợp) — vì có 42.638 cặp (Invoice, StockCode) lặp >1 dòng trong dataset gốc. |
| Vì sao chọn K=4 chứ không phải K=2 (Silhouette cao hơn)? | K=2 chỉ tách "hoạt động" vs "không hoạt động", ít ý nghĩa kinh doanh. K=4 cho 4 phân khúc diễn giải được (VIP/trung thành/mới/nguy cơ rời bỏ), đánh đổi hợp lý trong RFM segmentation. |
| RFM tính từ DW có khác baseline không? | Không — đã đối chiếu thật, khớp tuyệt đối (0 khách hàng lệch), vì cùng áp dụng đúng quy tắc nghiệp vụ trên 2 nguồn dữ liệu khác nhau. |
| Revenue trong `fact_sales` có tin được không? | Có — `revenue` là cột `GENERATED ALWAYS AS (ROUND(quantity*unit_price,2)) STORED`, PostgreSQL tự tính, không thể lưu giá trị lệch công thức. |
| Chạy lại pipeline có bị trùng dữ liệu không? | Không — Load dùng UPSERT (dimension) + TRUNCATE/INSERT toàn bộ (fact), đã test chạy 4 lần vẫn đúng 1.003.670 dòng; `customer_segment` UPSERT theo `customer_key`, test chạy 2 lần không trùng. |
| 5.942 (Database source) vs 5.853 (DW) khác nhau ở đâu? | 5.942 là Customer ID duy nhất trong dataset gốc (chưa áp quy tắc làm sạch). 5.853 là sau khi áp các quy tắc (bỏ trùng, đơn hủy, Qty/Price≤0, mã phi SP) — 89 khách hàng có toàn bộ giao dịch bị loại bởi các quy tắc này. |
| Dashboard Superset có dùng số liệu thật không? | Có — kết nối trực tiếp `postgres-dw`, đã kiểm thử từng chart bằng API, số liệu khớp truy vấn SQL trực tiếp, không có số liệu minh họa. |

## 10. Bảng phân công thành viên

> **Giới hạn cần nêu rõ:** repo chỉ có lịch sử Git ngắn, không có thông tin xác thực về tên
> thật và việc phân chia công việc giữa các thành viên. Bảng dưới đây dùng **đúng nhãn vai trò
> (TV1/TV2/TV3) đã định nghĩa sẵn trong `PROJECT_CHECKLIST.md`** (cột "Phụ trách" của từng
> task) — không tự suy đoán hoặc bịa thêm việc cho thành viên nào. **Cần điền tên thật của
> từng người vào cột "Thành viên" trước khi nộp.**

| Vai trò (theo checklist) | Thành viên (điền tên thật) | Phạm vi công việc chính (theo `PROJECT_CHECKLIST.md`) |
|---|---|---|
| TV1 | _____________ | Hạ tầng Docker Compose, API mock, 3 nguồn ingest (Phase 1), Extract trong Airflow (Phase 2.2) |
| TV2 | _____________ | Thiết kế DAG (Phase 2.1), Star Schema + DDL (Phase 3), OLAP (Phase 4), Superset Dashboard (Phase 6) |
| TV3 | _____________ | Khảo sát/làm sạch dữ liệu (Phase 0), RFM + K-Means + nạp cluster vào DW (Phase 5) |

## 11. Checklist cuối cùng trước khi nộp

- [x] `docker compose up -d --build` chạy được, 7 container `healthy` (đã test).
- [x] Trigger DAG Airflow chạy SUCCESS đầu-cuối (đã test).
- [x] Chạy lại DAG/Load/Superset provisioning lần 2 không làm trùng dữ liệu (đã test).
- [x] Tổng Revenue trong DW khớp tính tay (19.659.453,86, đã đối chiếu nhiều lần).
- [x] Số lượng khách hàng mỗi cụm + mô tả cụm có trong `customer_segment` (đã test qua SQL).
- [x] Chart Superset load dữ liệu thật qua API, khớp OLAP (đã test qua `verify_charts.py`).
- [ ] **Mở Superset UI bằng trình duyệt thật, xác nhận dashboard hiển thị đúng, chụp ảnh minh
      chứng** — chưa làm được từ môi trường này (không có browser), cần bạn tự làm.
- [ ] Điền tên thật vào bảng phân công (mục 10).
- [ ] 3 thành viên xác nhận bằng văn bản các quyết định nghiệp vụ Phase 0.3 (dự thảo đã có
      sẵn trong `PROJECT_CHECKLIST.md` mục 0.3).
- [ ] Đối chiếu `BaoCaoChuDe.pdf` (đề bài gốc) với báo cáo này nếu cần kiểm tra rubric điểm số
      chi tiết — chưa đối chiếu trong các lượt làm việc trước (để tiết kiệm token), và file này
      hiện bị xóa khỏi đĩa (`git status` báo `D BaoCaoChuDe.pdf`) — chưa khôi phục theo yêu cầu
      an toàn dữ liệu của các lượt trước.
- [ ] Review README.md một lượt cuối cùng với người ngoài nhóm (nếu có) để xác nhận chạy được
      từ máy sạch.
