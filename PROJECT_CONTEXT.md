# PROJECT CONTEXT — Tài liệu bàn giao

> Mục đích: cho phép mở một phiên Claude Code mới và tiếp tục làm việc ngay, không mất ngữ
> cảnh. Tài liệu này được tạo/cập nhật bằng cách đọc trực tiếp code, chạy lại script, kiểm tra
> container Docker thật — không suy đoán từ tên file.
>
> Cập nhật lần cuối: **2026-10-23** (sau khi hoàn thành Phase 1 và Phase 3.1/3.2). Nguồn đối
> chiếu chi tiết hơn:
> [`PROJECT_CHECKLIST.md`](PROJECT_CHECKLIST.md) (kế hoạch gốc + trạng thái từng task) và
> [`docs/PROJECT_GAP_ANALYSIS.md`](docs/PROJECT_GAP_ANALYSIS.md) (lịch sử đối chiếu qua các lượt).

---

## 1. Thông tin dự án

**Đề tài:** Phân tích hành vi mua sắm và phân nhóm khách hàng trong cửa hàng bán lẻ.
Đồ án môn Kho dữ liệu và Khai phá dữ liệu — PTIT — nhóm 3 thành viên (TV1/TV2/TV3 trong
checklist; thực tế công việc trong các phiên vừa qua được thực hiện/hỗ trợ bởi 1 người qua
Claude Code).

**Mục tiêu:** Hệ thống end-to-end: tích hợp dữ liệu đa nguồn (mô phỏng) → Data Lake → ETL
(Airflow) → Data Warehouse (PostgreSQL, Star Schema) → OLAP → Data Mining (RFM + K-Means) →
Dashboard (Superset).

**Dataset:** Kaggle *Online Retail II* — `data/raw/online_retail_II.xlsx` (đã có trên máy, 2
sheet 2009-2010/2010-2011, 1,067,371 dòng, 8 cột: Invoice, StockCode, Description, Quantity,
InvoiceDate, Price, Customer ID, Country). File gốc **không commit** (`.gitignore`), tải lại từ
Kaggle theo `data/README.md`.

**Giả định quan trọng (đã thống nhất, không hỏi lại):**
- 3 "nguồn dữ liệu" (API/File/Database) đều mô phỏng từ cùng 1 dataset, tách theo chiều dữ
  liệu — **không phải 3 nguồn độc lập ngoài đời thực**. Phải nêu rõ trong báo cáo cuối.
- Không tự sinh thuộc tính không có trong dataset (tên, tuổi, giới tính...).
- Database nguồn dùng PostgreSQL (không MySQL) để giảm số công nghệ.
- Không dùng Kafka/Debezium/Spark — dataset đủ nhỏ để xử lý batch bằng Pandas/Airflow/Postgres.
- Apriori/Decision Tree (Phase 8) chỉ làm sau khi Phase 0–6 xong và còn thời gian.

**Kiến trúc luồng dữ liệu:** xem sơ đồ đầy đủ ở `PROJECT_CHECKLIST.md` mục 1. Tóm tắt:
`Kaggle CSV → (mô phỏng 3 nguồn: API/File/DB) → Data Lake (raw/processed/metadata) → Airflow
ETL → PostgreSQL DW (Star Schema) → OLAP + Data Mining (RFM/K-Means) → Superset Dashboard`.

**Công nghệ:** Python/Pandas/Scikit-learn, FastAPI, PostgreSQL (2 instance nghiệp vụ + 1
metadata Airflow), Apache Airflow (LocalExecutor), Apache Superset, Docker Compose. Môi
trường Python quản lý bằng `uv` (xem `pyproject.toml`, `uv.lock`, `.venv/`).

---

## 2. Trạng thái hiện tại theo Phase

| Phase | Trạng thái | Ghi chú ngắn |
|---|---|---|
| 0.1 Khảo sát dataset | **DONE** | Chạy lại script, khớp 100% báo cáo |
| 0.2 Môi trường (Docker Compose) | **IN PROGRESS** | Hạ tầng dựng xong + healthy trên 1 máy; thiếu xác nhận 2 thành viên còn lại |
| 0.3 Quy tắc làm sạch dữ liệu | **IN PROGRESS** | Nội dung kỹ thuật xong + đã chạy thật; thiếu xác nhận nhóm bằng văn bản |
| 1. Data Lake & 3 nguồn ingest | **DONE** | `/products` FastAPI, `source_customers` trong Postgres nguồn, `datalake/` + ingest log — tất cả kiểm thử thật |
| 2.1 Thiết kế DAG | **DONE** (`[~]` chờ review TV2) | `airflow/dags/retail_etl_dag.py` — kiểm thử cấu trúc/parse/no-backfill thật |
| 2.2 Extract thật trong container | **DONE** | Cả 3 task SUCCESS thật (test riêng + DAG run đầy đủ) |
| 2.3 Transform thật | **DONE** | `scripts/transform/transform.py` — 1,003,670 dòng, 5 quy tắc áp dụng đúng thứ tự, có log số dòng loại từng bước |
| 2.4 Load thật | **DONE** | `scripts/load/load.py` — UPSERT dimension + TRUNCATE/INSERT `fact_sales`, `count(*)` khớp |
| 2.5 Idempotency | **DONE** | Chạy Load 4 lần (host×2 + Airflow×2), `fact_sales` luôn 1,003,670 dòng, không trùng |
| 3.1/3.2 Star Schema + DDL | **DONE** | 5 bảng đã tạo trên `postgres-dw`, kiểm thử PK/FK/CHECK/idempotency thật |
| 3.3 Xác thực sau nạp | **DONE** | 0 orphan FK, 0 đơn hủy trong fact, revenue khớp 100% tính độc lập, 226,813 dòng `customer_key=NULL` đúng thiết kế |
| 4. OLAP (4.1-4.4) | **DONE đầy đủ** | 13 view SQL (Roll-up Ngày/Tháng/Quý/Năm, Slice, Dice, Pivot, KPI), kiểm thử khớp tuyệt đối với `fact_sales`, idempotent — xem mục 3.3.g |
| 5.1 Tính RFM | **DONE** (từ DW, 2026-10-24) | `scripts/mining/rfm_dw.py` đọc trực tiếp `fact_sales JOIN dim_customer`, khớp tuyệt đối baseline CSV — xem mục 3.2b |
| 5.2–5.4 Chuẩn hóa/K-Means/diễn giải cụm | **DONE** | Có kết quả + log thật (baseline CSV và DW), xem mục 3.2/3.2b |
| 5.5 Nạp cluster vào DW | **DONE** (2026-10-24) | Bảng `customer_segment` (`sql/003_customer_segment.sql`) + UPSERT (`scripts/mining/load_segments_dw.py`), 5.853 dòng, 0 orphan FK, chạy lại idempotent — xem mục 3.2b |
| 6. Dashboard Superset | **DONE qua API (2026-10-24)** | Database+5 dataset+6 chart+1 dashboard tạo qua REST API, đối chiếu khớp DW; **chưa xác nhận bằng mắt trong browser** (môi trường không có browser) — xem `docs/06_superset_dashboard.md` |
| 7. Kiểm thử tổng/báo cáo/demo | **Phần lớn xong** | `docs/00_bao_cao_do_an.md` đã tổng hợp báo cáo+demo+Q&A+checklist; còn thiếu: xác nhận browser, tên thật thành viên, chữ ký nhóm 0.3 |
| 8.1 Apriori (Market Basket Analysis) | **DONE đầy đủ, kiểm thử thật 2026-10-28** (thuật toán + CSV + tích hợp DW + dashboard Superset riêng) | Xem mục 3.4 |
| 8.2 Decision Tree | **TODO** (chưa làm) | Chỉ làm nếu còn thời gian |

---

## 3. Những gì đã hoàn thành (bằng chứng kiểm chứng thực tế)

### 3.1 Khảo sát & làm sạch dữ liệu (Phase 0.1, 0.3 nội dung kỹ thuật)
- File: [`scripts/01_explore_data.py`](scripts/01_explore_data.py),
  [`scripts/02_clean_data.py`](scripts/02_clean_data.py),
  [`docs/01_bao_cao_tien_do.md`](docs/01_bao_cao_tien_do.md).
- Kết quả đã chạy lại và xác minh (2026-10-23): 1,067,371 dòng gốc → bỏ trùng lặp (-34,335) →
  bỏ thiếu Customer ID (-235,151, tổng thiếu 243,007/22.77%) → bỏ đơn hủy (-18,390) → bỏ
  Quantity/Price ≤ 0 (-70) → bỏ mã phí/điều chỉnh (-2,568) → **776,857 dòng sạch (72.8%),
  5,853 khách hàng duy nhất**.
- Output: `data/processed/transactions_clean.csv` (75.5MB, không commit).

### 3.2 RFM + K-Means (Phase 5.1–5.4)
- File: [`scripts/03_rfm.py`](scripts/03_rfm.py), [`scripts/04_kmeans.py`](scripts/04_kmeans.py).
- Đã chạy lại và xác minh (2026-10-23): RFM cho 5,853 khách hàng (Recency mean 200.2 ngày,
  Frequency mean 6.26, Monetary mean £2,918.5). Silhouette Score K=2..10 (K=2: 0.4182 cao
  nhất, K=4: 0.3611 — **chọn K=4 vì lý do nghiệp vụ**: 4 cụm có ý nghĩa phân khúc rõ — VIP (947
  KH), trung thành (1,875), mới/ít tương tác (1,397), nguy cơ rời bỏ (1,634).
- Output: `data/processed/rfm.csv`, `data/processed/customer_segments.csv`,
  `docs/silhouette_scores.png`.
- **Lưu ý:** pipeline này đọc trực tiếp từ `transactions_clean.csv`, **không phải từ Data
  Warehouse**. Đây là baseline/prototype, **giữ nguyên không sửa** — dùng để đối chiếu số liệu
  với pipeline chính thức đọc từ DW (mục 3.2b). `scripts/03_rfm.py`/`04_kmeans.py` KHÔNG bị sửa.

### 3.2b RFM + K-Means từ DW, nạp cluster vào DW (Phase 5.1, 5.5 chính thức — DONE 2026-10-24)
- File mới (không sửa baseline): [`scripts/mining/rfm_dw.py`](scripts/mining/rfm_dw.py),
  [`scripts/mining/cluster_dw.py`](scripts/mining/cluster_dw.py),
  [`sql/003_customer_segment.sql`](sql/003_customer_segment.sql),
  [`scripts/mining/load_segments_dw.py`](scripts/mining/load_segments_dw.py).
  Tài liệu đầy đủ: [`docs/05_rfm_kmeans_dw.md`](docs/05_rfm_kmeans_dw.md).
- RFM đọc trực tiếp `fact_sales JOIN dim_customer` (SQL, lọc `customer_key IS NOT NULL`) →
  5,853 khách hàng — **đối chiếu baseline: khớp tuyệt đối** (0 khách hàng lệch, lệch Monetary
  tối đa 0.002 do dấu phẩy động).
- K-Means tính lại từ đầu trên dữ liệu DW (không giả định K=4): best_k=2 (silhouette 0.4182),
  business_k=4 (silhouette 0.3611) — **giống tuyệt đối** giá trị baseline đã báo cáo trước đó,
  vì input giống tuyệt đối và `random_state`/`n_init` cố định.
- Bảng `customer_segment` (riêng, KHÔNG `ALTER dim_customer`) đã tạo trên `postgres-dw` và nạp
  bằng UPSERT: 5,853 dòng, 0 orphan FK, tổng 4 cụm (947+1,875+1,397+1,634) = 5,853, chạy lại
  UPSERT lần 2 vẫn 5,853 dòng (không trùng lặp), DDL chạy lại idempotent (NOTICE skip).
  `fact_sales` xác nhận không đổi: 1,003,670 dòng, revenue 19,659,453.86.
- Superset chưa kết nối thật (Phase 6.1 chưa làm) — nhưng truy vấn mẫu `GROUP BY segment_name`
  đã kiểm thử chạy đúng qua `psql` (xem `docs/05_rfm_kmeans_dw.md` mục 4).

### 3.3.b Phase 1 — Data Lake & 3 nguồn ingest (DONE, kiểm thử thật 2026-10-23)
- File: [`api/app/data.py`](api/app/data.py) (mới), [`api/app/main.py`](api/app/main.py) (sửa),
  [`scripts/ingest/`](scripts/ingest/) (`common.py`, `ingest_api.py`, `ingest_file.py`,
  `ingest_database.py`, `run_all.py` — tất cả mới).
- **1.1 API:** `GET /products` trả 5,304 StockCode duy nhất (đối chiếu độc lập khớp 100%
  ngoài API), `GET /products/{code}` trả đúng sản phẩm hoặc 404. Cache trong bộ nhớ, chỉ đọc
  Excel 1 lần (lần gọi đầu, ~60–90s do openpyxl parse file lớn; các lần sau tức thì).
- **1.2 File:** trích 5 cột giao dịch, giữ nguyên 1,067,371 dòng (khớp khảo sát 0.1), **không**
  áp dụng quy tắc làm sạch 0.3 (chưa được nhóm ký xác nhận).
- **1.3 Database:** bảng `source_customers` trong `postgres-source`, 5,942 khách hàng (khớp
  khảo sát 0.1), nạp idempotent (`TRUNCATE`+`INSERT`) — đã chạy 3 lần, verify bằng `psql` trực
  tiếp vẫn 5,942, không trùng.
- **1.4 Data Lake:** `datalake/{raw/api,raw/file,raw/database,processed,metadata}/` — có file
  mẫu thật, lưu trên host (không mất khi container restart).
- **1.5 Ingest log:** `datalake/metadata/ingest_log.csv` (append-only) — có cả dòng `success`
  và `failed` thật (đã test chủ động 2 kịch bản lỗi: thiếu file, FastAPI down).
- Dependency mới: `psycopg2-binary` (qua `uv add`, cần thiết để Python kết nối Postgres).
- Chi tiết đầy đủ: `docs/PROJECT_GAP_ANALYSIS.md` mục 8.

### 3.3 Hạ tầng Docker Compose (Phase 0.2)
- File: [`docker-compose.yml`](docker-compose.yml), `.env` (không commit, chứa secret thật),
  [`.env.example`](.env.example) (placeholder), [`api/`](api/) (FastAPI skeleton),
  [`superset/superset_config.py`](superset/superset_config.py),
  `airflow/{dags,logs,plugins}/` (rỗng, có `.gitkeep`).
- 7 container, đã kiểm thử healthy bằng lệnh thật (2026-10-23):
  - `postgres-source` (db `retail_source`, host port 5432) — `psql` OK.
  - `postgres-dw` (db `retail_dw`, host port 5433) — `psql` OK.
  - `postgres-airflow` (metadata Airflow, không expose host) — `psql` qua `docker exec` OK.
  - `airflow-webserver` (host port **8081** — đổi từ 8080 vì máy có service khác chiếm port
    đó) + `airflow-scheduler` — `curl :8081/health` → `metadatabase: healthy`, `scheduler:
    healthy`; `airflow users list` thấy `admin`.
  - `fastapi` (host port 8000) — `curl :8000/health` → `{"status":"healthy"}`. **Chỉ có
    `/` và `/health`, chưa có `/products`** (đó là nội dung Phase 1.1, chưa làm).
  - `superset` (host port 8088) — `curl :8088/health` → `OK`, `/login/` → HTTP 200.
- RAM đo thật qua `docker stats`: tổng ~2.2GB / 7.674GB cấp cho Docker Desktop — còn dư nhiều.
- **Chưa đạt tiêu chí nghiệm thu gốc:** checklist yêu cầu cả 3 thành viên tự `docker compose
  up` trên máy họ + chụp màn hình UI. Chỉ xác nhận được trên 1 máy → trạng thái `[~]`, không
  phải `[x]`.

### 3.3.c Phase 3.1 + 3.2 — Star Schema & DDL (DONE, kiểm thử thật 2026-10-23)
- File: [`docs/03_star_schema.md`](docs/03_star_schema.md) (thiết kế + ERD Mermaid + lý do +
  quyết định mở), [`sql/001_create_star_schema.sql`](sql/001_create_star_schema.sql) (DDL).
- Thiết kế dựa trên khảo sát lại thực tế dataset: `Quantity` không bao giờ = 0, `Price` âm chỉ
  5/1,067,371 dòng, có 42,638 cặp `(Invoice, StockCode)` xuất hiện >1 dòng (→ cần surrogate key
  riêng cho `fact_sales`, không dùng `(Invoice, StockCode)` làm khóa).
- 5 bảng: `dim_date`, `dim_customer`, `dim_product`, `dim_country` (PK `BIGINT GENERATED
  ALWAYS AS IDENTITY`), `fact_sales` (4 FK, `customer_key` **nullable**, `quantity`/
  `unit_price` kiểu `NUMERIC`, `revenue` là cột **`GENERATED ALWAYS AS (ROUND(quantity *
  unit_price, 2)) STORED`** — Postgres tự tính, không thể lệch công thức).
- **Đã chạy DDL thật trên `khodl-postgres-dw`/`retail_dw`:** lần 1 không lỗi, `\dt` đủ 5 bảng,
  `\d+`/`\d` xác nhận đúng PK/FK/CHECK/kiểu dữ liệu; **chạy lại lần 2** chỉ `NOTICE: already
  exists, skipping`, vẫn 5 bảng — an toàn khi chạy lại; **test CHECK/FK/NULL thật trong 1
  transaction rồi `ROLLBACK`** (revenue tự tính đúng, `quantity=0` bị chặn, FK sai bị chặn,
  `customer_key=NULL` được chấp nhận) — sau `ROLLBACK`, DW xác nhận vẫn **0 dòng**.
- **5 quyết định thiết kế còn mở, cần nhóm xác nhận** trước khi code Load (Phase 2.4): (1)
  `Country` theo dòng giao dịch hay theo khách hàng, (2) dòng thiếu Customer ID giữ `NULL` hay
  loại khỏi `fact_sales`, (3) đơn hủy giữ revenue âm hay loại bỏ, (4) có cần `dim_time` mức giờ,
  (5) thuật toán chọn `description` đại diện cho `dim_product`. Chi tiết: `docs/03_star_schema.md` mục 6.
- Chi tiết đầy đủ: `docs/PROJECT_GAP_ANALYSIS.md` mục 9.

### 3.3.d Phase 2.1 — Thiết kế DAG Airflow (DONE, kiểm thử thật 2026-10-23)
- File: [`airflow/dags/retail_etl_dag.py`](airflow/dags/retail_etl_dag.py) (mới), sửa
  `docker-compose.yml` (thêm mount `./scripts:/opt/airflow/scripts:ro` +
  biến `PG_DW_DB/USER/PASSWORD` vào `x-airflow-common` để DAG dùng được).
- DAG `retail_online_ii_etl`: `start → extract{extract_api, extract_file,
  extract_database} (song song) → transform → load → validate → end`.
  `schedule=None`, `catchup=False`, mặc định `paused`.
- **Đã kiểm thử thật trên `khodl-airflow-webserver`:** DAG parse không lỗi
  (`dags list-import-errors` → "No data found"); task/dependency đúng (`tasks list --tree`);
  không tự backfill (`dags details` → `schedule_interval: None`); `transform`/`load` →
  `airflow tasks test` xác nhận `SKIPPED`, không ghi dữ liệu; `validate` → kết nối **thật**
  tới `postgres-dw` (dùng tên service, không phải `localhost`), đếm `fact_sales` = 0 (đúng kỳ
  vọng), rồi skip — không báo thành công giả.
- **Extract — THẤT BẠI THẬT khi test ở Phase 2.1** (không phải lỗi thiết kế DAG, mà là giới
  hạn môi trường của `scripts/ingest/*.py` từ Phase 1, vốn viết cho máy host): `extract_api`
  `Connection refused` tới `localhost:8000`; `extract_file`/`extract_database` không tìm thấy
  dataset. **Đã sửa xong ở Phase 2.2** (xem mục 3.3.e) — không còn là vấn đề mở.
- Đã xác nhận: `daugiahtpt-*`/port 8080 không bị ảnh hưởng; `datalake/metadata/ingest_log.csv`
  thật trên host không bị ghi thêm dòng nào từ test trong container (ở thời điểm Phase 2.1).
- Chi tiết đầy đủ: `docs/PROJECT_GAP_ANALYSIS.md` mục 10.

### 3.3.e Phase 2.2 — Extract chạy thật trong Airflow (DONE, kiểm thử thật 2026-10-23)
- Sửa [`scripts/ingest/ingest_api.py`](scripts/ingest/ingest_api.py) và
  [`ingest_database.py`](scripts/ingest/ingest_database.py): hostname đọc từ
  `FASTAPI_HOST`/`PG_SOURCE_HOST` (biến môi trường), **mặc định vẫn `"localhost"`** — hành vi
  trên host (Phase 1) không đổi.
- Sửa `docker-compose.yml` (`x-airflow-common`): đặt `FASTAPI_HOST=fastapi`,
  `PG_SOURCE_HOST=postgres-source`; thêm mount **mirror cấu trúc thư mục gốc repo** dưới
  `/opt/airflow/` — `./data:/opt/airflow/data:ro`, `./datalake:/opt/airflow/datalake` (đọc/
  ghi), `./.env:/opt/airflow/.env:ro` — nhờ vậy `scripts/ingest/common.py` (tính `ROOT_DIR` từ
  `__file__`) tự tính đúng đường dẫn `.env`/`data/`/`datalake/` **mà không cần sửa code tính
  đường dẫn nào**. Thêm `_PIP_ADDITIONAL_REQUIREMENTS: "openpyxl==3.1.5"` (image Airflow gốc
  thiếu `openpyxl`; `pandas`/`psycopg2` đã có sẵn).
- **Đã kiểm thử thật, cả 3 Extract task SUCCESS:**
  - Kết nối mạng thật: `http://fastapi:8000/health` và TCP tới `postgres-source:5432` từ
    trong container Airflow — cả hai OK.
  - `airflow tasks test ... extract.extract_api` → SUCCESS, **5,304** sản phẩm.
  - `airflow tasks test ... extract.extract_file` → SUCCESS, **1,067,371** dòng (khớp 0.1).
  - `airflow tasks test ... extract.extract_database` → SUCCESS, **5,942** khách hàng; verify
    lại bằng `psql` trực tiếp từ host: `count=5942`, không trùng.
  - **DAG run đầy đủ qua scheduler** (`dags trigger` + tạm `unpause`, `pause` lại sau khi
    xong): `start` → 3 `extract.*` SUCCESS → `transform`/`load`/`validate` SKIPPED đúng thiết
    kế → `end` SUCCESS. Không task nào FAILED.
  - Output thật xuất hiện trên **host** (`datalake/raw/{api,file,database}/2026-10-23/...`),
    không chỉ trong container — xác nhận bằng `find -newer` và đọc trực tiếp file trên host.
  - `data/raw/online_retail_II.xlsx` không đổi (kích thước + mtime gốc giữ nguyên, mount
    `:ro`). `daugiahtpt-*`/port 8080 không bị ảnh hưởng.
- Chi tiết đầy đủ: `docs/PROJECT_GAP_ANALYSIS.md` mục 11.

### 3.3.f Phase 2.3+2.4+2.5+3.3 — Cụm ETL hoàn chỉnh (DONE, kiểm thử thật 2026-10-23)
- File mới: [`scripts/transform/transform.py`](scripts/transform/transform.py),
  [`scripts/load/load.py`](scripts/load/load.py). Sửa `airflow/dags/retail_etl_dag.py`
  (`transform`/`load` gọi module thật thay vì SKIP; `validate` nâng cấp thành kiểm tra Phase
  3.3 đầy đủ, FAIL thật nếu có kiểm tra không đạt). Sửa `docker-compose.yml` (thêm
  `PG_DW_HOST`, `PG_DW_PORT_INTERNAL`).
- **Transform** đọc trực tiếp `data/raw/online_retail_II.xlsx` (xem lý do kiến trúc ở mục 5 —
  2 nguồn mô phỏng File/Database không có khóa chung để join lại), xác thực lineage với
  `datalake/raw/*`, áp dụng 5 quy tắc theo thứ tự:
  1,067,371 → dedup (**-34,335**) → đơn hủy (**-19,104**) → Quantity/Price≤0 (**-6,019**) →
  mã phi sản phẩm (**-4,243**) → **giữ** 226,813 dòng thiếu Customer ID → **1,003,670 dòng**.
  Dimension: `dim_date=604`, `dim_customer=5,853`, `dim_product=4,905`, `dim_country=43`.
- **Load**: UPSERT 4 dimension (không xóa dòng cũ) + TRUNCATE/INSERT `fact_sales` (chỉ bảng
  này). Phát hiện + sửa 1 lỗi thật: `PG_DW_PORT` (.env, =5433) là port publish ra host, khi nối
  qua tên service `postgres-dw` phải dùng port container thật (5432) → thêm
  `PG_DW_PORT_INTERNAL`.
- **Idempotency**: chạy Load 4 lần (host×2, Airflow `tasks test`×1, DAG run đầy đủ×1) —
  `fact_sales` luôn đúng 1,003,670 dòng, dimension không tăng.
- **Validate (Phase 3.3)**: 0 orphan FK (date/product/country/customer), 0 đơn hủy sót trong
  fact, 0 dòng sai công thức revenue, tổng revenue DW (19,659,453.86) khớp **tuyệt đối** với
  tính độc lập bằng pandas từ CSV (không qua Load).
- **DAG run đầy đủ qua scheduler** (`dags trigger`, tạm `unpause` rồi `pause` lại): `start` →
  3 `extract.*` → `transform` → `load` → `validate` → `end`, **tất cả SUCCESS**, không task
  nào FAILED/SKIPPED.
- Đã xác nhận: dataset gốc không đổi (size/mtime), DDL (`sql/001_create_star_schema.sql`)
  không bị sửa, `scripts/03_rfm.py`/`04_kmeans.py` không bị sửa, `daugiahtpt-*`/port 8080
  không ảnh hưởng, không `docker compose down -v`.
- Chi tiết đầy đủ: `docs/PROJECT_GAP_ANALYSIS.md` mục 12.

### 3.4 Phase 8.1 — Market Basket Analysis / Apriori (DONE đầy đủ, kiểm thử thật 2026-10-28 — thuật toán+CSV+tích hợp DW+dashboard Superset)
- File mới: [`scripts/mining/apriori_mba.py`](scripts/mining/apriori_mba.py) (thuật toán Apriori
  tự cài đặt bằng Python/Pandas thuần — KHÔNG dùng `mlxtend`/thư viện ngoài, KHÔNG one-hot
  encoding — + CLI độc lập, hỗ trợ `--source dw|csv`),
  [`scripts/mining/test_apriori_mba.py`](scripts/mining/test_apriori_mba.py) (19 unit test
  `unittest` chuẩn), [`sql/004_market_basket_rules.sql`](sql/004_market_basket_rules.sql) +
  [`scripts/mining/load_apriori_dw.py`](scripts/mining/load_apriori_dw.py) (thiết kế tích hợp
  DW, theo đúng pattern `customer_segment`). Tài liệu đầy đủ:
  [`docs/08_apriori_mba.md`](docs/08_apriori_mba.md). Không sửa bất kỳ file nào của Phase 0–7.
- **Đã chạy thật (2026-10-28):** `uv run scripts/mining/apriori_mba.py --source csv` trên
  `datalake/processed/2026-10-23/fact_sales.csv` (1.003.670 dòng, cùng nội dung đã nạp DW) →
  39.536 hóa đơn, 4.905 sản phẩm → 323 frequent itemset (`min_support=0.02`), **101 luật**
  (`min_confidence=0.30`), **101/101 có `lift > 1`**. Top luật hợp lý nghiệp vụ (biến thể màu
  cùng 1 sản phẩm, ví dụ GREEN/PINK/ROSES REGENCY TEACUP AND SAUCER, lift 21–24). Chạy 4,2–4,7s.
  19/19 unit test PASS (kể cả trường hợp rỗng/không có frequent itemset, dữ liệu vi phạm quy
  tắc làm sạch).
- **Cập nhật 2026-10-28 (lượt kiểm thử thứ 2, Docker Desktop hoạt động):** đã khởi động
  `postgres-dw` sạch, nạp Star Schema + `fact_sales` thật (`uv run scripts/load/load.py` —
  khớp lại đúng 1.003.670 dòng/5.853 khách hàng như đã công bố), rồi chạy thật **cả 2 phần
  trước đây CHƯA kiểm thử**:
  - `apriori_mba.py --source dw` → kết quả **giống hệt tuyệt đối** `--source csv` (323 itemset,
    101 luật, cùng top-15 theo lift) — xác nhận SQL JOIN `fact_sales`/`dim_product` đúng.
  - `sql/004_market_basket_rules.sql` + `scripts/mining/load_apriori_dw.py`: DDL chạy lần 1 tạo
    bảng, **chạy lại lần 2 idempotent** (NOTICE skip, không mất dữ liệu); loader UPSERT 101
    dòng, **chạy lại lần 2 vẫn 101 dòng** (không trùng); kiểm tra validity trên DW thật (0 luật
    tự tham chiếu, 0 item rỗng, 0 vi phạm `confidence<support`, 0 giá trị ngoài khoảng hợp lệ,
    0 cặp khóa trùng); trường hợp biên 0 luật (`min_support=0.99`) không làm mất 101 dòng đã có.
  - Hồi quy: `rfm_dw.py`/`cluster_dw.py`/`load_segments_dw.py` chạy lại trên cùng DW cho đúng kết
    quả đã công bố trước đây (947/1.875/1.634/1.397 khách hàng/cụm) — bảng `market_basket_rules`
    mới không ảnh hưởng các thành phần khác.
  - Container `postgres-dw` dùng để kiểm thử đã `docker compose stop` sau khi xong (không có
    container nào khác của dự án đang chạy trước đó nên không ảnh hưởng gì).
  - Chi tiết đầy đủ, bao gồm số liệu cụ thể từng bước: `docs/08_apriori_mba.md` mục 8.
- **Cập nhật 2026-10-28 (lượt kiểm thử thứ 3) — Dashboard Superset riêng cho Apriori:** mở rộng
  [`scripts/superset/provision_superset.py`](scripts/superset/provision_superset.py) (tái dùng
  connection `Retail DW` có sẵn, thêm dataset `market_basket_rules` + 5 chart + dashboard mới
  "Apriori - Luat ket hop san pham", **không sửa/không xóa** dashboard Phase 6) và
  [`scripts/superset/verify_charts.py`](scripts/superset/verify_charts.py) (thêm
  `query_mode=aggregate` + `echarts_timeseries_scatter`). Chạy thật 2 lần: lần 2 idempotent
  (toàn bộ PUT, không tạo trùng — xác nhận `GET` API vẫn đúng 2 dashboard/11 chart/6 dataset).
  Cả 11 chart (6 cũ + 5 mới) xác minh qua `/api/v1/chart/data` khớp số liệu thật; dashboard
  Phase 6 xác nhận không đổi. Chart Scatter (`echarts_timeseries_scatter`, X=`support`,
  Y=AVG(`confidence`)) hoạt động đúng nhờ tính năng "Generic X-Axis" của Superset — đã xác nhận
  key viz này tồn tại thật trong bundle JS của container (không phải tự đặt); **không mã hóa
  `lift` bằng bubble-size** vì Superset 4.1.1 không có viz_type nào hỗ trợ 3 chiều liên tục độc
  lập cùng lúc (đã kiểm tra thực tế). **Chưa xác nhận bằng mắt trong trình duyệt** (môi trường
  không có browser). Chi tiết đầy đủ: `docs/08_apriori_mba.md` mục 11.
- Không thêm dependency (`mlxtend`/`efficient-apriori`/`apyori` đã kiểm tra không có sẵn, quyết
  định tự cài đặt thay vì `uv add` thêm thư viện).

### 3.3.g Phase 4.1–4.4 — OLAP views đầy đủ (DONE, kiểm thử thật 2026-10-24)
- File: [`sql/002_olap_views.sql`](sql/002_olap_views.sql) (**13 view**, lượt đầu 9 + lượt
  này bổ sung 4), [`docs/04_olap.md`](docs/04_olap.md) (thiết kế + kết quả kiểm thử đầy đủ,
  giải thích nghiệp vụ từng phép OLAP).
- **4.1 Roll-up/Drill-down:** `v_olap_sales_{daily,monthly,quarterly,yearly}` — hoàn thiện
  phân cấp **Năm → Quý → Tháng → Ngày** (thêm `v_olap_sales_daily` lượt này).
- **4.2 Slice:** `v_olap_sales_by_country`, `v_olap_product_revenue` + top10,
  `v_olap_customer_revenue` + top10 (loại NULL Customer ID) + `v_olap_sales_missing_customer`
  (mới, tách riêng phần bị loại để không "mất dấu" dữ liệu).
- **4.2 Dice (mới):** `v_olap_dice_country_year` — Quốc gia × Năm.
- **4.3 Pivot (mới):** `v_olap_pivot_country_by_year` — ma trận Quốc gia × Năm
  (2009/2010/2011, 3 giá trị thật từ `dim_date`), dùng `SUM(CASE WHEN...)`.
- **4.4 Tổng hợp:** 1 file `sql/002_olap_views.sql`, phân loại rõ "dashboard core" (bind trực
  tiếp Superset) vs "view minh họa OLAP" (Dice/Pivot).
- **Đã kiểm thử thật, mọi mức đều khớp tuyệt đối với `fact_sales` (1.003.670 dòng,
  19.659.453,86):** Roll-up cả 4 mức (kể cả Ngày mới, đối chiếu Ngày→Tháng cho 01/2010 =
  612.605,50 khớp 2 cách tính); Slice quốc gia/sản phẩm; Slice khách hàng
  (17.082.085,77/776.857 + phần loại NULL 2.577.368,09/226.813 = đúng tổng); Dice khớp đúng
  từng năm với Roll-up Năm; Pivot — 0/43 dòng sai tổng hàng, tổng cột khớp đúng 3 năm + tổng
  toàn kho; JOIN 2 dimension đồng thời (Dice/Pivot) vẫn giữ đúng 1.003.670 dòng (không nhân
  bản). **Chạy lại file SQL lần 2 — idempotent**, không lỗi, không tạo đối tượng trùng.
- **Phát hiện đối chiếu chéo thú vị (giữ từ lượt trước):** số dòng sau khi loại Customer ID
  NULL (776.857) trùng khớp hoàn toàn với số dòng baseline Phase 0.
- Chi tiết đầy đủ: `docs/PROJECT_GAP_ANALYSIS.md` mục 13.

---

## 4. Những gì chưa làm (và lý do/phụ thuộc)

| Việc chưa làm | Vì sao / phụ thuộc gì |
|---|---|
| Xác nhận nhóm cho 0.2, 0.3, 2.1 (review DAG), 3.1 (5 quyết định nghiệp vụ) | Cần hành động thủ công của nhóm (chụp màn hình, đồng ý bằng văn bản, xem xét 5 quyết định) — ngoài khả năng của 1 phiên code. Code đã chạy theo đề xuất, nhóm có thể đổi ý sau khi xem xét |
| Superset kết nối `postgres-dw` + chart (Phase 6) | 13 view OLAP (`sql/002_olap_views.sql`) đã sẵn sàng làm nguồn — **chưa cấu hình Superset UI** |
| OLAP Pivot Thời gian × Sản phẩm | Chỉ làm Pivot Quốc gia × Năm (4.905 sản phẩm quá nhiều cột); nếu cần, nên pivot Top N sản phẩm × Năm/Quý — **chưa làm** |
| Kiểm thử tổng, README, báo cáo, demo (Phase 7) | Phụ thuộc toàn bộ Phase 1–6 |
| Decision Tree (Phase 8.2) | Theo kế hoạch, chỉ làm sau khi còn thời gian — **không phải thiếu sót** |

---

## 5. Quyết định kỹ thuật đã thống nhất (KHÔNG tự ý đổi trong các phiên sau)

- PostgreSQL cho cả nguồn (`postgres-source`) và DW (`postgres-dw`) — không dùng MySQL.
- 1 Postgres thứ 3 riêng cho metadata Airflow (`postgres-airflow`), không dùng SQLite/CeleryDB.
- Airflow dùng **LocalExecutor** (không Celery/Redis) — đủ cho dataset batch nhỏ.
- Superset dùng **SQLite nội bộ** cho metadata của chính nó (không thêm Postgres/Redis thứ 4);
  kết nối Superset → `postgres-dw` chỉ để làm **datasource cho chart**, khai báo ở Phase 6.
- Toàn bộ hạ tầng chạy qua **Docker Compose** (`docker-compose.yml` ở gốc repo), 1 network
  `khodl_net`. Airflow UI publish ở **port 8081** (không phải 8080 mặc định) vì máy đang có
  sẵn 1 project Docker khác (`daugiahtpt-*`) chiếm port 8080 — **không tắt/đụng vào stack đó**.
- Không dùng Kafka/Debezium/Spark/streaming.
- Secret/mật khẩu nằm trong `.env` (gitignored, sinh ngẫu nhiên), `.env.example` chỉ chứa
  placeholder — không hardcode trong file commit.
- K-Means chọn **K=4** cho dashboard (dù K=2 có Silhouette cao hơn) vì lý do nghiệp vụ (4 cụm
  có ý nghĩa phân khúc rõ ràng hơn: VIP / trung thành / mới / rời bỏ).
- Script `03_rfm.py`/`04_kmeans.py` hiện tại là **baseline tạm**, sẽ viết lại nguồn đọc dữ liệu
  (từ DW) khi Phase 3 xong — không xoá/viết lại script cũ, chỉ thêm biến thể đọc từ DW.
- Phase 1.2 (nguồn File) **không áp dụng** bộ quy tắc làm sạch đề xuất ở 0.3 — giữ bản ghi
  trùng/đơn hủy/giá âm nguyên vẹn trong snapshot `datalake/raw/file/`. Quy tắc 0.3 chỉ áp dụng
  ở bước Transform (Phase 2.3), sau khi nhóm xác nhận chính thức.
- **Star Schema (Phase 3.1/3.2) — kiến trúc đã chốt, KHÔNG đổi:** 4 dimension dùng surrogate
  PK `BIGINT GENERATED ALWAYS AS IDENTITY`; `fact_sales.revenue` là cột `GENERATED ALWAYS AS
  (ROUND(quantity * unit_price, 2)) STORED` (Postgres tự tính, không tính ở ETL); `unit_price`/
  `revenue` dùng `NUMERIC`, không `FLOAT`; `fact_sales.country_key` là FK độc lập (lấy từ dòng
  giao dịch, không suy từ `dim_customer`). Chi tiết + lý do: `docs/03_star_schema.md`.
- **5 quyết định nghiệp vụ của Star Schema — ĐÃ TRIỂN KHAI THẬT trong `scripts/transform/
  transform.py` (Phase 2.3, 2026-10-23), CHƯA PHẢI đã được cả 3 thành viên chính thức xác
  nhận** (nhóm xem xét sau vẫn có thể yêu cầu đổi — chỉ cần sửa `transform.py`, không đổi
  schema DW):
  1. Dòng thiếu Customer ID: **giữ** trong `fact_sales` (`customer_key = NULL`), **loại** khỏi
     RFM theo khách hàng.
  2. Đơn hủy (`Invoice` bắt đầu bằng `C`): **loại** khỏi `fact_sales` phục vụ doanh thu/RFM;
     **không xóa** dữ liệu gốc trong Data Lake.
  3. Chiều thời gian: **MVP chỉ `dim_date`**, không tạo `dim_time`.
  4. `Description` đại diện: mô tả không rỗng xuất hiện nhiều nhất; đồng hạng → theo thứ tự
     chữ cái; chuẩn hóa `strip()` + thu gọn khoảng trắng, không ép hoa/thường.
  5. `Country`: lấy theo **dòng giao dịch**, không suy từ khách hàng.
  Chi tiết + căn cứ số liệu: `docs/03_star_schema.md` mục 6. **Nhóm vẫn cần xem xét/nghiệm
  thu** — không tự quyết thay nhóm là quyết định cuối cùng khi code Phase 2.3/2.4 thật.
- `source_customers` trong `postgres-source` nạp theo chiến lược **TRUNCATE + INSERT lại toàn
  bộ** mỗi lần chạy (không phải upsert tăng dần) — đơn giản, idempotent, phù hợp vì bảng này
  phản ánh toàn bộ danh sách khách hàng tĩnh từ 1 dataset, không phải luồng dữ liệu tăng dần.
- `datalake/raw/`, `datalake/processed/` nằm trong `.gitignore` (dữ liệu lớn, tái tạo được
  bằng `scripts/ingest/*.py`); riêng `datalake/metadata/ingest_log.csv` được commit làm bằng
  chứng kiểm thử.
- **Airflow container mount `./scripts:/opt/airflow/scripts:ro`, `./data:/opt/airflow/data:ro`,
  `./datalake:/opt/airflow/datalake` (đọc/ghi), `./.env:/opt/airflow/.env:ro`** — mirror đúng
  cấu trúc thư mục gốc repo dưới `/opt/airflow/` để `scripts/ingest/common.py` tự tính đúng
  đường dẫn (không sửa code tính đường dẫn). Nhận thêm `PG_DW_DB/USER/PASSWORD`,
  `FASTAPI_HOST=fastapi`, `PG_SOURCE_HOST=postgres-source`, `_PIP_ADDITIONAL_REQUIREMENTS:
  "openpyxl==3.1.5"`.
- **`scripts/ingest/ingest_api.py`/`ingest_database.py` đọc hostname từ biến môi trường
  (`FASTAPI_HOST`/`PG_SOURCE_HOST`), mặc định `"localhost"`** — chạy đúng cả trên host (Phase
  1, không đổi hành vi) và trong container Airflow (Phase 2.2, đã verify SUCCESS thật). Không
  tự ý đổi thêm cách tham số hóa này nếu không cần thiết.
- **`scripts/load/load.py` đọc `PG_DW_HOST`/`PG_DW_PORT_INTERNAL`** (mặc định `"localhost"`/
  `PG_DW_PORT`) — tách riêng `PG_DW_PORT_INTERNAL` vì `PG_DW_PORT` (.env=5433) là port publish
  ra HOST, khác với port nội bộ container Postgres (luôn 5432) khi kết nối qua tên service
  `postgres-dw`. Đây là lỗi thật đã gặp và sửa ở Phase 2.4 — nhớ áp dụng cùng pattern nếu thêm
  script nào khác cần nối `postgres-dw` qua tên service trong container.
- **`scripts/transform/transform.py` đọc TRỰC TIẾP `data/raw/online_retail_II.xlsx`**, không
  join lại 3 "nguồn" mô phỏng trong Data Lake — vì nguồn File (1.2) không có Customer ID/
  Country, nguồn Database (1.3) chỉ có Customer ID+Country đã khử trùng (không còn liên kết
  Invoice) → không có khóa chung để join. Đây là hạn chế kiến trúc đã biết của thiết kế
  3-nguồn-mô-phỏng (không phải lỗi code) — Transform vẫn xác thực `raw/file`/`raw/database`/
  `raw/api` tồn tại trước khi tính toán, để giữ ý nghĩa "đọc từ Data Lake" ở mức lineage-check.

---

## 6. Bước tiếp theo nên làm (ưu tiên cao nhất)

> Cập nhật 2026-10-24: **Toàn bộ Phase 4 (4.1–4.4: Roll-up Ngày/Tháng/Quý/Năm, Slice, Dice,
> Pivot) đã xong**, 13 view kiểm thử khớp tuyệt đối với `fact_sales`, idempotent. Xem mục
> 3.3.g. Ưu tiên tiếp theo: **Phase 6 (Superset)**.

**Task: Phase 6 — Kết nối Superset với `postgres-dw` và dựng chart từ các view OLAP.**

**Lý do chọn đây là ưu tiên cao nhất:**
- Superset đã chạy healthy (Phase 0.2) nhưng chưa có datasource nào. 13 view OLAP (đủ cả
  roll-up/slice/dice/pivot/KPI) đã sẵn sàng làm nguồn chart trực tiếp — không cần viết SQL
  mới trong Superset.
- Rủi ro thấp: chỉ cấu hình qua UI Superset (thêm database connection + dataset), không đổi
  schema/dữ liệu.
- Phase 4 không còn việc gì chặn Phase 6 (Pivot/Dice đã xong ở lượt này).

**Đầu vào:**
- `sql/002_olap_views.sql` (13 view đã kiểm thử, xem mục 3.3.g) — ưu tiên dùng nhóm "dashboard
  core" cho chart đơn (`v_olap_sales_yearly`, `v_olap_sales_by_country`,
  `v_olap_top10_products`, `v_olap_kpi_summary`...).
- Thông tin kết nối `postgres-dw` trong `.env` (`PG_DW_*`).

**File cần sửa/tạo:**
- Không cần file mới — cấu hình qua Superset UI (Database connection → Dataset → Chart →
  Dashboard). Có thể cần ghi lại bước cấu hình vào `docs/05_superset.md` nếu muốn tái lập.

**Tiêu chí kiểm thử (phải làm thật trước khi tick `[x]` cho Phase 6):**
- Superset kết nối `postgres-dw` thành công, thấy được danh sách view/bảng.
- Tạo được ít nhất 1 chart từ `v_olap_kpi_summary` hoặc `v_olap_sales_yearly`, số liệu khớp
  với `psql` trực tiếp.
- **Không sửa `scripts/03_rfm.py`/`04_kmeans.py`, không tự triển khai Phase 5.5** trong cùng
  lượt trừ khi được giao rõ.

---

## 7. Lệnh chạy dự án

### Đã kiểm thử thật trong các phiên trước (xác nhận chạy được)
```bash
# Khởi động toàn bộ hạ tầng (hoặc từng service riêng — xem thứ tự khuyến nghị dưới)
docker compose up -d postgres-source postgres-dw postgres-airflow   # chạy trước
docker compose up -d --build fastapi
docker compose up -d airflow-init        # chờ exit code 0 rồi mới chạy dòng dưới
docker compose up -d airflow-webserver airflow-scheduler
docker compose up -d superset

# Kiểm tra trạng thái / log
docker compose ps
docker logs <container_name>

# Kiểm thử từng service
curl http://localhost:8000/health        # FastAPI
curl http://localhost:8081/health        # Airflow (UI: http://localhost:8081, user admin, pass trong .env)
curl http://localhost:8088/health         # Superset (UI: http://localhost:8088, user admin, pass trong .env)
docker exec khodl-postgres-source psql -U source_user -d retail_source -c "SELECT 1;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT 1;"

# Phase 3.2 — tao Star Schema tren postgres-dw (an toan chay lai nhieu lan, da verify 2026-10-23)
docker exec -i khodl-postgres-dw psql -U dw_user -d retail_dw -v ON_ERROR_STOP=1 < sql/001_create_star_schema.sql
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "\dt"          # xac nhan du 5 bang
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "\d+ fact_sales"  # xem PK/FK/CHECK

# Phase 4.1/4.2 - tao view OLAP tren postgres-dw (an toan chay lai, da verify 2026-10-24)
docker exec -i khodl-postgres-dw psql -U dw_user -d retail_dw -v ON_ERROR_STOP=1 < sql/002_olap_views.sql
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_kpi_summary;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_sales_yearly;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_top10_products;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_top10_customers;"

# Phase 4.1 (Drill-down Ngay), 4.2 (Dice), 4.3 (Pivot) - bo sung 2026-10-24, da verify
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_sales_daily ORDER BY full_date LIMIT 10;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_sales_missing_customer;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_dice_country_year ORDER BY total_revenue DESC LIMIT 10;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT * FROM v_olap_pivot_country_by_year LIMIT 10;"

# Phase 2.1 - kiem tra DAG Airflow (da verify 2026-10-23)
docker exec khodl-airflow-webserver airflow dags list-import-errors   # phai "No data found"
docker exec khodl-airflow-webserver airflow dags list                # xem DAG + is_paused
docker exec khodl-airflow-webserver airflow tasks list retail_online_ii_etl --tree
docker exec khodl-airflow-webserver airflow dags details retail_online_ii_etl   # schedule/catchup
# Test tung task rieng (khong can trigger DagRun day du):
docker exec khodl-airflow-webserver airflow tasks test retail_online_ii_etl transform 2024-01-01   # -> SUCCESS (Phase 2.3)
docker exec khodl-airflow-webserver airflow tasks test retail_online_ii_etl load 2024-01-01         # -> SUCCESS (Phase 2.4)
docker exec khodl-airflow-webserver airflow tasks test retail_online_ii_etl validate 2024-01-01     # -> SUCCESS (Phase 3.3, kiem tra thuc)
docker exec khodl-airflow-webserver airflow tasks test retail_online_ii_etl extract.extract_api 2024-01-01  # -> SUCCESS (Phase 2.2)
docker exec khodl-airflow-webserver airflow tasks test retail_online_ii_etl extract.extract_file 2024-01-01     # -> SUCCESS
docker exec khodl-airflow-webserver airflow tasks test retail_online_ii_etl extract.extract_database 2024-01-01 # -> SUCCESS

# Phase 2.2 - chay DAG day du qua scheduler (can tam unpause vi DAG mac dinh paused)
docker exec khodl-airflow-webserver airflow dags unpause retail_online_ii_etl
docker exec khodl-airflow-webserver airflow dags trigger retail_online_ii_etl
docker exec khodl-airflow-webserver airflow tasks states-for-dag-run retail_online_ii_etl "<dag_run_id>"
docker exec khodl-airflow-webserver airflow dags pause retail_online_ii_etl   # pause lai sau khi xong (an toan mac dinh)

# Script phân tích (đã chạy lại và verify output trong phiên 2026-10-23)
.venv/Scripts/python.exe scripts/01_explore_data.py
.venv/Scripts/python.exe scripts/02_clean_data.py
.venv/Scripts/python.exe scripts/03_rfm.py
.venv/Scripts/python.exe scripts/04_kmeans.py

# Phase 1 — ingest 3 nguồn + Data Lake (đã chạy và verify trong phiên 2026-10-23)
# Yêu cầu: postgres-source + fastapi đang healthy (docker compose ps)
.venv/Scripts/python.exe scripts/ingest/run_all.py        # chạy cả 3 nguồn tuần tự (API→File→Database)
.venv/Scripts/python.exe scripts/ingest/ingest_api.py      # chỉ nguồn API
.venv/Scripts/python.exe scripts/ingest/ingest_file.py      # chỉ nguồn File
.venv/Scripts/python.exe scripts/ingest/ingest_database.py  # chỉ nguồn Database (idempotent, chạy lại an toàn)
cat datalake/metadata/ingest_log.csv   # xem log ingest (append-only)

# Phase 2.3/2.4 - Transform + Load tren host (da chay va verify trong phien 2026-10-23)
.venv/Scripts/python.exe scripts/transform/transform.py   # doc data/raw/*.xlsx, xuat datalake/processed/<ngay>/
.venv/Scripts/python.exe scripts/load/load.py              # nap vao postgres-dw (idempotent, chay lai an toan)

# Phase 3.3 - kiem tra thuc tren DW (da chay va verify)
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT count(*) FROM fact_sales;"
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT count(*) FROM fact_sales WHERE invoice_no LIKE 'C%';"  # phai = 0
docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -c "SELECT count(*) FROM fact_sales WHERE customer_key IS NULL;"   # phai = 226813
```

### Dạng "uv run" tương đương (đã tự xác nhận chạy được trong phiên 2026-10-23)
```bash
uv sync                                   # cài dependency theo pyproject.toml/uv.lock
uv run python scripts/01_explore_data.py  # hoặc "uv run scripts/01_explore_data.py" (khong can "python") - ca 2 da test OK
uv run scripts/ingest/ingest_api.py       # tuong tu cho cac script ingest
```

### Chưa viết/chưa kiểm thử (vì Phase chưa tới)
- Lệnh cấu hình Superset connect tới `postgres-dw` qua UI — chưa làm (Phase 6).
- Query OLAP (Roll-up/Slice/Dice/Pivot) trên `fact_sales` — chưa viết (Phase 4, dù DW đã có
  dữ liệu thật để làm ngay).

---

## 8. Lỗi còn tồn tại và rủi ro

1. **Không có lỗi runtime nào đang mở** tại thời điểm cập nhật tài liệu này — toàn bộ 7
   container đều `healthy` (kiểm tra `docker compose ps` lúc viết tài liệu này).
2. **Lệch kiến trúc còn tồn tại, có chủ đích, chưa sửa:** `scripts/03_rfm.py`/`04_kmeans.py`
   (Phase 5 baseline) vẫn đọc từ `data/processed/transactions_clean.csv`, **chưa đổi sang đọc
   từ `fact_sales`/DW** — DW giờ đã có dữ liệu thật (Phase 2.4 xong) nên **không còn bị chặn kỹ
   thuật**, nhưng việc sửa 2 script này nằm ngoài phạm vi các task đã giao (luôn được yêu cầu
   rõ "không sửa script RFM/K-Means") — để lại cho task Phase 5 chính thức sau này.
3. **Thiếu xác nhận đa thành viên:** 0.2, 0.3, 2.1 (review DAG), 3.1 (5 quyết định) đã có nội
   dung kỹ thuật/hạ tầng/thiết kế **và code thật chạy đúng** nhưng theo đúng tiêu chí nghiệm
   thu gốc của đồ án (3 thành viên tự chạy/review + đồng ý bằng văn bản), các task này vẫn ở
   trạng thái `[~]`, không nên tự tick `[x]` nếu chưa có bằng chứng đó.
4. **[ĐÃ TRIỂN KHAI — Phase 2.3]** 5 quyết định nghiệp vụ của Star Schema (mục 5 ở trên) đã
   code thật trong `transform.py` và chạy đúng (xem mục 3.3.f) — vẫn chờ nhóm xem xét/nghiệm
   thu chính thức, có thể cần sửa lại nếu nhóm quyết khác (không đổi schema DW, chỉ sửa logic
   filter trong `transform.py`).
5. **[ĐÃ SỬA — Phase 2.2/2.4]** 2 lỗi môi trường container đã gặp và sửa thật:
   - Extract: `localhost` không trỏ tới service Docker → tham số hóa `FASTAPI_HOST`/
     `PG_SOURCE_HOST` + mount mirror `data/`/`datalake/`/`.env`.
   - Load: `PG_DW_PORT` (.env=5433, port publish host) khác port nội bộ container (5432) khi
     nối qua tên service `postgres-dw` → thêm `PG_DW_PORT_INTERNAL=5432` riêng cho container.
   Cả hai đã verify SUCCESS thật qua DAG run đầy đủ.
9. **Baseline (Phase 0/5) và `fact_sales` (Phase 2.3/2.4) có số dòng khác nhau có chủ đích**:
   baseline 776,857 dòng (loại cả null-Customer-ID + đơn hủy + giá/số lượng≤0 1 lượt, dùng cho
   RFM); `fact_sales` 1,003,670 dòng (giữ null-Customer-ID, chỉ loại đơn hủy + giá/số lượng≤0,
   dùng cho doanh thu DW đầy đủ). **Không phải bất thường** — đã ghi rõ trong 3.3, nhưng cần
   nhớ khi đối chiếu số liệu ở Phase 7 (kiểm thử tổng) để không nhầm là lỗi.
6. **`_PIP_ADDITIONAL_REQUIREMENTS: "openpyxl==3.1.5"`** làm container Airflow chạy `pip
   install` **mỗi lần khởi động/recreate** (không persist vào image) — chậm hơn vài giây mỗi
   lần `docker compose up` cho `airflow-webserver`/`airflow-scheduler`/`airflow-init`. Nếu sau
   này cần thêm nhiều package hoặc muốn khởi động nhanh hơn, nên chuyển sang Dockerfile riêng
   build sẵn — chưa cần thiết ở quy mô hiện tại (chỉ 1 package nhỏ).
7. **`.env` (chứa secret thật) được mount read-only vào container Airflow** (`/opt/airflow/.env`)
   để `scripts/ingest/*.py::load_env_file()` hoạt động đúng khi chạy trong container — rủi ro
   thấp (Airflow là service nội bộ, không public-facing, `.env` vẫn không commit) nhưng cần
   biết: ai có quyền `docker exec` vào container Airflow đều đọc được các secret này.
8. **`datalake/metadata/ingest_log.csv`** giờ có cả dòng ghi `output_path` dạng
   `/opt/airflow/datalake/...` (khi chạy từ Airflow) lẫn dạng `P:\...` (khi chạy từ host) — hai
   định dạng đường dẫn khác nhau cho cùng 1 file vật lý (do mount mirror), không phải lỗi, chỉ
   là log ghi đúng đường dẫn nhìn từ môi trường đã chạy.
4. **File `BaoCaoChuDe.pdf`** (đề bài gốc, đã commit trước đây ở commit `be01f05`) hiện **không
   còn trên đĩa** (`git status` báo `D BaoCaoChuDe.pdf`, chưa stage/commit). Đây là quan sát từ
   phiên trước, **không phải do các phiên Claude Code gây ra** (không có lệnh xoá file nào được
   chạy) — có thể người dùng đã xoá thủ công. Chưa tự khôi phục; nếu cần xem lại đề bài, chạy
   `git checkout -- BaoCaoChuDe.pdf`.
5. **Rủi ro resource khi mở rộng:** máy đang chạy sẵn 1 project Docker khác (`daugiahtpt-*`, 5
   container). Docker Desktop hiện cấp 7.674GB RAM, dự án này đang dùng ~2.2GB — còn dư, nhưng
   nếu thêm Celery worker cho Airflow hoặc đổi Superset sang Postgres+Redis riêng (hiện đang
   dùng SQLite để giảm tải) cần tính lại RAM.
6. **Airflow dùng port 8081 thay vì 8080 mặc định** — nếu copy lệnh/tài liệu Airflow chuẩn từ
   nguồn khác (hay từ `AIRFLOW_WEBSERVER_PORT` trong `.env`), cần nhớ đổi port khi test thủ
   công, tránh nhầm với port 8080 của project khác trên máy.
7. **[ĐÃ VIẾT]** `README.md` đã có hướng dẫn đầy đủ chạy từ máy sạch (clone → `.env` →
   dataset → `docker compose up` → Star Schema → trigger DAG) — chưa test thật trên máy thứ 2
   hoàn toàn mới (chỉ test lại trên chính máy này sau khi daemon/container restart).
8. **FastAPI `/products` chậm ở lần gọi đầu tiên sau mỗi lần container khởi động lại** (~60–90s,
   do `openpyxl` parse file Excel 45MB) — không phải lỗi, nhưng nếu demo trực tiếp nên gọi
   `/products` 1 lần "làm nóng" trước khi người chấm xem, hoặc giải thích rõ trong demo.
9. **Dependency mới `psycopg2-binary`** đã thêm vào `pyproject.toml`/`uv.lock` qua `uv add` —
   máy khác cần chạy `uv sync` (hoặc tương đương) để có dependency này trước khi chạy
   `scripts/ingest/ingest_database.py`.
10. **[ĐÃ XONG — 2026-10-24]** Phase 4 nay đã đầy đủ 4.1+4.2+4.3+4.4 (Roll-up Ngày/Tháng/
    Quý/Năm, Slice, Dice, Pivot, tổng hợp 13 view) — không còn thiếu theo checklist gốc. Giới
    hạn còn lại chỉ là phạm vi hẹp: Pivot mới làm Quốc gia × Năm, chưa làm Thời gian × Sản
    phẩm (4.905 sản phẩm quá nhiều cột cho 1 pivot đọc được).
10. **`datalake/raw/`, `datalake/processed/`** đã thêm vào `.gitignore` — dữ liệu trong đó
    *không* được commit, máy khác cần tự chạy `scripts/ingest/run_all.py` để tái tạo. Chỉ
    `datalake/metadata/ingest_log.csv` được commit.
