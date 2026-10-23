# PROJECT CONTEXT — Tài liệu bàn giao

> Mục đích: cho phép mở một phiên Claude Code mới và tiếp tục làm việc ngay, không mất ngữ
> cảnh. Tài liệu này được tạo/cập nhật bằng cách đọc trực tiếp code, chạy lại script, kiểm tra
> container Docker thật — không suy đoán từ tên file.
>
> Cập nhật lần cuối: **2026-10-23** (sau khi hoàn thành Phase 1). Nguồn đối chiếu chi tiết hơn:
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
| 2. ETL Airflow | **TODO** | Airflow đã chạy (hạ tầng) nhưng **chưa có DAG nào** trong `airflow/dags/`; có thể tái sử dụng `scripts/ingest/*.py` làm core logic Extract |
| 3. Data Warehouse (Star Schema) | **TODO** | `sql/` rỗng, chưa có DDL, Postgres DW chạy nhưng chưa có bảng nghiệp vụ |
| 4. OLAP | **BLOCKED** | Phụ thuộc Phase 3 (DW phải có dữ liệu) |
| 5.1 Tính RFM | **IN PROGRESS** (lệch kiến trúc) | Có kết quả thật nhưng đọc từ CSV, chưa từ DW — xem mục 5 |
| 5.2–5.4 Chuẩn hóa/K-Means/diễn giải cụm | **DONE** | Có kết quả + log thật, xem mục 3 |
| 5.5 Nạp cluster vào DW | **BLOCKED** | Phụ thuộc Phase 3 (DW chưa có bảng) |
| 6. Dashboard Superset | **TODO** | Superset đã chạy (hạ tầng) nhưng chưa kết nối DW, chưa có chart nào |
| 7. Kiểm thử tổng/báo cáo/demo | **BLOCKED** | Phụ thuộc Phase 1–4, 6 |
| 8. Mở rộng (Apriori/Decision Tree) | **TODO** (đúng kế hoạch, chưa cần làm) | Chỉ làm nếu còn thời gian sau Phase 0–7 |

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
- **Lưu ý quan trọng:** pipeline này đọc trực tiếp từ `transactions_clean.csv`, **không phải
  từ Data Warehouse** như thiết kế gốc yêu cầu (vì DW chưa tồn tại). Đây là baseline/prototype
  dùng để đối chiếu số liệu sau này (Phase 7.1/7.2), **không phải pipeline chính thức**. Khi
  Phase 3 xong, cần viết lại bước đọc dữ liệu (đọc từ `Fact_Sales`/`Dim_Customer` trong
  `postgres-dw`) rồi mới tick `[x]` thật cho 5.1 và làm 5.5.

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

---

## 4. Những gì chưa làm (và lý do/phụ thuộc)

| Việc chưa làm | Vì sao / phụ thuộc gì |
|---|---|
| Xác nhận nhóm cho 0.2, 0.3 | Cần hành động thủ công của TV2/TV3 (chụp màn hình, đồng ý bằng văn bản) — ngoài khả năng của 1 phiên code |
| DAG Airflow Extract/Transform/Load (Phase 2) | Chưa viết DAG nào trong `airflow/dags/`; Phase 1 đã xong và `scripts/ingest/*.py` dùng lại được cho task Extract, nhưng Transform/Load còn cần thiết kế Star Schema (3.1) chốt trước khi code Load |
| Star Schema DDL (Phase 3) | `sql/` rỗng; cần thiết kế ERD rồi viết `.sql`, chạy trên `postgres-dw` |
| OLAP queries (Phase 4) | Phụ thuộc Phase 3 (DW phải có dữ liệu thật để viết/test query) |
| RFM/K-Means đọc từ DW thay vì CSV, nạp cluster vào DW (5.1 chính thức, 5.5) | Phụ thuộc Phase 3 |
| Superset kết nối DW + chart + dashboard (Phase 6) | Phụ thuộc Phase 3, 4, 5.5 — Superset hiện chỉ chạy trống, chưa khai báo datasource nào |
| Kiểm thử tổng, README, báo cáo, demo (Phase 7) | Phụ thuộc toàn bộ Phase 1–6 |
| Apriori/Decision Tree (Phase 8) | Theo kế hoạch, chỉ làm sau khi Phase 0–7 xong và còn thời gian — **không phải thiếu sót** |

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
- `source_customers` trong `postgres-source` nạp theo chiến lược **TRUNCATE + INSERT lại toàn
  bộ** mỗi lần chạy (không phải upsert tăng dần) — đơn giản, idempotent, phù hợp vì bảng này
  phản ánh toàn bộ danh sách khách hàng tĩnh từ 1 dataset, không phải luồng dữ liệu tăng dần.
- `datalake/raw/`, `datalake/processed/` nằm trong `.gitignore` (dữ liệu lớn, tái tạo được
  bằng `scripts/ingest/*.py`); riêng `datalake/metadata/ingest_log.csv` được commit làm bằng
  chứng kiểm thử.

---

## 6. Bước tiếp theo nên làm (ưu tiên cao nhất)

> Cập nhật 2026-10-23: Phase 1 (mục tiêu của bản trước đây của mục này) **đã xong**, xem mục
> 3.3.b. Ưu tiên tiếp theo đổi sang Phase 3.1–3.2.

**Task: Phase 3.1 + 3.2 — Thiết kế Star Schema và tạo DDL trên `postgres-dw`.**

**Lý do chọn đây là ưu tiên cao nhất:**
- Đây là điểm đồng bộ bắt buộc #2 của dự án (mục 6 `PROJECT_CHECKLIST.md`): phải chốt Star
  Schema **trước khi** code task Load (2.4), và Phase 2.1 (thiết kế DAG tổng thể) cũng ghi rõ
  phụ thuộc `3.1`. Nếu làm Phase 2 (DAG) trước, dễ phải sửa lại khi schema đổi.
- Hạ tầng `postgres-dw` đã chạy healthy sẵn (Phase 0.2) — chỉ cần viết DDL và chạy thử, rủi ro
  thấp, không đụng service khác.
- Dữ liệu đã sẵn ở Phase 1 (`datalake/raw/file`, `datalake/raw/database`) + kết quả khảo sát
  0.1 đủ để thiết kế schema có cơ sở thực tế (không phải đoán).

**Đầu vào:**
- Kết quả khảo sát 0.1 (cột, kiểu dữ liệu, phạm vi giá trị) trong `docs/01_bao_cao_tien_do.md`.
- Dự thảo quy tắc 0.3 trong `PROJECT_CHECKLIST.md` (grain của Fact_Sales, cách xử lý đơn hủy).
- Cấu trúc gợi ý đã có sẵn trong `PROJECT_CHECKLIST.md` mục 3.1: `Fact_Sales` (grain = 1 dòng
  hàng trong 1 hóa đơn, measures Quantity/UnitPrice/Revenue) + `Dim_Date`, `Dim_Customer`,
  `Dim_Product`, `Dim_Country`.

**File cần sửa/tạo:**
- Tạo `sql/001_create_star_schema.sql` (DDL `CREATE TABLE` cho toàn bộ Fact/Dim + khóa
  chính/ngoại) — thư mục `sql/` hiện đang rỗng.
- Có thể thêm `docs/03_star_schema.md` hoặc hình ERD đơn giản (không bắt buộc, nhưng tiêu chí
  nghiệm thu gốc của 3.1 yêu cầu "sơ đồ ERD" — nếu bỏ qua cần ghi rõ lý do).

**Tiêu chí kiểm thử (phải làm thật trước khi tick `[x]`):**
- Chạy `sql/001_create_star_schema.sql` trên `postgres-dw` (qua `docker exec ... psql -f` hoặc
  tương đương) không lỗi.
- `\dt` (hoặc `SELECT table_name FROM information_schema.tables WHERE table_schema='public'`)
  liệt kê đủ `fact_sales`, `dim_date`, `dim_customer`, `dim_product`, `dim_country`.
- Kiểm tra khóa ngoại tồn tại đúng (`\d fact_sales` hoặc query `information_schema`).
- **Chưa cần nạp dữ liệu thật vào DW ở bước này** (đó là Phase 2.4) — chỉ cần schema tạo đúng,
  rỗng dữ liệu là hợp lệ cho 3.1/3.2.

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
```

### Ghi trong docstring script nhưng CHƯA tự kiểm thử trong các phiên này
```bash
uv run scripts/01_explore_data.py   # tương đương lệnh .venv/Scripts/python.exe ở trên,
                                     # nhưng chưa tự chạy qua "uv run" để xác nhận — chỉ xác
                                     # nhận "uv --version" chạy được (uv 0.11.6) trên máy này
```

### Chưa viết/chưa kiểm thử (vì Phase chưa tới)
- Lệnh trigger DAG Airflow (`airflow dags trigger ...`) — chưa có DAG nào trong `airflow/dags/`.
- Lệnh chạy DDL Star Schema (`psql -f sql/*.sql` hay tương đương) — `sql/` đang rỗng, đây là
  nội dung của task tiếp theo (mục 6).
- Lệnh cấu hình Superset connect tới `postgres-dw` qua UI — chưa làm.

---

## 8. Lỗi còn tồn tại và rủi ro

1. **Không có lỗi runtime nào đang mở** tại thời điểm cập nhật tài liệu này — toàn bộ 7
   container đều `healthy` (kiểm tra `docker compose ps` lúc viết tài liệu này).
2. **Lệch kiến trúc đã biết, chưa sửa (có chủ đích):** RFM/K-Means (Phase 5) đọc từ CSV thay
   vì từ DW — xem mục 3.2. Không sửa cho tới khi Phase 3 (DW) xong, để tránh sửa nửa chừng gây
   vỡ phụ thuộc.
3. **Thiếu xác nhận đa thành viên:** 0.2 và 0.3 đã có nội dung kỹ thuật/hạ tầng thật nhưng theo
   đúng tiêu chí nghiệm thu gốc của đồ án (3 thành viên tự chạy + đồng ý bằng văn bản), hai task
   này vẫn ở trạng thái `[~]`, không nên tự tick `[x]` trong phiên code tiếp theo nếu chưa có
   bằng chứng đó.
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
7. **Danh sách người ngoài nhóm chưa kiểm thử được:** README hướng dẫn chạy từ máy sạch (Phase
   7.3) chưa viết — hiện mới có hướng dẫn rải rác trong `PROJECT_CHECKLIST.md`/tài liệu này.
8. **FastAPI `/products` chậm ở lần gọi đầu tiên sau mỗi lần container khởi động lại** (~60–90s,
   do `openpyxl` parse file Excel 45MB) — không phải lỗi, nhưng nếu demo trực tiếp nên gọi
   `/products` 1 lần "làm nóng" trước khi người chấm xem, hoặc giải thích rõ trong demo.
9. **Dependency mới `psycopg2-binary`** đã thêm vào `pyproject.toml`/`uv.lock` qua `uv add` —
   máy khác cần chạy `uv sync` (hoặc tương đương) để có dependency này trước khi chạy
   `scripts/ingest/ingest_database.py`.
10. **`datalake/raw/`, `datalake/processed/`** đã thêm vào `.gitignore` — dữ liệu trong đó
    *không* được commit, máy khác cần tự chạy `scripts/ingest/run_all.py` để tái tạo. Chỉ
    `datalake/metadata/ingest_log.csv` được commit.
