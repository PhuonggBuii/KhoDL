# KhoDL — Phân tích hành vi mua sắm & phân nhóm khách hàng (Online Retail II)

Đồ án môn **Kho dữ liệu và Khai phá dữ liệu** — PTIT. Hướng dẫn này giúp một thành viên
**chưa từng chạy dự án** clone repo về máy và chạy được toàn bộ phần đã hoàn thành.

> **Trạng thái hiện tại: Phase 0 (khảo sát + môi trường) và Phase 1 (3 nguồn ingest + Data
> Lake) đã hoàn thành, có kiểm thử thật.** Phase 2 (Airflow DAG), Phase 3 (Data Warehouse),
> Phase 4 (OLAP), Phase 6 (Superset dashboard) **chưa triển khai** — hạ tầng (container) của
> các service này đã chạy sẵn nhưng chưa có nội dung nghiệp vụ. Chi tiết đầy đủ, bằng chứng
> kiểm thử, và việc cần làm tiếp theo: xem [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md),
> [`PROJECT_CHECKLIST.md`](PROJECT_CHECKLIST.md) và
> [`docs/PROJECT_GAP_ANALYSIS.md`](docs/PROJECT_GAP_ANALYSIS.md).

---

## A. Giới thiệu

**Mục tiêu:** xây dựng hệ thống end-to-end: tích hợp dữ liệu đa nguồn (mô phỏng) → Data Lake
→ ETL (Airflow) → Data Warehouse (PostgreSQL, Star Schema) → OLAP → Data Mining (RFM +
K-Means) → Dashboard (Superset).

**Dataset:** [Online Retail II (Kaggle)](https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset)
— giao dịch bán lẻ online 2009–2011.

**Ba "nguồn dữ liệu" đều là mô phỏng từ cùng 1 dataset Online Retail II**, tách theo chiều dữ
liệu — **không phải 3 nguồn độc lập ngoài đời thực**:

| Nguồn mô phỏng | Lấy từ dataset | Công nghệ |
|---|---|---|
| **API** | Danh mục sản phẩm duy nhất (StockCode, Description, giá tham chiếu) | FastAPI (`api/`) |
| **File** | Giao dịch (Invoice, StockCode, Quantity, InvoiceDate, Price), giữ nguyên bản gốc | Đọc Excel trực tiếp (`scripts/ingest/ingest_file.py`) |
| **Database** | Khách hàng (Customer ID, Country) | PostgreSQL nguồn (`postgres-source`) |

**Kiến trúc tổng quan:**

```
online_retail_II.xlsx (Kaggle)
   │
   ├─ API (FastAPI /products)        ─┐
   ├─ File (doc truc tiep Excel)      ├─► Data Lake (datalake/raw/{api,file,database})
   └─ Database (postgres-source)     ─┘        + datalake/metadata/ingest_log.csv
                                               │
                                     (Phase 2 — CHƯA LÀM)
                                               ▼
                                   PostgreSQL Data Warehouse (Star Schema)
                                               │
                                     (Phase 4/6 — CHƯA LÀM)
                                               ▼
                                   OLAP + Data Mining → Superset Dashboard
```

Data Mining (RFM + K-Means, `scripts/03_rfm.py`, `scripts/04_kmeans.py`) đã có kết quả thật
nhưng hiện đọc từ `data/processed/transactions_clean.csv` (baseline tạm), **chưa đọc từ Data
Warehouse** vì DW (Phase 3) chưa tồn tại.

---

## B. Yêu cầu cài đặt

| Công cụ | Bắt buộc cho | Đã xác minh chạy với |
|---|---|---|
| **Git** | Clone repo | — |
| **Docker Desktop** (kèm Docker Compose) | Chạy toàn bộ hạ tầng (Postgres, Airflow, FastAPI, Superset) | Docker 29.8.1, Compose v5.5.1 (phiên bản mới hơn tương đương vẫn dùng được) |
| **Python ≥ 3.11** + **[uv](https://docs.astral.sh/uv/)** | Chạy script trên host: khảo sát/làm sạch dữ liệu (`scripts/`), ingest Phase 1 (`scripts/ingest/`) | Python 3.11 (`.python-version`), uv 0.11.6 |
| Tài khoản Kaggle (miễn phí) | Tải dataset gốc | — |

**Ghi chú:** FastAPI/Airflow/Superset chạy **trong container** (không cần cài Python trực
tiếp để chạy chúng) — Python + uv trên host chỉ cần khi bạn muốn tự chạy script khảo sát dữ
liệu, ingest Phase 1, hoặc RFM/K-Means. Nếu chỉ cần xem hạ tầng chạy, Docker Desktop là đủ.

---

## C. Lấy dataset

1. Đăng nhập Kaggle, vào trang dataset chính thức:
   https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset
2. Tải file **`online_retail_II.xlsx`** (không đổi tên, không chỉnh sửa nội dung).
3. Đặt file vào đúng đường dẫn: **`data/raw/online_retail_II.xlsx`**.

File này gồm 2 sheet `Year 2009-2010` và `Year 2010-2011`, cột: `Invoice, StockCode,
Description, Quantity, InvoiceDate, Price, Customer ID, Country` (đã xác minh trực tiếp từ
code, xem `scripts/01_explore_data.py`).

**Không commit dataset gốc lên Git** — file này đã có trong `.gitignore`
(`data/raw/*`), xem thêm [`data/README.md`](data/README.md).

---

## D. Cấu hình môi trường

### 1. Clone repository
```bash
git clone https://github.com/PhuonggBuii/KhoDL.git
cd KhoDL
```

### 2. Tạo file `.env` từ mẫu
PowerShell (Windows):
```powershell
Copy-Item .env.example .env
```
Linux/macOS:
```bash
cp .env.example .env
```

Mở `.env`, thay **tất cả** giá trị `change_me` bằng giá trị ngẫu nhiên riêng của máy bạn —
**không dùng lại giá trị của người khác, không chia sẻ file `.env` cho ai, không commit file
này** (đã nằm trong `.gitignore`). Có thể sinh giá trị ngẫu nhiên bằng Python (đã cài ở bước
B):
```bash
python -c "import secrets; print(secrets.token_urlsafe(24))"                       # cho cac *_PASSWORD, *_SECRET_KEY
python -c "import os, base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"  # rieng cho AIRFLOW_FERNET_KEY
```
`AIRFLOW_ADMIN_USER`/`SUPERSET_ADMIN_USER` có thể để `admin`; đặt password riêng cho
`AIRFLOW_ADMIN_PASSWORD`/`SUPERSET_ADMIN_PASSWORD`. Các biến `*_PORT` giữ nguyên trừ khi bị
xung đột cổng trên máy bạn (xem mục F).

### 3. Cài Python dependencies (cho script host)
```bash
uv sync
```
Lệnh này đọc đúng `pyproject.toml`/`uv.lock`, tạo `.venv/` với các package cần thiết
(pandas, scikit-learn, psycopg2-binary, openpyxl, matplotlib).

---

## E. Khởi chạy dự án

### 1. Khởi động toàn bộ hạ tầng bằng Docker Compose
```bash
docker compose up -d --build
```
Lệnh này tự khởi động đúng thứ tự (Postgres → Airflow init → Airflow webserver/scheduler;
Superset, FastAPI độc lập) nhờ các điều kiện `depends_on`/`healthcheck` đã khai báo trong
`docker-compose.yml` — **đã kiểm thử thật**: toàn bộ 7 container chuyển sang `healthy` trong
dưới 1 phút từ trạng thái sạch.

### 2. Kiểm tra trạng thái container
```bash
docker compose ps
```
Cả 7 dòng (`postgres-source`, `postgres-dw`, `postgres-airflow`, `airflow-webserver`,
`airflow-scheduler`, `fastapi`, `superset`) phải ở trạng thái `healthy` (có thể mất 30–90s sau
khi chạy lệnh ở bước 1).

| Service | URL/Port trên host | Đăng nhập |
|---|---|---|
| FastAPI | http://localhost:8000 (`/health`, `/products`) | — |
| Airflow UI | http://localhost:8081 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` trong `.env` |
| Superset UI | http://localhost:8088 | `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` trong `.env` |
| Postgres nguồn | `localhost:5432` (db `retail_source`) | user/pass trong `.env` (`PG_SOURCE_*`) |
| Postgres DW | `localhost:5433` (db `retail_dw`) | user/pass trong `.env` (`PG_DW_*`) |

> Airflow UI dùng cổng **8081** (không phải 8080 mặc định) để tránh xung đột với service khác
> có thể đang chạy trên máy bạn — xem `AIRFLOW_WEBSERVER_PORT` trong `.env`.

### 3. Chạy Phase 1 — ingest 3 nguồn vào Data Lake
```bash
uv run scripts/ingest/run_all.py
```
Script này gọi FastAPI `/products` (nguồn API), đọc Excel (nguồn File), và nạp+SELECT lại
PostgreSQL nguồn (nguồn Database) — tuần tự, in log ra console và ghi vào
`datalake/metadata/ingest_log.csv`. Có thể chạy lại nhiều lần an toàn (nguồn Database là
idempotent — không tạo bản ghi trùng; nguồn API/File ghi đè file snapshot cùng ngày).

Chạy riêng từng nguồn nếu cần:
```bash
uv run scripts/ingest/ingest_api.py
uv run scripts/ingest/ingest_file.py
uv run scripts/ingest/ingest_database.py
```

### 4. Kiểm tra kết quả
```bash
curl http://localhost:8000/health              # {"status":"healthy"}
curl http://localhost:8000/products             # danh sách sản phẩm (JSON), ~5.300 StockCode
cat datalake/metadata/ingest_log.csv            # lịch sử ingest (append-only)
```
Kiểm tra bảng `source_customers` trong Postgres nguồn:
```bash
docker exec khodl-postgres-source psql -U <PG_SOURCE_USER_trong_.env> -d <PG_SOURCE_DB_trong_.env> -c "SELECT count(*) FROM source_customers;"
```
(Giá trị mặc định trong `.env.example`: user `source_user`, db `retail_source` — kết quả mong
đợi: **5.942** dòng, đã xác minh khớp số liệu khảo sát ở `docs/01_bao_cao_tien_do.md`.)

### 5. Dừng service an toàn (không mất dữ liệu)
```bash
docker compose stop           # dung container, GIU container + du lieu (volume)
# hoac
docker compose down           # xoa container, GIU volume (du lieu Postgres/Airflow/Superset van con)
```
**Không** chạy `docker compose down -v` hoặc xóa volume Docker — lệnh đó sẽ xóa toàn bộ dữ
liệu đã nạp trong `postgres-source`/`postgres-dw`/Airflow/Superset.

---

## F. Khắc phục lỗi thường gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `docker compose up` báo lỗi `...dockerDesktopLinuxEngine...The system cannot find the file specified` | Docker Desktop chưa chạy (daemon chưa khởi động) | Mở ứng dụng Docker Desktop, chờ icon trạng thái "Running", chạy lại lệnh |
| `FileNotFoundError: Khong tim thay dataset tai .../data/raw/online_retail_II.xlsx` (khi gọi `/products` hoặc chạy `scripts/ingest/ingest_file.py`) | Chưa đặt dataset đúng vị trí | Xem lại mục C, đảm bảo đúng tên file `online_retail_II.xlsx` trong `data/raw/` |
| `curl /products` trả `{"detail":"Khong tim thay dataset..."}` (HTTP 503) | FastAPI container không thấy dataset | Kiểm tra `docker-compose.yml` có mount `./data:/app/data:ro` cho service `fastapi`; đảm bảo file tồn tại trên host trước khi `docker compose up` |
| `docker compose up` báo port đã được dùng (`bind: address already in use`) | Một service khác trên máy đang chiếm cổng (ví dụ 5432, 8080, 8088) | Đổi cổng tương ứng trong `.env` (`PG_SOURCE_PORT`, `AIRFLOW_WEBSERVER_PORT`, `SUPERSET_PORT`, `FASTAPI_PORT`, `PG_DW_PORT`), chạy lại `docker compose up -d --build` |
| `docker compose ps` cho thấy Postgres chưa `healthy` sau >1 phút | Máy chậm hoặc đang khởi động lần đầu | Chờ thêm, xem log cụ thể bằng `docker logs <container_name>` |
| `scripts/ingest/ingest_database.py` lỗi `KeyError: 'PG_SOURCE_PORT'` hoặc tương tự | Chưa tạo `.env` hoặc thiếu biến | Xem lại mục D bước 2 — đảm bảo `.env` tồn tại ở gốc repo và có đủ biến như `.env.example` |
| `uv run ...` báo thiếu package (`ModuleNotFoundError`) | Chưa chạy `uv sync` | Chạy `uv sync` ở gốc repo rồi thử lại |
| Lần gọi `/products` đầu tiên sau khi (re)build container rất chậm (~60–90s) | FastAPI phải đọc + parse Excel 45MB bằng `openpyxl` lần đầu để tạo cache | Không phải lỗi — các lần gọi sau trả về gần như tức thì nhờ cache trong bộ nhớ |

---

## G. Hướng phát triển tiếp theo

Theo [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) mục 6, task ưu tiên cao nhất tiếp theo là:

- **Phase 3.1 + 3.2 — Thiết kế Star Schema và viết DDL cho `postgres-dw`** (`sql/` hiện đang
  rỗng). Đây là điểm đồng bộ bắt buộc trước khi code DAG Airflow Load (Phase 2.4).

Các phần **chưa triển khai**, không được coi là đã hoàn thành dù hạ tầng container đã chạy:

- Phase 2 — DAG Airflow Extract/Transform/Load (`airflow/dags/` hiện đang rỗng).
- Phase 3 — Star Schema PostgreSQL DW (`sql/` hiện đang rỗng).
- Phase 4 — OLAP queries.
- Phase 5.5 — Nạp kết quả phân cụm (`cluster_label`) vào Data Warehouse.
- Phase 6 — Kết nối Superset với DW + dashboard (Superset hiện chạy nhưng chưa có datasource/chart nào).
- Phase 7 — Kiểm thử tổng, báo cáo, demo.
- Phase 8 — Apriori/Decision Tree (mở rộng, chỉ làm nếu còn thời gian).

Chi tiết đầy đủ từng Phase, bằng chứng kiểm thử và quyết định kỹ thuật đã thống nhất: xem
[`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md), [`PROJECT_CHECKLIST.md`](PROJECT_CHECKLIST.md),
[`docs/PROJECT_GAP_ANALYSIS.md`](docs/PROJECT_GAP_ANALYSIS.md).
