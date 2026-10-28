# KhoDL — Phân tích hành vi mua sắm & phân nhóm khách hàng (Online Retail II)

Đồ án môn **Kho dữ liệu và Khai phá dữ liệu** — PTIT. Hướng dẫn này giúp một thành viên
**chưa từng chạy dự án** clone repo về máy khác và chạy được toàn bộ phần đã hoàn thành, từ
`docker compose up` tới khi có dữ liệu thật trong Data Warehouse.

> **Trạng thái hiện tại: Phase 0–6 đã triển khai và kiểm thử thật.** Extract (3 nguồn mô
> phỏng) → Transform → Load → Validate (Airflow DAG `retail_online_ii_etl`, `fact_sales` =
> **1.003.670 dòng**) → OLAP (13 view SQL) → Data Mining (RFM + K-Means từ DW, bảng
> `customer_segment` = **5.853 khách hàng**) → Superset Dashboard (6 chart, 2 bộ lọc, kết nối
> thật tới Data Warehouse). **Phase 7 (kiểm thử tổng/báo cáo/demo) đang hoàn thiện. Phase 8.1
> (Market Basket Analysis / Apriori) đã hoàn thành và kiểm thử thật** (thuật toán tự cài đặt +
> CSV + tích hợp DW `market_basket_rules` + dashboard Superset riêng, xem mục E bước 6);
> **Phase 8.2 (Decision Tree)**
> vẫn là mở rộng tùy chọn, chưa làm. Chi tiết đầy đủ, bằng chứng kiểm
> thử, quyết định kỹ thuật: xem [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md),
> [`PROJECT_CHECKLIST.md`](PROJECT_CHECKLIST.md), [`docs/PROJECT_GAP_ANALYSIS.md`](docs/PROJECT_GAP_ANALYSIS.md)
> và [`docs/00_bao_cao_do_an.md`](docs/00_bao_cao_do_an.md) (báo cáo tổng hợp + kịch bản demo).

---

## A. Giới thiệu

**Mục tiêu:** xây dựng hệ thống end-to-end: tích hợp dữ liệu đa nguồn (mô phỏng) → Data Lake
→ ETL (Airflow) → Data Warehouse (PostgreSQL, Star Schema) → OLAP → Data Mining (RFM +
K-Means) → Dashboard (Superset).

**Dataset:** [Online Retail II (Kaggle)](https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset)
— giao dịch bán lẻ online 2009–2011 (1.067.371 dòng gốc).

**Ba "nguồn dữ liệu" đều là mô phỏng từ cùng 1 dataset Online Retail II**, tách theo chiều dữ
liệu — **không phải 3 nguồn độc lập ngoài đời thực**:

| Nguồn mô phỏng | Lấy từ dataset | Công nghệ |
|---|---|---|
| **API** | Danh mục sản phẩm duy nhất (StockCode, Description, giá tham chiếu) | FastAPI (`api/`) |
| **File** | Giao dịch (Invoice, StockCode, Quantity, InvoiceDate, Price), giữ nguyên bản gốc | Đọc Excel trực tiếp (`scripts/ingest/ingest_file.py`) |
| **Database** | Khách hàng (Customer ID, Country), đã khử trùng theo khách hàng | PostgreSQL nguồn (`postgres-source`) |

**Kiến trúc & luồng dữ liệu (đã chạy thật đầu-cuối):**

```
online_retail_II.xlsx (Kaggle)
   │
   ├─ API (FastAPI /products)        ─┐
   ├─ File (doc truc tiep Excel)      ├─► Data Lake (datalake/raw/{api,file,database})
   └─ Database (postgres-source)     ─┘        + datalake/metadata/ingest_log.csv
                                               │   Airflow DAG: task group "extract"
                                               ▼
                                  Transform (scripts/transform/transform.py)
                                  - doc truc tiep dataset goc (xem ghi chu *)
                                  - bo trung lap, loai don huy, loai Qty/Price<=0,
                                    loai ma phi san pham; GIU dong thieu Customer ID
                                               │
                                               ▼
                                  datalake/processed/<ngay>/{dim_*,fact_sales}.csv
                                               │   Airflow DAG task: "load"
                                               ▼
                       PostgreSQL Data Warehouse (Star Schema) — postgres-dw
                       dim_date · dim_customer · dim_product · dim_country · fact_sales
                       (1.003.670 dong, da xac thuc - Airflow DAG task: "validate")
                                               │
                        ┌──────────────────────┼──────────────────────┐
                        ▼                      ▼                      │
              OLAP (sql/002_olap_views.sql)   RFM + K-Means tu DW      │
              13 view SQL (roll-up/slice/     (scripts/mining/*.py)    │
              dice/pivot/KPI)                 -> bang customer_segment │
                        │                      │                       │
                        └──────────┬───────────┘                       │
                                   ▼                                   │
                        Apache Superset Dashboard  <────────────────────┘
                        (6 chart, 2 filter, ket noi postgres-dw thuc)
```

**(\*) Ghi chú kiến trúc quan trọng:** `Transform` đọc trực tiếp `data/raw/online_retail_II.xlsx`
(không join lại 3 "nguồn" mô phỏng trong Data Lake), vì nguồn **File** không mang Customer
ID/Country và nguồn **Database** chỉ lưu Customer ID+Country **đã khử trùng theo khách hàng**
(không còn liên kết Invoice) — hai nguồn này không có khóa chung để join lại thành 1 dòng giao
dịch đầy đủ. Đây là hạn chế đã biết của kiến trúc "3 nguồn mô phỏng từ 1 dataset", không phải
lỗi. Transform vẫn xác thực `datalake/raw/*` (kết quả Extract) tồn tại và khớp số dòng trước
khi tính toán. Chi tiết: [`docs/03_star_schema.md`](docs/03_star_schema.md) mục 6,
[`docs/PROJECT_GAP_ANALYSIS.md`](docs/PROJECT_GAP_ANALYSIS.md) mục 12.

Data Mining (RFM + K-Means) có **2 pipeline**: baseline (`scripts/03_rfm.py`,
`scripts/04_kmeans.py`, đọc từ `data/processed/transactions_clean.csv`, giữ nguyên làm tham
chiếu) và pipeline chính thức đọc trực tiếp từ Data Warehouse
(`scripts/mining/rfm_dw.py`, `scripts/mining/cluster_dw.py`,
`scripts/mining/load_segments_dw.py`) — đã đối chiếu khớp tuyệt đối baseline. Chi tiết:
[`docs/05_rfm_kmeans_dw.md`](docs/05_rfm_kmeans_dw.md).

**(Mở rộng, Phase 8.1)** `scripts/mining/apriori_mba.py` phân tích luật kết hợp (Market Basket
Analysis) trên cùng dữ liệu `fact_sales` — độc lập, không nằm trong DAG, không ảnh hưởng luồng
trên. Chi tiết: [`docs/08_apriori_mba.md`](docs/08_apriori_mba.md).

### Công nghệ & phiên bản chính (đối chiếu trực tiếp `docker-compose.yml`/`pyproject.toml`)

| Thành phần | Phiên bản/image | Vai trò |
|---|---|---|
| PostgreSQL | `postgres:16-alpine` (×3: `postgres-source`, `postgres-dw`, `postgres-airflow`) | DB nguồn, Data Warehouse, metadata Airflow |
| Apache Airflow | `apache/airflow:2.10.5-python3.12`, `LocalExecutor` | Điều phối DAG ETL |
| Apache Superset | `apache/superset:4.1.1` | Dashboard BI |
| Python | `>=3.11` (xem `.python-version`), quản lý bằng [uv](https://docs.astral.sh/uv/) | Script host (ingest/transform/load/mining) |
| Thư viện Python chính | `pandas`, `scikit-learn`, `psycopg2-binary`, `openpyxl`, `matplotlib` (xem `pyproject.toml`) | Xử lý dữ liệu, RFM/K-Means, kết nối Postgres, đọc Excel, vẽ biểu đồ |
| FastAPI | theo `api/requirements.txt`, build từ `api/Dockerfile` | Mô phỏng nguồn "API" |

### Cấu trúc thư mục chính

```
KhoDL/
├── airflow/dags/retail_etl_dag.py   # DAG Airflow duy nhat (Extract->Transform->Load->Validate)
├── api/                              # FastAPI mo phong nguon "API" (app/main.py, app/data.py)
├── data/
│   ├── raw/                          # dataset Kaggle goc (KHONG commit — xem muc C)
│   └── processed/                    # output RFM/K-Means/Apriori dang CSV (KHONG commit)
├── datalake/
│   ├── raw/{api,file,database}/<ngay>/   # snapshot Extract (KHONG commit)
│   ├── processed/<ngay>/                  # output Transform: dim_*.csv, fact_sales.csv (KHONG commit)
│   └── metadata/ingest_log.csv            # log ingest append-only (CO commit — bang chung kiem thu)
├── docs/                              # tai lieu thiet ke + bao cao tung Phase (xem muc H)
│   └── architecture/                  # so do kien truc he thong (PNG)
├── scripts/
│   ├── 01_explore_data.py, 02_clean_data.py   # khao sat/lam sach du lieu (Phase 0)
│   ├── 03_rfm.py, 04_kmeans.py                 # RFM+K-Means baseline, doc CSV (Phase 5 goc)
│   ├── ingest/                                  # 3 nguon mo phong + Data Lake (Phase 1)
│   ├── transform/transform.py                   # Transform — ap dung quy tac lam sach (Phase 2.3)
│   ├── load/load.py                             # Load vao PostgreSQL DW (Phase 2.4)
│   ├── mining/                                   # RFM/K-Means tu DW (5.1, 5.5) + Apriori (8.1)
│   └── superset/                                 # tao database/dataset/chart qua REST API (Phase 6)
├── sql/                               # DDL: Star Schema, OLAP view, customer_segment, market_basket_rules
├── superset/superset_config.py        # cau hinh Superset (SECRET_KEY doc tu env)
├── docker-compose.yml                 # toan bo ha tang (7 service)
├── .env.example                       # mau bien moi truong (an toan, khong chua secret that)
└── pyproject.toml / uv.lock           # dependency Python (quan ly boi uv)
```

---

## B. Yêu cầu cài đặt

| Công cụ | Bắt buộc cho | Đã xác minh chạy với |
|---|---|---|
| **Git** | Clone repo | — |
| **Docker Desktop** (kèm Docker Compose) | Chạy toàn bộ hạ tầng (Postgres×3, Airflow, FastAPI, Superset) | Docker 29.8.1, Compose v5.5.1 (phiên bản mới hơn tương đương vẫn dùng được) |
| **Python ≥ 3.11** + **[uv](https://docs.astral.sh/uv/)** | Chạy script trên host: khảo sát/làm sạch dữ liệu, ingest (Phase 1), Transform/Load (Phase 2.3/2.4) | Python 3.11 (`.python-version`), uv 0.11.6 |
| Tài khoản Kaggle (miễn phí) | Tải dataset gốc | — |

**Ghi chú:** FastAPI/Airflow/Superset/Postgres chạy **trong container** (không cần cài Python
trực tiếp để chạy chúng) — Python + uv trên host chỉ cần khi bạn muốn tự chạy script
khảo sát/ingest/transform/load **từ host** thay vì qua Airflow. Nếu chỉ cần hạ tầng + chạy
pipeline qua Airflow UI, Docker Desktop là đủ.

---

## C. Lấy dataset

1. Đăng nhập Kaggle, vào trang dataset chính thức:
   https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset
2. Tải file **`online_retail_II.xlsx`** (không đổi tên, không chỉnh sửa nội dung).
3. Đặt file vào đúng đường dẫn: **`data/raw/online_retail_II.xlsx`**.

File này gồm 2 sheet `Year 2009-2010` và `Year 2010-2011`, cột: `Invoice, StockCode,
Description, Quantity, InvoiceDate, Price, Customer ID, Country` (đã xác minh trực tiếp từ
code và dữ liệu thật nhiều lần, xem `scripts/01_explore_data.py`).

**Không commit dataset gốc lên Git** — file này đã có trong `.gitignore`
(`data/raw/*`), xem thêm [`data/README.md`](data/README.md). Dataset **phải có trước** khi
chạy bất kỳ bước ingest/transform nào ở mục E — nếu thiếu, các script sẽ báo lỗi rõ
`FileNotFoundError` (xem mục F).

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

`.env.example` chứa **đầy đủ, đúng tên** mọi biến môi trường mà `docker-compose.yml` và các
script thực sự đọc (đối chiếu trực tiếp với code trong lượt cập nhật này) — không cần thêm
biến nào khác. Vài biến host-nội-bộ (`FASTAPI_HOST`, `PG_SOURCE_HOST`, `PG_DW_HOST`,
`PG_DW_PORT_INTERNAL`) được đặt cố định ngay trong `docker-compose.yml` (không qua `.env`) vì
chỉ dùng khi script chạy **trong** container Airflow — không cần bạn tự cấu hình.

### 3. Cài Python dependencies (cho script host)
```bash
uv sync
```
Lệnh này đọc đúng `pyproject.toml`/`uv.lock`, tạo `.venv/` với các package cần thiết (pandas,
scikit-learn, psycopg2-binary, openpyxl, matplotlib).

---

## E. Khởi chạy dự án

### Lần đầu tiên (từ máy sạch)

**1. Khởi động toàn bộ hạ tầng bằng Docker Compose**
```bash
docker compose up -d --build
```
Lệnh này tự khởi động đúng thứ tự (Postgres → Airflow init → Airflow webserver/scheduler;
Superset, FastAPI độc lập) nhờ `depends_on`/`healthcheck` đã khai báo trong
`docker-compose.yml` — **đã kiểm thử thật**: toàn bộ 7 container chuyển sang `healthy` trong
dưới 2 phút từ trạng thái sạch (Airflow cài thêm `openpyxl` lúc khởi động nên có thể chậm hơn
vài chục giây ở lần đầu — xem mục F).

**2. Kiểm tra trạng thái container**
```bash
docker compose ps
```
Cả 7 dòng (`postgres-source`, `postgres-dw`, `postgres-airflow`, `airflow-webserver`,
`airflow-scheduler`, `fastapi`, `superset`) phải ở trạng thái `healthy`.

| Service | URL/Port trên host | Đăng nhập |
|---|---|---|
| FastAPI | http://localhost:8000 (`/health`, `/products`) | — |
| Airflow UI | http://localhost:8081 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` trong `.env` |
| Superset UI | http://localhost:8088 | `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` trong `.env` |
| Postgres nguồn | `localhost:5432` (db `retail_source`) | user/pass trong `.env` (`PG_SOURCE_*`) |
| Postgres DW | `localhost:5433` (db `retail_dw`) | user/pass trong `.env` (`PG_DW_*`) |

> Airflow UI dùng cổng **8081** (không phải 8080 mặc định) để tránh xung đột với service khác
> có thể đang chạy trên máy bạn — xem `AIRFLOW_WEBSERVER_PORT` trong `.env`.

**3. Tạo Star Schema trên PostgreSQL DW** (chỉ cần làm 1 lần — script an toàn chạy lại nhiều
lần, dùng `CREATE TABLE IF NOT EXISTS`, không xóa gì):
```bash
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER_trong_.env> -d <PG_DW_DB_trong_.env> -v ON_ERROR_STOP=1 < sql/001_create_star_schema.sql
docker exec khodl-postgres-dw psql -U <PG_DW_USER_trong_.env> -d <PG_DW_DB_trong_.env> -c "\dt"
```
(Giá trị mặc định trong `.env.example`: user `dw_user`, db `retail_dw`.) Kết quả mong đợi:
đúng 5 bảng `dim_date, dim_customer, dim_product, dim_country, fact_sales`.

**4. Chạy toàn bộ pipeline ETL qua Airflow DAG**

DAG `retail_online_ii_etl` đã có sẵn trong `airflow/dags/retail_etl_dag.py`, mặc định ở trạng
thái **tạm dừng (paused)** để tránh tự kích hoạt ngoài ý muốn. Kích hoạt và chạy thử công:
```bash
docker exec khodl-airflow-webserver airflow dags unpause retail_online_ii_etl
docker exec khodl-airflow-webserver airflow dags trigger retail_online_ii_etl
```
Theo dõi tiến trình qua Airflow UI (http://localhost:8081, mục *DAGs* → `retail_online_ii_etl`
→ *Grid*), hoặc qua lệnh:
```bash
docker exec khodl-airflow-webserver airflow dags list-runs -d retail_online_ii_etl
```
Luồng task: `start → extract (api, file, database, chạy song song) → transform → load →
validate → end`. Toàn bộ pipeline (Extract → Transform → Load → Validate) **đã kiểm thử chạy
SUCCESS thật** — thời gian chạy đầy đủ khoảng 3–5 phút (Transform đọc + xử lý ~1 triệu dòng
Excel, Load nạp lại vào Postgres).

Sau khi DAG chạy xong, nên tạm dừng lại để tránh chạy lại ngoài ý muốn (DAG set `schedule=None`
nên không tự chạy lại, nhưng tạm dừng vẫn là thói quen an toàn):
```bash
docker exec khodl-airflow-webserver airflow dags pause retail_online_ii_etl
```

**5. Tạo OLAP view + nạp Data Mining (RFM/K-Means) vào DW + cấu hình Superset**

Sau khi DAG Load xong (`fact_sales` đã có dữ liệu), chạy tiếp (idempotent, chạy lại an toàn):

```bash
# OLAP (Phase 4) - 13 view SQL (roll-up, slice, dice, pivot, KPI)
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -f - < sql/002_olap_views.sql

# Data Mining tu DW (Phase 5.1, 5.5) - RFM + K-Means + bang customer_segment
uv run scripts/mining/rfm_dw.py
uv run scripts/mining/cluster_dw.py
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -f - < sql/003_customer_segment.sql
uv run scripts/mining/load_segments_dw.py

# Superset (Phase 6) - them driver Postgres (1 lan), tao database/dataset/chart/dashboard
docker compose up -d superset
uv run scripts/superset/provision_superset.py
```

Chi tiết từng bước, công thức, số liệu đối chiếu: [`docs/04_olap.md`](docs/04_olap.md),
[`docs/05_rfm_kmeans_dw.md`](docs/05_rfm_kmeans_dw.md), [`docs/06_superset_dashboard.md`](docs/06_superset_dashboard.md).
Sau khi chạy xong, mở `http://localhost:8088` (đăng nhập bằng `SUPERSET_ADMIN_USER`/
`SUPERSET_ADMIN_PASSWORD` trong `.env`) → **Dashboards** → "Online Retail II - Tong quan kinh
doanh" để xem dashboard.

**6. (Mở rộng, Phase 8.1) Market Basket Analysis — luật kết hợp Apriori**

Độc lập với pipeline chính (không nằm trong DAG Airflow, không ảnh hưởng ETL/RFM/K-Means/
Superset — đã kiểm thử hồi quy). Thuật toán Apriori **tự cài đặt** (không one-hot encoding,
không thêm dependency ngoài — xem `docs/08_apriori_mba.md` mục 3).

Chạy **không cần Docker** (đọc snapshot CSV đã có trên máy):
```bash
uv run scripts/mining/apriori_mba.py --source csv       # doc datalake/processed/<ngay moi nhat>/
```
→ Xuất `data/processed/apriori_frequent_itemsets.csv` + `apriori_association_rules.csv`
(**đã kiểm thử thật**: 39.536 hóa đơn, 323 frequent itemset, **101 luật**, 101/101 có `lift>1`).

Chạy **kiến trúc chính thức qua DW** (cần `postgres-dw` đang `healthy` + đã Load dữ liệu ở bước
3–4) + nạp kết quả vào bảng riêng `market_basket_rules` — **đã kiểm thử thật, idempotent**:
```bash
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -v ON_ERROR_STOP=1 < sql/004_market_basket_rules.sql
uv run scripts/mining/apriori_mba.py --source dw         # ket qua giong het --source csv
uv run scripts/mining/load_apriori_dw.py                 # UPSERT vao market_basket_rules
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT count(*) FROM market_basket_rules;"  -- = 101
```

Chạy unit test (19 test, **không cần Docker, không cần dữ liệu thật**):
```bash
uv run scripts/mining/test_apriori_mba.py -v
```
Kết quả mong đợi: `Ran 19 tests ... OK`. Nếu có `FAIL`/`ERROR`, kiểm tra lại
`scripts/mining/apriori_mba.py` chưa bị sửa sai logic.

**Dashboard Superset riêng cho Apriori** (sau khi đã nạp `market_basket_rules`, xem bước ở trên,
và Superset đang `healthy`) — tạo/cập nhật qua cùng script provision đã dùng cho dashboard chính,
**idempotent, không đụng tới dashboard "Online Retail II - Tong quan kinh doanh" đã có**:
```bash
uv run scripts/superset/provision_superset.py   # tao ca 2 dashboard (dashboard chinh + Apriori)
uv run scripts/superset/verify_charts.py        # doi chieu toan bo chart (ca cu lan moi) voi du lieu that
```
→ Dashboard **"Apriori - Luat ket hop san pham"**: `http://localhost:8088/superset/dashboard/2/`
(id có thể khác nếu đã có dashboard khác được tạo trước — script in ra URL chính xác sau khi
chạy). **5 chart**, antecedent/consequent hiển thị **tên sản phẩm thật kèm mã** (không chỉ
StockCode), đã xác minh thật qua `/api/v1/chart/data`, số liệu khớp `psql`:

| Chart | Loại | Nội dung |
|---|---|---|
| Apriori - KPI tong quan | Bảng (aggregate) | Tổng số luật, số luật `lift>1`, confidence/lift trung bình |
| Apriori - Top 10 luat theo Lift | Bảng (aggregate, sort `Lift` giảm dần) | Tên+mã sản phẩm 2 vế, Support, Confidence (%), Lift |
| Apriori - Top 10 luat theo Confidence | Bảng (aggregate, sort `Confidence (%)` giảm dần) | Như trên |
| Apriori - Phan bo Support, Confidence, Lift theo luat | Bảng (aggregate, sort `Lift` tăng dần) | Như trên, toàn bộ 101 luật |
| Apriori - Bang chi tiet luat ket hop | Bảng (aggregate, sort `Lift` giảm dần) | Như trên, toàn bộ 101 luật |

Tên sản phẩm lấy từ view mới `v_market_basket_rules_named` (nối `dim_product.description` theo
StockCode, xem `sql/004_market_basket_rules.sql`) — đã kiểm tra thật: **45/45 StockCode xuất
hiện trong luật đều tra được tên (0 mã "Unknown product")**, view không làm nhân bản luật
(101 dòng view = 101 dòng bảng gốc).

**Lưu ý quan trọng (xem đầy đủ ở `docs/08_apriori_mba.md` mục 12–14):** lượt tạo dashboard đầu
tiên có **lỗi hiển thị thật** (chỉ chart KPI hiện ra) do `order_by_cols` không hoạt động đúng
trong bản Superset này — đã sửa bằng `query_mode: aggregate` + `timeseries_limit_metric`/
`order_desc` (cơ chế đã ổn định ở dashboard chính). Lượt thêm tên sản phẩm sau đó **vô tình tạo
1 chart trùng** do dùng nhầm tên tra cứu cũ — đã phát hiện qua API và xóa ngay (xem mục 14).
**Dashboard chưa được xác nhận bằng mắt** — người dùng cần tải lại trang (hard refresh) tại
`http://localhost:8088/superset/dashboard/2/` và tự kiểm tra.

Chi tiết cách chọn ngưỡng `min_support`/`min_confidence`, toàn bộ số liệu kiểm thử (gồm cả lượt
kiểm thử DW thật với Docker, dashboard Superset, và việc nối tên sản phẩm), thiết kế bảng
`market_basket_rules`/view `v_market_basket_rules_named`, và giới hạn đã biết: xem
[`docs/08_apriori_mba.md`](docs/08_apriori_mba.md).

### Các lần chạy sau (đã có `.env`, dataset, Star Schema)

Chỉ cần `docker compose up -d` (không cần `--build` lại trừ khi sửa code `api/`) rồi trigger
lại DAG như bước 4 nếu muốn chạy lại pipeline — **Load là idempotent** (đã kiểm thử chạy lại
nhiều lần, không tạo dữ liệu trùng), nên chạy lại an toàn bất cứ lúc nào. Các bước 5 (OLAP/
Mining/Superset) cũng idempotent — chạy lại không tạo view/dòng/object trùng.

### Chạy pipeline trực tiếp trên host (không qua Airflow, để debug nhanh)

```bash
uv run scripts/ingest/run_all.py        # Extract: API + File + Database
uv run scripts/transform/transform.py   # Transform: doc dataset goc, ap dung quy tac, xuat datalake/processed/
uv run scripts/load/load.py             # Load: nap vao postgres-dw (idempotent)
```

### Kiểm tra kết quả

```bash
curl http://localhost:8000/health              # {"status":"healthy"}
curl http://localhost:8000/products             # danh sách sản phẩm (JSON), ~5.300 StockCode
cat datalake/metadata/ingest_log.csv            # lịch sử ingest (append-only)
```

Kiểm tra dữ liệu trong PostgreSQL DW:
```bash
docker exec khodl-postgres-dw psql -U <PG_DW_USER_trong_.env> -d <PG_DW_DB_trong_.env> -c "
SELECT 'dim_date' t, count(*) FROM dim_date
UNION ALL SELECT 'dim_customer', count(*) FROM dim_customer
UNION ALL SELECT 'dim_product', count(*) FROM dim_product
UNION ALL SELECT 'dim_country', count(*) FROM dim_country
UNION ALL SELECT 'fact_sales', count(*) FROM fact_sales;"
```
Kết quả mong đợi (đã verify thật, 2026-10-23): `dim_date=604`, `dim_customer=5853`,
`dim_product=4905`, `dim_country=43`, `fact_sales=1003670`.

Kiểm tra OLAP view + bảng `customer_segment`:
```bash
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT count(*) FROM information_schema.views WHERE table_schema='public';"  -- = 13
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT segment_name, count(*) FROM customer_segment GROUP BY segment_name;"   -- tong = 5853
```

Kiểm tra bảng `market_basket_rules` (Phase 8.1, nếu đã chạy bước E.6):
```bash
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT count(*) FROM market_basket_rules;"   -- = 101 (voi nguong mac dinh)
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT antecedent_items, consequent_items, support, confidence, lift FROM market_basket_rules ORDER BY lift DESC LIMIT 5;"
```

Kiểm tra view `v_market_basket_rules_named` (tên sản phẩm, xem `docs/08_apriori_mba.md` mục 13):
```bash
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT count(*) FROM v_market_basket_rules_named;"   -- = 101, phai bang market_basket_rules (khong nhan ban)
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT antecedent_label, consequent_label, confidence_pct, lift FROM v_market_basket_rules_named ORDER BY lift DESC LIMIT 5;"
```

Kiểm tra bảng `source_customers` trong Postgres nguồn:
```bash
docker exec khodl-postgres-source psql -U <PG_SOURCE_USER_trong_.env> -d <PG_SOURCE_DB_trong_.env> -c "SELECT count(*) FROM source_customers;"
```
(Mặc định: user `source_user`, db `retail_source` — kết quả mong đợi: **5.942** dòng.)

### Chạy test tự động và xác nhận thành công

Dự án hiện có 1 bộ unit test tự động (`unittest` chuẩn thư viện Python, không cần dependency
thêm, không cần Docker/dữ liệu thật) cho module Apriori:
```bash
uv run scripts/mining/test_apriori_mba.py -v
```
**Kết quả thành công:** dòng cuối in `Ran 19 tests in ...s` và `OK` (không có `FAIL`/`ERROR`).

Các phần còn lại của dự án **chưa có bộ unit test riêng** — việc "kiểm thử thành công" dựa trên
đối chiếu số liệu thực tế qua `psql`/`curl` như liệt kê ở mục "Kiểm tra kết quả" phía trên
(ví dụ `fact_sales=1.003.670`, `customer_segment` tổng `5.853`, `market_basket_rules=101`) và
task Airflow `validate` (kiểm tra orphan FK, công thức revenue — tự FAIL nếu sai, xem
`airflow/dags/retail_etl_dag.py::validate_dw()`). Xem thêm
[`docs/08_apriori_mba.md`](docs/08_apriori_mba.md) mục 8 cho log kiểm thử đầy đủ của Phase 8.1
(bao gồm idempotency DDL/UPSERT và kiểm tra tính hợp lệ dữ liệu trên DW thật).

### Dừng service an toàn (không mất dữ liệu)
```bash
docker compose stop           # dung container, GIU container + du lieu (volume)
# hoac
docker compose down           # xoa container, GIU volume (du lieu Postgres/Airflow/Superset van con)
```
**Không** chạy `docker compose down -v` hoặc xóa volume Docker — lệnh đó sẽ xóa toàn bộ dữ
liệu đã nạp trong `postgres-source`/`postgres-dw`/Airflow/Superset, phải làm lại từ đầu.

---

## F. Khắc phục lỗi thường gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `docker compose up` báo lỗi `...dockerDesktopLinuxEngine...The system cannot find the file specified` | Docker Desktop chưa chạy (daemon chưa khởi động) | Mở ứng dụng Docker Desktop, chờ icon trạng thái "Running", chạy lại lệnh |
| `FileNotFoundError: Khong tim thay dataset tai .../data/raw/online_retail_II.xlsx` (khi gọi `/products`, chạy ingest, hoặc chạy Transform) | Chưa đặt dataset đúng vị trí | Xem lại mục C, đảm bảo đúng tên file `online_retail_II.xlsx` trong `data/raw/` |
| `curl /products` trả `{"detail":"Khong tim thay dataset..."}` (HTTP 503) | FastAPI container không thấy dataset | Kiểm tra `docker-compose.yml` có mount `./data:/app/data:ro` cho service `fastapi`; đảm bảo file tồn tại trên host trước khi `docker compose up` |
| `docker compose up` báo port đã được dùng (`bind: address already in use`) | Một service khác trên máy đang chiếm cổng (ví dụ 5432, 8080, 8088) | Đổi cổng tương ứng trong `.env` (`PG_SOURCE_PORT`, `AIRFLOW_WEBSERVER_PORT`, `SUPERSET_PORT`, `FASTAPI_PORT`, `PG_DW_PORT`), chạy lại `docker compose up -d --build` |
| `docker compose ps` cho thấy Postgres/Airflow chưa `healthy` sau >2 phút | Máy chậm, đang cài `openpyxl` lúc khởi động (Airflow), hoặc khởi động lần đầu | Chờ thêm, xem log cụ thể bằng `docker logs <container_name>` (ví dụ `docker logs khodl-airflow-webserver` sẽ thấy dòng cài `openpyxl` nếu đang chạy) |
| `scripts/ingest/ingest_database.py` hoặc `scripts/load/load.py` lỗi `KeyError: 'PG_SOURCE_PORT'`/`'PG_DW_PORT'` | Chưa tạo `.env` hoặc thiếu biến | Xem lại mục D bước 2 — đảm bảo `.env` tồn tại ở gốc repo và có đủ biến như `.env.example` |
| `uv run ...` báo thiếu package (`ModuleNotFoundError`) | Chưa chạy `uv sync` | Chạy `uv sync` ở gốc repo rồi thử lại |
| Lần gọi `/products` đầu tiên sau khi (re)build container rất chậm (~60–90s) | FastAPI phải đọc + parse Excel 45MB bằng `openpyxl` lần đầu để tạo cache | Không phải lỗi — các lần gọi sau trả về gần như tức thì nhờ cache trong bộ nhớ |
| DAG `retail_online_ii_etl` không chạy sau khi `dags trigger` | DAG mặc định ở trạng thái **paused** | Chạy `airflow dags unpause retail_online_ii_etl` trước khi `dags trigger` (xem mục E bước 4) |
| Task `load` trong DAG lỗi `connection ... failed` tới `postgres-dw` khi tự thêm script mới kết nối DW | Nhầm port: `PG_DW_PORT` (.env) là port publish ra **host** (5433), nhưng kết nối qua tên service Docker nội bộ phải dùng port **container** (luôn là 5432) | Dùng biến `PG_DW_PORT_INTERNAL` (đã đặt sẵn `=5432` trong `docker-compose.yml` cho Airflow) khi viết script mới cần nối `postgres-dw` từ trong container — xem `scripts/load/load.py::_connection_params()` làm mẫu |
| Task trong DAG lỗi `relation "fact_sales" does not exist` | Chưa tạo Star Schema | Chạy lại bước E.3 (`sql/001_create_star_schema.sql`) trước khi chạy DAG |
| `test_connection`/tạo database Superset lỗi `Could not load database driver: PostgresEngineSpec` | Image `apache/superset` gốc chưa có driver `psycopg2` | Đảm bảo `docker-compose.yml` đã có dòng `pip install psycopg2-binary` trong `command` của service `superset` (đã có sẵn trong repo), rồi `docker compose up -d superset` để tạo lại container với lệnh mới |
| `scripts/superset/provision_superset.py` lỗi `KeyError: 'SUPERSET_ADMIN_USER'`/kết nối `Connection refused` tới `localhost:8088` | Thiếu `.env` hoặc Superset chưa `healthy` | Kiểm tra `.env` có đủ biến `SUPERSET_*`; chạy `docker compose ps` đợi `khodl-superset` thành `healthy` trước khi chạy script |
| Chạy `provision_superset.py` báo lỗi khi tạo dataset/chart (ví dụ thiếu view) | Chưa chạy `sql/002_olap_views.sql` hoặc `sql/003_customer_segment.sql` trước | Chạy đúng thứ tự ở mục E bước 5: tạo view/bảng trước, rồi mới chạy script mining và Superset |
| `scripts/mining/apriori_mba.py --source dw` lỗi `KeyError: 'PG_DW_PORT'`/`connection refused` | Chạy `--source dw` nhưng Docker/`postgres-dw` chưa chạy hoặc chưa Load dữ liệu | Dùng `--source csv` nếu không cần Docker (đọc `datalake/processed/<ngày>/`), hoặc chạy đủ bước E.1/E.3/E.4 trước khi dùng `--source dw` |
| `scripts/mining/apriori_mba.py --source csv` lỗi `Khong tim thay thu muc nao co fact_sales.csv` | Chưa chạy Transform lần nào (`datalake/processed/` rỗng) | Chạy `uv run scripts/transform/transform.py` trước (xem mục "Chạy pipeline trực tiếp trên host"), hoặc dùng `--input <thư_mục>` trỏ tới `datalake/processed/<ngày>/` có sẵn |
| `load_apriori_dw.py` báo `Chua co .../apriori_association_rules.csv` | Chưa chạy `apriori_mba.py` lần nào | Chạy `uv run scripts/mining/apriori_mba.py --source csv` (hoặc `--source dw`) trước |
| `provision_superset.py` lỗi khi tạo dataset/chart `market_basket_rules`/`Apriori - ...` | Chưa chạy `sql/004_market_basket_rules.sql` + `load_apriori_dw.py` trước (bảng chưa tồn tại hoặc rỗng) | Chạy đủ 3 lệnh ở mục E bước 6 ("kiến trúc chính thức qua DW") trước khi chạy `provision_superset.py` |

---

## G. Hạn chế của dữ liệu và phương pháp

- **3 "nguồn dữ liệu" (API/File/Database) là mô phỏng** từ cùng 1 dataset Online Retail II,
  không phải hệ thống độc lập ngoài đời thực (xem mục A).
- **Không có khóa chung** để join lại 3 nguồn raw trong Data Lake thành 1 dòng giao dịch đầy
  đủ (File thiếu Customer ID/Country, Database chỉ có Customer ID+Country đã khử trùng) —
  Transform đọc trực tiếp dataset gốc, đây là hạn chế kiến trúc đã biết, không phải lỗi.
- **22,77% dòng gốc thiếu Customer ID** — giữ trong `fact_sales` (`customer_key=NULL`, đo đủ
  doanh thu), loại khỏi RFM/phân khúc theo khách hàng.
- **K=4 cho K-Means không phải Silhouette cao nhất** (K=2 cao hơn) — chọn vì lý do nghiệp vụ
  (4 phân khúc diễn giải được: VIP/trung thành/mới-ít tương tác/nguy cơ rời bỏ).
- **segment_name là heuristic xếp hạng tương đối** giữa các cụm, không phải chuẩn ngành — xem
  `docs/05_rfm_kmeans_dw.md` mục 3.
- Dashboard Superset được tạo qua REST API, chưa xác nhận bằng mắt trong trình duyệt (môi
  trường phát triển không có browser) — xem `docs/06_superset_dashboard.md` mục 6.

## H. Tài liệu đầy đủ

| Chủ đề | File |
|---|---|
| Ngữ cảnh, quyết định kỹ thuật, bằng chứng kiểm thử | [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) |
| Checklist từng task, trạng thái, tiêu chí nghiệm thu | [`PROJECT_CHECKLIST.md`](PROJECT_CHECKLIST.md) |
| Đối chiếu kế hoạch vs hiện trạng | [`docs/PROJECT_GAP_ANALYSIS.md`](docs/PROJECT_GAP_ANALYSIS.md) |
| Thiết kế Star Schema + ERD | [`docs/03_star_schema.md`](docs/03_star_schema.md) |
| OLAP (Roll-up/Drill-down, Slice, Dice, Pivot) | [`docs/04_olap.md`](docs/04_olap.md) |
| RFM + K-Means từ DW, bảng `customer_segment` | [`docs/05_rfm_kmeans_dw.md`](docs/05_rfm_kmeans_dw.md) |
| Superset — cấu hình, chart, dashboard | [`docs/06_superset_dashboard.md`](docs/06_superset_dashboard.md) |
| (Mở rộng, Phase 8.1) Market Basket Analysis — Apriori | [`docs/08_apriori_mba.md`](docs/08_apriori_mba.md) |
| Báo cáo tổng hợp, kịch bản demo, Q&A, checklist nộp bài | [`docs/00_bao_cao_do_an.md`](docs/00_bao_cao_do_an.md) |
