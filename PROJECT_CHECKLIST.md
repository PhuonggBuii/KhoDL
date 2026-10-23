# PROJECT CHECKLIST — Phân tích hành vi mua sắm và phân nhóm khách hàng trong cửa hàng bán lẻ

> Đồ án môn Kho dữ liệu và Khai phá dữ liệu — PTIT — Nhóm 3 thành viên.
> Trạng thái file: Kế hoạch & checklist, đã đối chiếu với code thực tế.
> **Đã có code**: khảo sát dữ liệu (0.1), RFM/K-Means (5.1–5.4) chạy thật có kết quả kiểm
> chứng, **Phase 1 (Data Lake & 3 nguồn ingest) đã hoàn thành và kiểm thử thật** (API/File/
> Database + Data Lake + ingest log — xem `scripts/ingest/`). **Hạ tầng Docker Compose (0.2)
> đã dựng và kiểm thử healthy trên 1 máy** (Postgres×3, Airflow, FastAPI, Superset — xem
> `docker-compose.yml`), còn thiếu xác nhận của các thành viên khác. **Phase 2, 3, 4, 6 (ETL
> Airflow/DW/OLAP/dashboard) chưa bắt đầu.** Chi tiết đối chiếu:
> [`docs/PROJECT_GAP_ANALYSIS.md`](docs/PROJECT_GAP_ANALYSIS.md),
> [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) (tài liệu bàn giao đầy đủ).

---

## 1. Mục tiêu, phạm vi, kiến trúc

**Mục tiêu:** Xây dựng hệ thống end-to-end: tích hợp dữ liệu đa nguồn (mô phỏng) → Data Lake → ETL (Airflow) → Data Warehouse (PostgreSQL, Star Schema) → OLAP → Data Mining (RFM + K-Means) → Dashboard (Superset).

**Dataset gốc:** Kaggle *Online Retail II* (Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country).

**Giả định quan trọng (ghi rõ vì không hỏi lại nhiều lần):**
- Ba "nguồn dữ liệu" (API / File / Database) đều được mô phỏng từ cùng một dataset Online Retail II, tách theo chiều dữ liệu (xem mục 3), **không phải ba nguồn độc lập ngoài đời thực**. Báo cáo phải nêu rõ điều này.
- Không tự sinh thêm thuộc tính không có trong dataset (tên khách hàng, tuổi, giới tính, phương thức thanh toán...). Nếu về sau cần dữ liệu giả lập để demo, phải gắn nhãn rõ "dữ liệu mô phỏng" trong code/báo cáo.
- Database nguồn dùng **PostgreSQL** (một instance riêng, khác với DW) để tránh phải cài thêm MySQL — giảm số công nghệ phải vận hành. Có thể đổi sang MySQL nếu nhóm đã quen sẵn, nhưng mặc định chọn PostgreSQL cho đơn giản.
- Docker Compose dùng để chạy PostgreSQL (nguồn + DW), Airflow, Superset, FastAPI trong cùng một mạng cho dễ kết nối. Nếu nhóm thấy cài đặt local đơn giản hơn với năng lực hiện tại thì có thể thay thế, nhưng khuyến nghị Docker Compose vì tránh xung đột version.
- Apriori và Decision Tree là **hạng mục mở rộng**, chỉ làm sau khi Phase 0–6 (MVP) đã xong và còn thời gian.

**Kiến trúc luồng dữ liệu:**

```
Kaggle Online Retail II (CSV gốc)
        │
        ├── chia theo chiều dữ liệu mô phỏng 3 nguồn
        │
   ┌────┴────┬─────────────┬───────────────┐
   │ API      │ File         │ Database      │
   │ (FastAPI)│ (CSV/Excel)  │ (PostgreSQL)  │
   └────┬────┴─────────────┴───────────────┘
        │  (Airflow Extract)
        ▼
   Data Lake (local dir / Docker volume)
   raw/api/ · raw/file/ · raw/database/ · processed/ · metadata/
        │  (Airflow Transform + Load)
        ▼
   PostgreSQL Data Warehouse (Star Schema)
   Fact_Sales + Dim_Date, Dim_Customer, Dim_Product, Dim_Country
        │
   ┌────┴─────────────┐
   ▼                   ▼
 OLAP (SQL)       Data Mining (Scikit-learn: RFM + K-Means)
   │                   │
   └────────┬──────────┘
            ▼
     Apache Superset Dashboard
```

---

## 2. Công nghệ và vai trò

| Công nghệ | Vai trò trong dự án |
|---|---|
| Kaggle Online Retail II | Dataset nguồn duy nhất, mô phỏng 3 luồng ingest |
| Python + Pandas | Khảo sát, làm sạch, transform dữ liệu; script sinh dữ liệu mô phỏng cho 3 nguồn |
| FastAPI | API Service local, giả lập nguồn API trả JSON |
| PostgreSQL (nguồn) | Database nguồn, nạp một phần dữ liệu để ETL truy vấn bằng SQL |
| Data Lake (thư mục local / Docker volume) | Lưu raw data riêng theo từng nguồn + metadata ingest log |
| Apache Airflow | Điều phối, lập lịch, theo dõi trạng thái các DAG ETL (Extract–Transform–Load) |
| PostgreSQL (DW) | Data Warehouse, mô hình Star Schema |
| Scikit-learn | RFM feature engineering, K-Means, Silhouette Score |
| Apache Superset | Dashboard kết nối trực tiếp PostgreSQL DW |
| Docker Compose | Triển khai các service (Postgres x2, Airflow, Superset, FastAPI) cùng lúc, nếu nhóm chọn dùng |

**Không dùng:** Kafka, Debezium, Spark, hoặc công nghệ streaming/phân tán — dataset đủ nhỏ để xử lý batch bằng Pandas/Airflow/Postgres.

---

## 3. Ba luồng ingest (chi tiết dữ liệu)

| Luồng | Cách mô phỏng | Trường dữ liệu | Công cụ ETL extract |
|---|---|---|---|
| **API** | FastAPI local expose endpoint `/products` trả danh sách sản phẩm (StockCode, Description, giá tham chiếu) suy ra từ dataset | StockCode, Description, UnitPrice tham chiếu | Airflow task gọi HTTP, lưu JSON vào `raw/api/` |
| **File** | Đọc trực tiếp CSV/Excel gốc của Kaggle (phần giao dịch) | Invoice, StockCode, Quantity, InvoiceDate, Price | Airflow task đọc file, copy/snapshot vào `raw/file/` |
| **Database** | Nạp Customer ID + Country (và có thể Invoice liên quan) vào PostgreSQL nguồn trước | Customer ID, Country, (Invoice liên kết) | Airflow task chạy SQL SELECT, lưu kết quả vào `raw/database/` |

**Data Lake — cấu trúc thư mục:**
```
datalake/
  raw/api/yyyy-mm-dd/*.json
  raw/file/yyyy-mm-dd/*.csv
  raw/database/yyyy-mm-dd/*.csv
  processed/yyyy-mm-dd/*.parquet (hoặc csv)
  metadata/ingest_log.csv   # nguồn, thời gian, số bản ghi, trạng thái
```

---

## 4. Giai đoạn thực hiện (theo thứ tự, MVP trước)

| Phase | Nội dung | Loại |
|---|---|---|
| 0 | Khảo sát dữ liệu & setup môi trường | MVP |
| 1 | Data Lake & 3 nguồn ingest | MVP |
| 2 | ETL với Airflow | MVP |
| 3 | Data Warehouse (Star Schema) | MVP |
| 4 | OLAP | MVP |
| 5 | Data Mining (RFM + K-Means) | MVP |
| 6 | Dashboard Superset | MVP |
| 7 | Kiểm thử tổng, báo cáo, demo | MVP |
| 8 | Apriori / Decision Tree | Mở rộng (chỉ nếu còn thời gian) |

---

## 5. Checklist chi tiết theo giai đoạn

Ký hiệu trạng thái: `[ ]` Chưa bắt đầu · `[~]` Đang làm · `[x]` Hoàn thành (chỉ tick khi có bằng chứng kiểm thử — file/log/ảnh chụp kết quả).

### Phase 0 — Khảo sát dữ liệu & setup môi trường

- [x] **0.1 Tải và khảo sát dataset Online Retail II**
  - Phụ trách: TV1 | Công cụ: Python, Pandas, Jupyter
  - Đầu vào: File Kaggle gốc (CSV/Excel) — đã có tại `data/raw/online_retail_II.xlsx`
  - Đầu ra: [`scripts/01_explore_data.py`](scripts/01_explore_data.py) + [`docs/01_bao_cao_tien_do.md`](docs/01_bao_cao_tien_do.md) mục 1–2 — đủ số dòng, kiểu dữ liệu, % thiếu, trùng lặp, đơn hủy, Quantity/Price âm/0
  - Tiêu chí nghiệm thu: **Đạt** — đã chạy lại script để xác minh (2026-10-23), số liệu khớp 100% báo cáo (thiếu Customer ID 243,007/22.77%; Quantity≤0: 22,950; Price≤0: 6,207)
  - Phụ thuộc: Không

- [~] **0.2 Thiết lập môi trường dự án**
  - Phụ trách: TV1 (chủ trì) + cả nhóm xác nhận chạy được
  - Công cụ: Docker Compose (hoặc cài local), Python venv
  - Đầu vào: Danh sách service cần (Postgres nguồn, Postgres DW, Airflow, Superset, FastAPI)
  - Đầu ra: [`docker-compose.yml`](docker-compose.yml) — 7 container: `postgres-source`, `postgres-dw`,
    `postgres-airflow` (metadata), `airflow-webserver`, `airflow-scheduler`, `fastapi`, `superset`.
    Mật khẩu/secret key sinh ngẫu nhiên trong `.env` (không commit), mẫu tham khảo ở `.env.example`.
  - **Đã kiểm thử trên máy này (2026-10-23), tất cả `healthy` và trả response thật:**
    - `psql` vào `postgres-source` (db `retail_source`) và `postgres-dw` (db `retail_dw`) qua cả `docker exec` và port host (5432/5433) — OK.
    - `curl http://localhost:8000/health` (FastAPI) → `{"status":"healthy"}` — OK.
    - `curl http://localhost:8081/health` (Airflow) → `metadatabase: healthy`, `scheduler: healthy`; `airflow users list` thấy user `admin` — OK.
    - `curl http://localhost:8088/health` (Superset) → `OK`, trang `/login/` → HTTP 200 — OK.
    - Tổng RAM dùng ~2.2GB/7.67GB cấp cho Docker Desktop, không ảnh hưởng container của dự án khác đang chạy trên máy (`daugiahtpt-*`), không đổi port 8080 đang bị chiếm (Airflow UI dùng port 8081 thay vì 8080).
  - Tiêu chí nghiệm thu: Cả 3 thành viên chạy `docker compose up` thành công trên máy mình, chụp màn hình các UI — **mới xác nhận được trên 1 máy**, còn thiếu xác nhận + ảnh chụp của 2 thành viên còn lại
  - Phụ thuộc: Không

- [~] **0.3 Thống nhất quy tắc xử lý dữ liệu (data cleaning rules)**
  - Phụ trách: TV1 + TV2 + TV3 (họp thống nhất)
  - Đầu vào: Kết quả 0.1
  - Đầu ra: Văn bản ngắn (trong README hoặc phần đầu checklist) liệt kê: cách xử lý null Customer ID, đơn hủy, Quantity/Price âm, trùng lặp; định dạng ngày; đơn vị tiền tệ
  - **Dự thảo quy tắc (đã triển khai và chạy thật trong `scripts/02_clean_data.py`, chờ 3 thành viên xác nhận bằng commit/ghi chú):**
    1. Bỏ dòng trùng lặp hoàn toàn (`drop_duplicates`).
    2. Bỏ dòng thiếu `Customer ID` (không gán được giao dịch cho khách hàng → không dùng cho RFM/segmentation; vẫn giữ nguyên trong dữ liệu gốc, không sửa/impute).
    3. Bỏ đơn hủy (`Invoice` bắt đầu bằng `C`) — là giao dịch trả hàng, không phải doanh thu bán.
    4. Bỏ dòng `Quantity <= 0` hoặc `Price <= 0` — giao dịch lỗi/điều chỉnh, không phải doanh thu thực.
    5. Bỏ `StockCode` không phải sản phẩm thực (phí/điều chỉnh sổ sách): `POST, DOT, D, M, BANK CHARGES, ADJUST, S, B, AMAZONFEE, CRUK, GIFT, DCGS*`.
    6. `TotalPrice = Quantity * Price`; đơn vị tiền tệ giữ nguyên GBP (£) như dataset gốc, không quy đổi.
    7. Định dạng ngày: giữ nguyên `InvoiceDate` gốc (datetime), chuẩn hóa về `YYYY-MM-DD HH:MM:SS` khi nạp Dim_Date (áp dụng ở Phase 3).
    8. Kết quả áp dụng: 776,857/1,067,371 dòng sạch (72.8%), 5,853 khách hàng duy nhất — xem bảng chi tiết từng bước loại bỏ ở `docs/01_bao_cao_tien_do.md` mục 3.
  - Tiêu chí nghiệm thu: Cả 3 thành viên đồng ý bằng văn bản (commit message hoặc ghi chú) trước khi bắt đầu Phase 1 — **còn thiếu bước xác nhận nhóm**, nội dung kỹ thuật đã sẵn sàng ở trên
  - Phụ thuộc: 0.1

### Phase 1 — Data Lake & 3 nguồn ingest

- [x] **1.1 Xây dựng API mock (FastAPI)**
  - Phụ trách: TV1 | Công cụ: FastAPI, Pandas
  - Đầu vào: Danh sách sản phẩm duy nhất trích từ dataset (StockCode, Description, giá)
  - Đầu ra: [`api/app/main.py`](api/app/main.py) + [`api/app/data.py`](api/app/data.py) —
    `GET /products` (list), `GET /products/{stock_code}` (404 nếu không có). Catalog nạp +
    cache 1 lần trong bộ nhớ (lazy, lần gọi đầu), không đọc lại Excel mỗi request. Lỗi thiếu
    dataset trả HTTP 503 thay vì crash.
  - **Đã kiểm thử thật (2026-10-23):** `curl /products` → 200, **5,304 StockCode duy nhất**
    (khớp đếm độc lập `df['StockCode'].nunique()` ngoài API — cùng 5,304); `curl
    /products/85048` → 200 đúng sản phẩm; `curl /products/NOPE123` → 404. Mẫu response đã lưu
    bằng chứng trong `docs/PROJECT_GAP_ANALYSIS.md` mục Phase 1.
  - Tiêu chí nghiệm thu: **Đạt**
  - Phụ thuộc: 0.3 (nội dung kỹ thuật của 0.3 đã sẵn sàng dù chưa có chữ ký nhóm — không chặn vì không cần áp dụng quy tắc làm sạch transaction cho catalog sản phẩm)

- [x] **1.2 Chuẩn bị nguồn File**
  - Phụ trách: TV1
  - Công cụ: Pandas
  - Đầu vào: File CSV/Excel gốc Kaggle
  - Đầu ra: [`scripts/ingest/ingest_file.py`](scripts/ingest/ingest_file.py) → trích
    `Invoice, StockCode, Quantity, InvoiceDate, Price`, lưu snapshot vào
    `datalake/raw/file/<ngày>/transactions_file_source.csv`. **Giữ nguyên dữ liệu gốc, không
    áp dụng quy tắc làm sạch 0.3** (bỏ đơn hủy, bỏ Quantity/Price≤0...) vì nhóm chưa xác nhận
    chính thức — đây là snapshot RAW đúng tinh thần Data Lake.
  - **Đã kiểm thử thật (2026-10-23):** chạy script, đọc **1,067,371 dòng** — khớp 100% số dòng
    khảo sát ở 0.1. Test lỗi: trỏ sang file không tồn tại → script trả exit code 1, ghi log
    `status=failed` kèm thông báo lỗi, không crash.
  - Tiêu chí nghiệm thu: **Đạt**
  - Phụ thuộc: 0.3

- [x] **1.3 Chuẩn bị nguồn Database**
  - Phụ trách: TV1
  - Công cụ: PostgreSQL (nguồn), Pandas + `psycopg2-binary` (mới thêm qua `uv add`, cần thiết
    để kết nối Postgres)
  - Đầu vào: Customer ID, Country từ dataset
  - Đầu ra: [`scripts/ingest/ingest_database.py`](scripts/ingest/ingest_database.py) — tạo
    bảng `source_customers(customer_id PK, country)` trong `postgres-source`, nạp dữ liệu bằng
    `TRUNCATE` + `INSERT` (idempotent, không cần Invoice vì không có mục đích sử dụng rõ ràng).
  - **Đã kiểm thử thật (2026-10-23):** trích **5,942 khách hàng duy nhất** (khớp số liệu khảo
    sát 0.1: "So Customer ID duy nhat: 5,942"). `SELECT count(*)` qua `docker exec psql` trực
    tiếp (không qua script) xác nhận **5,942** dòng, `count(DISTINCT customer_id) = 5,942`.
    Chạy script 3 lần liên tiếp → vẫn 5,942 dòng, không trùng (idempotent xác nhận thật).
  - Tiêu chí nghiệm thu: **Đạt**
  - Phụ thuộc: 0.3

- [x] **1.4 Thiết kế quy ước Data Lake**
  - Phụ trách: TV1 (đề xuất) + TV2 xác nhận (vì ảnh hưởng ETL load vào DW)
  - Đầu vào: Cấu trúc thư mục ở mục 3
  - Đầu ra: `datalake/{raw/api,raw/file,raw/database,processed,metadata}/` — đã tạo và có file
    mẫu thật mỗi loại (xem bằng chứng kiểm thử 1.1–1.3, 1.5). Khóa liên kết: StockCode nối
    API↔File, Customer ID nối Database↔File (đúng quy ước mục 3 của checklist này). Lưu trên
    đĩa host (bind mount, không phải Docker volume nội bộ container) → **không mất khi
    container khởi động lại**, vì script ingest chạy trên host, ghi trực tiếp vào
    `datalake/` của repo.
  - **Lưu ý:** `processed/` còn trống — đúng phạm vi, Phase 1 chưa transform/join 3 nguồn (đó
    là Phase 2.3). `datalake/raw|processed` đã thêm vào `.gitignore` (dữ liệu lớn, tái tạo được
    bằng script), riêng `metadata/ingest_log.csv` được commit làm bằng chứng.
  - Tiêu chí nghiệm thu: **Đạt** — TV2 cần xác nhận lại khóa liên kết trước khi code Load (2.4), chưa thể tự xác nhận trong phiên code
  - Phụ thuộc: 1.1, 1.2, 1.3

- [x] **1.5 Cơ chế ghi nhật ký ingest**
  - Phụ trách: TV1
  - Công cụ: Python (module `scripts/ingest/common.py`), file CSV log append-only
  - Đầu vào: Kết quả ingest từ 3 nguồn
  - Đầu ra: `datalake/metadata/ingest_log.csv` — cột `timestamp_utc, source, status,
    record_count, output_path, error_message`; mỗi lần chạy **bổ sung dòng mới**, không ghi đè
    lịch sử cũ.
  - **Đã kiểm thử thật (2026-10-23):** chạy đủ pipeline (`scripts/ingest/run_all.py`, API →
    File → Database tuần tự) — log có đủ ≥ 3 dòng `success` cho 3 nguồn trong cùng 1 lần chạy
    full pipeline, cộng thêm các dòng `failed` từ 2 bài test lỗi chủ động (dừng tạm container
    `fastapi`; trỏ sai đường dẫn file) — log phản ánh đúng thực tế, không có dòng "thành công
    giả".
  - Tiêu chí nghiệm thu: **Đạt**
  - Phụ thuộc: 1.4

### Phase 2 — ETL với Airflow

- [ ] **2.1 Thiết kế DAG tổng thể**
  - Phụ trách: TV1
  - Công cụ: Airflow
  - Đầu vào: Kết quả Phase 1
  - Đầu ra: Sơ đồ DAG (task Extract API/File/DB → Transform → Load) — có thể vẽ tay/draw.io trước khi code
  - Tiêu chí nghiệm thu: Sơ đồ được TV2 review và xác nhận khớp với thiết kế Star Schema (Phase 3)
  - Phụ thuộc: 1.5, 3.1 (schema phải thống nhất trước khi code Load task)

- [ ] **2.2 Task Extract (3 nguồn)**
  - Phụ trách: TV1
  - Công cụ: Airflow PythonOperator
  - Đầu vào: API FastAPI, file nguồn, Postgres nguồn
  - Đầu ra: Dữ liệu thô ghi vào `raw/` theo đúng quy ước 1.4
  - Tiêu chí nghiệm thu: Chạy DAG, 3 task Extract SUCCESS trên Airflow UI, file xuất hiện đúng thư mục
  - Phụ thuộc: 2.1

- [ ] **2.3 Task Transform**
  - Phụ trách: TV1 (phối hợp TV2 để khớp schema đích)
  - Công cụ: Pandas trong Airflow task
  - Đầu vào: `raw/*`
  - Đầu ra: Dữ liệu sạch trong `processed/` (đã join 3 nguồn theo khóa, tính Revenue = Quantity * Price, loại/tách đơn hủy, chuẩn hóa ngày)
  - Tiêu chí nghiệm thu: So sánh số dòng trước/sau transform có log rõ ràng (bao nhiêu dòng bị loại và lý do)
  - Phụ thuộc: 2.2, 0.3

- [ ] **2.4 Task Load vào PostgreSQL DW**
  - Phụ trách: TV1 (phối hợp TV2)
  - Công cụ: Airflow + psycopg2/SQLAlchemy
  - Đầu vào: `processed/*`
  - Đầu ra: Dữ liệu nạp vào Fact/Dim tables theo Star Schema (3.1)
  - Tiêu chí nghiệm thu: Query `SELECT count(*)` ở DW khớp số dòng processed; không có dòng trùng khi chạy lại DAG
  - Phụ thuộc: 2.3, 3.1

- [ ] **2.5 Cơ chế chạy lại không trùng (idempotency ở mức cần thiết)**
  - Phụ trách: TV1
  - Công cụ: Airflow (DAG param ngày chạy / execution_date), SQL (upsert hoặc xóa-nạp lại theo batch/ngày)
  - Đầu vào: DAG đã chạy ở 2.4
  - Đầu ra: Quy tắc đơn giản: mỗi lần Load xóa dữ liệu cùng batch_id/ngày rồi nạp lại (không cần CDC phức tạp)
  - Tiêu chí nghiệm thu: Chạy DAG 2 lần liên tiếp với cùng input, số dòng trong Fact_Sales không tăng gấp đôi
  - Phụ thuộc: 2.4

### Phase 3 — Data Warehouse (Star Schema)

- [ ] **3.1 Thiết kế Star Schema**
  - Phụ trách: TV2 (chủ trì, phối hợp TV1)
  - Đầu vào: Kết quả khảo sát 0.1, quy tắc 0.3
  - Đầu ra: Tài liệu schema: `Fact_Sales` (grain: 1 dòng = 1 dòng hàng trong 1 hóa đơn; khóa ngoại tới Dim_Date, Dim_Customer, Dim_Product, Dim_Country; measures: Quantity, UnitPrice, Revenue), `Dim_Date`, `Dim_Customer`, `Dim_Product`, `Dim_Country` (chỉ tạo dimension khi có dữ liệu/nhu cầu thực)
  - Tiêu chí nghiệm thu: Sơ đồ ERD + DDL script (CREATE TABLE) được cả 3 thành viên xác nhận trước khi TV1 code Load task
  - Phụ thuộc: 0.3

- [ ] **3.2 Tạo DDL trên PostgreSQL DW**
  - Phụ trách: TV2
  - Công cụ: PostgreSQL, SQL script
  - Đầu vào: 3.1
  - Đầu ra: Script `.sql` tạo toàn bộ bảng Fact/Dim + ràng buộc khóa chính/ngoại
  - Tiêu chí nghiệm thu: Chạy script không lỗi, `\dt` liệt kê đủ bảng
  - Phụ thuộc: 3.1

- [ ] **3.3 Xác thực dữ liệu sau khi nạp lần đầu**
  - Phụ trách: TV2 (phối hợp TV1)
  - Công cụ: SQL
  - Đầu vào: Dữ liệu đã Load ở 2.4
  - Đầu ra: Bộ query kiểm tra: tổng Revenue khớp giữa Fact_Sales và dữ liệu processed gốc, không có khóa ngoại NULL bất thường
  - Tiêu chí nghiệm thu: Tất cả query kiểm tra trả kết quả khớp (ghi lại trong file test log)
  - Phụ thuộc: 2.4, 3.2

### Phase 4 — OLAP

- [ ] **4.1 Viết query Roll-up/Drill-down (theo thời gian)**
  - Phụ trách: TV2 | Công cụ: SQL (GROUP BY ROLLUP hoặc GROUP BY nhiều cấp)
  - Đầu vào: DW đã có dữ liệu
  - Đầu ra: Query doanh thu theo Năm → Quý → Tháng → Ngày
  - Tiêu chí nghiệm thu: Query chạy đúng, kết quả tổng khớp con số tổng Revenue toàn bộ
  - Phụ thuộc: 3.3

- [ ] **4.2 Viết query Slice/Dice (sản phẩm, quốc gia, khách hàng)**
  - Phụ trách: TV2
  - Đầu ra: Query doanh thu theo sản phẩm, theo quốc gia, kết hợp 2 chiều (dice)
  - Tiêu chí nghiệm thu: Kết quả đối chiếu thủ công với vài dòng dữ liệu gốc (spot-check)
  - Phụ thuộc: 3.3

- [ ] **4.3 Viết query Pivot (ma trận thời gian x quốc gia hoặc sản phẩm)**
  - Phụ trách: TV2
  - Đầu ra: Query dùng `crosstab` (tablefunc extension) hoặc pivot bằng SQL CASE WHEN
  - Tiêu chí nghiệm thu: Bảng pivot hiển thị đúng, tổng theo hàng/cột khớp 4.1/4.2
  - Phụ thuộc: 4.1, 4.2

- [ ] **4.4 Tổng hợp bộ query OLAP để dùng cho Dashboard**
  - Phụ trách: TV2
  - Đầu ra: File `olap_queries.sql` tập hợp tất cả query đã kiểm thử, sẵn sàng làm nguồn cho Superset chart
  - Tiêu chí nghiệm thu: File chạy được nguyên khối không lỗi
  - Phụ thuộc: 4.1, 4.2, 4.3

### Phase 5 — Data Mining (RFM + K-Means)

- [~] **5.1 Tính toán đặc trưng RFM**
  - Phụ trách: TV3 | Công cụ: Python, Pandas, SQL (đọc từ DW)
  - Đầu vào: Fact_Sales + Dim_Customer đã nạp
  - Đầu ra: [`scripts/03_rfm.py`](scripts/03_rfm.py) → `data/processed/rfm.csv`, 5,853 khách hàng, R/F/M không âm (đã chạy lại xác minh 2026-10-23, khớp 100% báo cáo cũ)
  - Tiêu chí nghiệm thu: Đạt về mặt số liệu, **nhưng input hiện đọc từ `data/processed/transactions_clean.csv`, chưa đọc từ DW** (DW chưa tồn tại — Phase 3 chưa làm). Đây là prototype/baseline, cần làm lại bước đọc dữ liệu khi Phase 3 xong — xem `docs/PROJECT_GAP_ANALYSIS.md` mục 3.1
  - Phụ thuộc: 3.3 (chưa đáp ứng — xem ghi chú trên)

- [x] **5.2 Chuẩn hóa đặc trưng**
  - Phụ trách: TV3 | Công cụ: Scikit-learn (StandardScaler hoặc log-transform trước khi scale do Monetary lệch phải)
  - Đầu vào: 5.1
  - Đầu ra: [`scripts/04_kmeans.py`](scripts/04_kmeans.py) — `log1p(Frequency)`, `log1p(Monetary)` + `StandardScaler`
  - Tiêu chí nghiệm thu: Đạt (đã chạy thật, có trong `docs/01_bao_cao_tien_do.md` mục 5)
  - Phụ thuộc: 5.1

- [x] **5.3 Thử nhiều K và đánh giá Silhouette Score**
  - Phụ trách: TV3 | Công cụ: Scikit-learn K-Means, silhouette_score
  - Đầu vào: 5.2
  - Đầu ra: Bảng Silhouette Score K=2..10 + [`docs/silhouette_scores.png`](docs/silhouette_scores.png); chọn K=4 (lý do nghiệp vụ, không chỉ silhouette cao nhất — K=2 cao nhất nhưng ít ý nghĩa phân khúc)
  - Tiêu chí nghiệm thu: Đạt — có số liệu từng K và lý giải rõ lựa chọn K=4 trong `docs/01_bao_cao_tien_do.md` mục 5
  - Phụ thuộc: 5.2

- [x] **5.4 Gán cụm và diễn giải đặc điểm từng cụm**
  - Phụ trách: TV3
  - Đầu vào: Model K-Means đã chọn ở 5.3
  - Đầu ra: `data/processed/customer_segments.csv` (nhãn K=2 và K=4); mô tả 4 cụm (VIP, trung thành, mới/ít tương tác, nguy cơ rời bỏ) với R/F/M trung bình từng cụm
  - Tiêu chí nghiệm thu: Đạt — bảng mô tả đầy đủ trong `docs/01_bao_cao_tien_do.md` mục 5, số liệu không mâu thuẫn
  - Phụ thuộc: 5.3

- [ ] **5.5 Nạp kết quả phân cụm vào DW**
  - Phụ trách: TV3 (phối hợp TV2 để thêm cột/bảng)
  - Công cụ: SQL/Pandas to_sql
  - Đầu vào: 5.4
  - Đầu ra: Bảng `Dim_Customer` có thêm cột `cluster_label` hoặc bảng `Customer_Segment` riêng
  - Tiêu chí nghiệm thu: Superset có thể query được cột cluster
  - Phụ thuộc: 5.4, 3.2

### Phase 6 — Dashboard (Superset)

- [ ] **6.1 Kết nối Superset với PostgreSQL DW**
  - Phụ trách: TV2 | Công cụ: Superset
  - Tiêu chí nghiệm thu: Superset kết nối thành công, duyệt được danh sách bảng DW
  - Phụ thuộc: 3.3

- [ ] **6.2 Chart doanh thu theo thời gian/sản phẩm/quốc gia**
  - Phụ trách: TV2
  - Đầu vào: 4.4 (OLAP queries) làm dataset/SQL Lab query
  - Đầu ra: 3 chart tương ứng
  - Tiêu chí nghiệm thu: Chart hiển thị đúng số liệu khớp OLAP đã kiểm thử
  - Phụ thuộc: 6.1, 4.4

- [ ] **6.3 Chart số lượng khách hàng và tỷ lệ từng cụm**
  - Phụ trách: TV2 (phối hợp TV3 cung cấp ý nghĩa cụm)
  - Đầu vào: 5.5
  - Đầu ra: Pie/Bar chart tỷ lệ cụm
  - Tiêu chí nghiệm thu: Tổng % các cụm = 100%, khớp số lượng khách hàng ở 5.4
  - Phụ thuộc: 6.1, 5.5

- [ ] **6.4 Chart so sánh chi tiêu/tần suất theo cụm**
  - Phụ trách: TV2 (phối hợp TV3)
  - Đầu ra: Bar chart R/F/M trung bình theo cụm
  - Tiêu chí nghiệm thu: Số liệu khớp bảng mô tả cụm ở 5.4
  - Phụ thuộc: 6.3

- [ ] **6.5 Gộp thành Dashboard hoàn chỉnh**
  - Phụ trách: TV2
  - Đầu ra: 1 Superset Dashboard chứa toàn bộ chart ở 6.2–6.4, có filter theo thời gian/quốc gia
  - Tiêu chí nghiệm thu: Dashboard load không lỗi, filter hoạt động đúng
  - Phụ thuộc: 6.2, 6.3, 6.4

### Phase 7 — Kiểm thử tổng, báo cáo, demo (bắt buộc trước khi nộp)

- [ ] **7.1 Kiểm tra số bản ghi qua từng bước (end-to-end count check)**
  - Phụ trách: TV1 (chủ trì) + TV2 + TV3 xác nhận phần của mình
  - Đầu ra: Bảng đối chiếu: raw → processed → DW → RFM → Superset, số dòng/khách hàng từng bước
  - Tiêu chí nghiệm thu: Không có chênh lệch bất thường không giải thích được
  - Phụ thuộc: Tất cả Phase 1–6

- [ ] **7.2 Đối chiếu kết quả SQL, mô hình và dashboard**
  - Phụ trách: Cả nhóm
  - Đầu ra: Checklist đối chiếu: số liệu OLAP = số liệu hiển thị trên Superset; số cụm K-Means = số cụm hiển thị
  - Tiêu chí nghiệm thu: Tất cả mục khớp, có ảnh chụp màn hình minh chứng
  - Phụ thuộc: 7.1

- [ ] **7.3 Chuẩn bị sơ đồ kiến trúc và hướng dẫn chạy (README)**
  - Phụ trách: TV1 (hạ tầng) + TV2 (DW/OLAP) + TV3 (mining) viết phần của mình
  - Đầu ra: `README.md` hướng dẫn cài đặt, chạy `docker compose up`, trigger DAG, mở Superset
  - Tiêu chí nghiệm thu: Một người ngoài nhóm có thể làm theo README và chạy được hệ thống
  - Phụ thuộc: 7.2

- [ ] **7.4 Viết báo cáo cuối và kịch bản demo**
  - Phụ trách: Cả nhóm
  - Đầu ra: Báo cáo đồ án + kịch bản demo (mục 7 bên dưới)
  - Tiêu chí nghiệm thu: Báo cáo nêu rõ giả định mô phỏng 3 nguồn, có số liệu kiểm thử thực tế (không chỉ lý thuyết)
  - Phụ thuộc: 7.3

### Phase 8 — Mở rộng (chỉ làm nếu còn thời gian, không ảnh hưởng Phase 0–7)

- [ ] **8.1 Market Basket Analysis (Apriori)**
  - Phụ trách: TV3 | Công cụ: mlxtend hoặc tự cài thêm thư viện tương đương
  - Đầu ra: Luật kết hợp sản phẩm thường mua cùng nhau
  - Tiêu chí nghiệm thu: Có ít nhất vài luật với support/confidence hợp lý, không làm chậm/crash pipeline chính
  - Phụ thuộc: Phase 5 hoàn tất

- [ ] **8.2 Decision Tree dự đoán phân khúc/khả năng rời bỏ**
  - Phụ trách: TV3 | Công cụ: Scikit-learn
  - Đầu ra: Model đơn giản + đánh giá accuracy/confusion matrix
  - Tiêu chí nghiệm thu: Model chạy được, có đánh giá, không bắt buộc đạt độ chính xác cao
  - Phụ thuộc: 8.1 (không bắt buộc, có thể làm độc lập sau Phase 5)

---

## 6. Phân công và điểm bàn giao giữa 3 thành viên

| Ai | Phụ trách chính | Bàn giao cho | Nội dung bàn giao |
|---|---|---|---|
| TV1 | Khảo sát dữ liệu, 3 nguồn ingest, Data Lake, Airflow ETL | TV2 | Dữ liệu `processed/` đã sạch + thống nhất schema đích trước khi code Load (sau 2.1/3.1) |
| TV2 | Thiết kế DW (cùng TV1), OLAP, Superset | TV3 | DW đã có dữ liệu (sau 3.3) để TV3 tính RFM; nhận lại `cluster_label` (sau 5.5) để lên chart |
| TV3 | RFM, K-Means, diễn giải cụm, trực quan hóa cụm | TV2 | Bảng kết quả phân cụm (sau 5.4) để nạp vào DW/Superset |

**Điểm đồng bộ bắt buộc (không làm song song trước các mốc này):**
1. Trước Phase 1: thống nhất quy tắc xử lý dữ liệu (0.3).
2. Trước khi TV1 code task Load (2.4): TV2 phải chốt xong Star Schema (3.1–3.2).
3. Trước khi TV3 tính RFM (5.1): DW phải có dữ liệu đã xác thực (3.3).
4. Trước khi TV2 làm chart cụm (6.3–6.4): TV3 phải bàn giao bảng cụm (5.4–5.5).

Sau mốc 1, TV1 có thể làm Phase 1–2 song song với việc TV2 thiết kế Phase 3 (không phụ thuộc lẫn nhau tới bước Load). Sau mốc 2–3, TV2 (OLAP/Dashboard khung) và TV3 (RFM/K-Means) có thể làm song song, chỉ hội tụ lại ở Phase 6.

---

## 7. Mốc nghiệm thu (milestones)

- [ ] **M1 — Dữ liệu sẵn sàng:** Phase 0 + Phase 1 hoàn tất, có ingest log chứng minh 3 nguồn hoạt động.
- [ ] **M2 — Pipeline ETL chạy ổn định:** Phase 2 hoàn tất, DAG chạy lại được không trùng dữ liệu.
- [ ] **M3 — Data Warehouse đúng:** Phase 3 hoàn tất, số liệu xác thực khớp.
- [ ] **M4 — Phân tích hoàn chỉnh:** Phase 4 + Phase 5 hoàn tất, có kết quả cụm diễn giải được.
- [ ] **M5 — Dashboard demo được:** Phase 6 hoàn tất.
- [ ] **M6 — Sẵn sàng nộp/bảo vệ:** Phase 7 hoàn tất (báo cáo + demo + README).

### Checklist kiểm thử cuối (trước khi nộp)
- [ ] `docker compose up` (hoặc hướng dẫn chạy) hoạt động từ máy sạch
- [ ] Trigger DAG Airflow chạy thành công từ đầu đến cuối, không lỗi task
- [ ] Chạy lại DAG lần 2 không làm trùng dữ liệu trong DW
- [ ] Tổng Revenue trong DW khớp tổng tính tay từ dataset gốc (sau khi áp quy tắc làm sạch)
- [ ] Số lượng khách hàng mỗi cụm + mô tả cụm hiển thị đúng trên Superset
- [ ] Toàn bộ chart Superset load không lỗi, số liệu khớp OLAP query gốc
- [ ] README cho phép người ngoài chạy lại toàn bộ hệ thống

---

## 8. Rủi ro chính và phương án xử lý

| Rủi ro | Ảnh hưởng | Cách xử lý | Phương án tối giản nếu thiếu thời gian |
|---|---|---|---|
| Dataset Online Retail II có nhiều dữ liệu bẩn (null Customer ID ~25%, đơn hủy) | Sai lệch doanh thu, cụm không chính xác | Thống nhất quy tắc xử lý từ Phase 0 (0.3), log rõ số dòng loại bỏ | Loại thẳng các dòng null Customer ID khỏi RFM/Mining, giữ lại trong Fact_Sales ở dạng "Unknown Customer" |
| Airflow khó cài/debug lần đầu (3 sinh viên chưa quen) | Chậm tiến độ Phase 2 | TV1 làm trước 1 DAG đơn giản (chỉ 1 nguồn) để quen, sau đó mở rộng 3 nguồn | Nếu Airflow quá khó, chạy ETL bằng Python script + cron/schedule thủ công, vẫn giữ cấu trúc Extract-Transform-Load, ghi rõ trong báo cáo là giới hạn thời gian |
| Chưa thống nhất schema giữa TV1 và TV2 gây phải sửa lại Load task | Mất thời gian làm lại | Chốt Star Schema (3.1) bằng văn bản trước khi TV1 code (xem mốc đồng bộ #2) | Dùng schema tối giản nhất: chỉ Fact_Sales + Dim_Date + Dim_Customer + Dim_Product, bỏ Dim_Country nếu quá gấp |
| K-Means không ra cụm rõ ràng (Silhouette thấp) | Khó diễn giải kinh doanh | Thử nhiều K, thử log-transform Monetary trước khi scale | Chọn K=3 hoặc K=4 cố định với lý do nghiệp vụ (ví dụ VIP/Thường/Rời bỏ) thay vì tối ưu tuyệt đối |
| Superset cấu hình phức tạp (secret key, cache...) | Mất thời gian setup | Dùng Docker Compose với file cấu hình mẫu chuẩn, test sớm ở Phase 0 (0.2) | Nếu Superset quá vướng, demo bằng Python (matplotlib/plotly) thay thế tạm, nêu rõ giới hạn trong báo cáo |
| 3 thành viên làm song song nhưng dữ liệu/định dạng lệch nhau | Phải sửa lại nhiều chỗ | Tuân thủ nghiêm các điểm đồng bộ ở mục 6, không bắt đầu phần phụ thuộc khi chưa nhận bàn giao | Họp nhanh (15-30 phút) mỗi khi hoàn thành 1 Phase trước khi Phase tiếp theo bắt đầu |
| Hết thời gian trước Apriori/Decision Tree | Thiếu hạng mục mở rộng | Phase 8 làm sau cùng, có thể bỏ hoàn toàn | Không làm Phase 8, báo cáo nêu "định hướng mở rộng" thay vì kết quả thực tế |

---

## 9. Kịch bản demo cuối (dựa trên luồng dữ liệu thực tế)

1. Giới thiệu kiến trúc tổng thể (hình ở mục 1), nêu rõ giả định mô phỏng 3 nguồn từ 1 dataset.
2. Mở Airflow UI, trigger DAG ETL trực tiếp (hoặc show lần chạy gần nhất) → chỉ 3 task Extract chạy song song → Transform → Load.
3. Mở psql/DBeaver vào PostgreSQL DW, chạy 1–2 query OLAP mẫu (rollup theo thời gian, slice theo quốc gia) trực tiếp để chứng minh dữ liệu thật.
4. Mở notebook/script Data Mining: show bảng RFM, biểu đồ Silhouette Score theo K, kết quả cụm cuối cùng và diễn giải từng cụm.
5. Mở Superset Dashboard: đi qua từng chart (doanh thu theo thời gian/sản phẩm/quốc gia, tỷ lệ cụm khách hàng, so sánh chi tiêu/tần suất theo cụm), đối chiếu nhanh 1 con số với query SQL đã chạy ở bước 3.
6. Chạy lại DAG lần 2 trước mặt người chấm để chứng minh không nạp trùng dữ liệu (idempotency).
7. Tổng kết: nhắc lại giới hạn (dữ liệu mô phỏng, phạm vi MVP đã hoàn thành, phần mở rộng nếu có).

---

## 10. Bước đầu tiên nhóm cần thực hiện

1. Tải dataset Kaggle Online Retail II về máy.
2. Cả nhóm họp thống nhất Phase 0.3 (quy tắc xử lý dữ liệu) trước khi code bất cứ thứ gì.
3. TV1 bắt đầu task 0.1 (khảo sát dữ liệu) song song với TV2 phác thảo sơ bộ Star Schema (3.1) dựa trên các cột đã biết của dataset — để hai việc này gặp nhau sớm ở quyết định schema cuối cùng.
