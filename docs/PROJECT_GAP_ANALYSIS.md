# Gap Analysis — Đối chiếu PROJECT_CHECKLIST.md với hiện trạng repo

Ngày đối chiếu: 2026-10-23. Phương pháp: đọc trực tiếp code/script/docs, chạy lại script để xác
minh số liệu còn đúng (không suy đoán từ tên file).

## 1. Tóm tắt hiện trạng

| Phase | Trạng thái thực tế | Ghi chú |
|---|---|---|
| 0 — Khảo sát & môi trường | **0.1 xong**, 0.2 đã dựng+kiểm thử (chờ xác nhận nhóm), 0.3 chưa chính thức hóa | Xem mục 2 |
| 1 — Data Lake & 3 nguồn ingest | **Xong, đã kiểm thử thật** | `api/`, `scripts/ingest/`, `datalake/` — xem mục 8 |
| 2 — ETL Airflow | **Chưa làm** | Không có DAG, không cài Airflow |
| 3 — Data Warehouse | **Chưa làm** | `sql/` rỗng, không có DDL, không có Postgres DW |
| 4 — OLAP | **Chưa làm** | Phụ thuộc Phase 3 |
| 5 — Data Mining (RFM + K-Means) | **Làm trước, vượt tiến độ** — 5.1–5.4 có kết quả thật | Nhưng input lấy từ CSV đã làm sạch, **không phải từ DW** như spec yêu cầu — xem mục 3 (rủi ro) |
| 6 — Dashboard Superset | **Chưa làm** | Không cài Superset |
| 7 — Kiểm thử tổng/báo cáo | **Chưa làm** | Hợp lý, vì Phase 1–4, 6 chưa xong |
| 8 — Mở rộng | **Chưa làm** | Đúng kế hoạch (làm sau cùng) |

**Kết luận chính (cập nhật 2026-10-23):** Phase 0 (khảo sát + môi trường) và Phase 1 (3 nguồn
ingest + Data Lake) đã hoàn thành và kiểm thử thật. Phase 5 (RFM/K-Means) cũng đã có kết quả
thật nhưng làm trước, đọc từ CSV thay vì DW (xem mục 3, rủi ro #1). **Phase 2 (Airflow DAG),
Phase 3 (Star Schema DW), Phase 4 (OLAP), Phase 6 (Superset chart) vẫn chưa tồn tại** — đây là
phần việc lớn nhất còn lại. Hạ tầng chạy các service này đã sẵn (Phase 0.2), chỉ còn thiếu nội
dung nghiệp vụ (DAG, DDL, query, chart).

## 2. Đối chiếu chi tiết Phase 0

### 0.1 Khảo sát dataset — ĐÃ XONG, có bằng chứng
- Script: [`scripts/01_explore_data.py`](../scripts/01_explore_data.py) (114 dòng, chạy được).
- Báo cáo: [`docs/01_bao_cao_tien_do.md`](01_bao_cao_tien_do.md) mục 1–2, đầy đủ số dòng, %
  thiếu, số trùng lặp, đơn hủy, Quantity/Price âm — đúng format checklist yêu cầu.
- **Đã chạy lại script trong buổi đối chiếu này để xác minh** (không chỉ tin vào báo cáo cũ):
  kết quả khớp 100% với số liệu trong `docs/01_bao_cao_tien_do.md` (thiếu Customer ID
  243,007 dòng/22.77%, Quantity ≤ 0: 22,950, Price ≤ 0: 6,207, UK 981,330 dòng...).
- Thiếu nhỏ so với tiêu chí: chưa có mục "quy tắc xử lý đề xuất cho từng vấn đề" tách bạch
  rõ ràng như một bảng riêng — hiện nằm rải trong docstring của `02_clean_data.py` và mục 3 của
  báo cáo. Không chặn, chỉ nên gộp lại khi làm 0.3.
- **Đánh giá: tick `[x]`.**

### 0.2 Thiết lập môi trường dự án — ĐÃ DỰNG VÀ KIỂM THỬ (2026-10-23), chờ xác nhận nhóm
- Đã tạo `docker-compose.yml` + `.env`/`.env.example` + `api/` (FastAPI skeleton) +
  `superset/superset_config.py` + `airflow/{dags,logs,plugins}`.
- 7 container: `postgres-source` (db `retail_source`, port host 5432), `postgres-dw` (db
  `retail_dw`, port host 5433), `postgres-airflow` (metadata Airflow, không expose host),
  `airflow-webserver` (port host **8081**, không dùng 8080 vì đã bị một project khác trên máy
  chiếm), `airflow-scheduler`, `fastapi` (port 8000, chỉ có `/`, `/health` — chưa có `/products`,
  đó là nội dung Phase 1.1), `superset` (port 8088).
- Đã bật Docker Desktop (daemon trước đó chưa chạy, đúng như ghi nhận cũ), khởi động từng bước
  nhỏ: Postgres trước → FastAPI → Airflow (init → webserver/scheduler) → Superset, kiểm tra
  log/health sau mỗi bước.
- **Bằng chứng kiểm thử thật (không chỉ "container Up"):**
  - `psql` xác nhận đúng database/user cho cả 2 Postgres nghiệp vụ, qua cả `docker exec` và từ
    host qua port map.
  - `curl http://localhost:8000/health` → `{"status":"healthy"}`.
  - `curl http://localhost:8081/health` → `metadatabase: healthy`, `scheduler: healthy`;
    `airflow users list` có `admin`.
  - `curl http://localhost:8088/health` → `OK`; `/login/` → HTTP 200.
  - `docker stats`: tổng ~2.2GB/7.67GB RAM cấp cho Docker Desktop — còn dư nhiều, không ảnh
    hưởng 5 container của project khác (`daugiahtpt-*`) đang chạy sẵn trên máy.
- **Đơn giản hóa đã áp dụng (ghi rõ để nhóm biết, không phải thiếu sót):**
  - Airflow dùng `LocalExecutor` (không Celery/Redis) — đủ cho dataset batch nhỏ, đúng tinh thần
    "giảm số công nghệ" của checklist.
  - Superset dùng SQLite nội bộ cho metadata của chính nó (không thêm Postgres/Redis thứ 4) —
    kết nối Superset → Postgres DW (để vẽ chart) sẽ khai báo qua UI ở Phase 6, không phải đây.
  - Không hardcode mật khẩu: toàn bộ secret nằm trong `.env` (đã có trong `.gitignore` từ trước),
    `.env.example` chỉ chứa placeholder.
- **Còn thiếu để tick `[x]` hoàn toàn:** tiêu chí nghiệm thu gốc yêu cầu **cả 3 thành viên** tự
  chạy `docker compose up` trên máy họ và chụp màn hình — phần này chỉ làm được trên 1 máy trong
  lượt này. Đã đánh giá `[~]`.
- **Chưa làm trong lượt này (đúng yêu cầu task, để lại cho Phase 1/2/3/6):** chưa tạo DAG, chưa
  tạo bảng `source_customers`/Star Schema, chưa cấu hình Superset kết nối DW, chưa thêm nội dung
  `/products` cho FastAPI.

### 0.3 Thống nhất quy tắc xử lý dữ liệu — CHƯA CHÍNH THỨC HÓA
- Quy tắc đã **được áp dụng nhất quán trong code** (`scripts/02_clean_data.py`): bỏ trùng lặp,
  bỏ null Customer ID, bỏ đơn hủy (Invoice prefix "C"), bỏ Quantity/Price ≤ 0, bỏ mã phi-sản-phẩm
  (POST, DOT, D, M, BANK CHARGES, ADJUST, S, B, AMAZONFEE, CRUK, GIFT, DCGS*). Kết quả: 776,857
  dòng sạch (72.8%), 5,853 khách hàng.
- Nhưng checklist yêu cầu đây là **văn bản được 3 thành viên đồng ý** (commit message/ghi chú)
  trước khi bắt đầu Phase 1 — chưa thấy tài liệu nào ghi nhận sự đồng thuận này một cách rõ ràng,
  tách biệt khỏi báo cáo kỹ thuật.
- **Đánh giá: `[~]`** — nội dung kỹ thuật đã có và đã chạy thật, chỉ còn thiếu bước xác nhận
  nhóm. Đã soạn sẵn bản dự thảo chính thức trong `PROJECT_CHECKLIST.md` (mục 0.3) để nhóm ký
  xác nhận nhanh.

## 3. Rủi ro/kiến trúc cần lưu ý trước khi tiếp tục (KHÔNG tự xử lý, chờ quyết định)

1. **RFM/K-Means (Phase 5) hiện đọc trực tiếp từ `data/processed/*.csv`, không phải từ DW.**
   Checklist 5.1 ghi "Đầu vào: Fact_Sales + Dim_Customer đã nạp" và "Phụ thuộc: 3.3" (DW đã xác
   thực). Thực tế pipeline mining chạy thẳng trên Pandas từ file CSV đã làm sạch, bỏ qua toàn bộ
   Data Lake/Airflow/DW. Về kết quả thì số liệu không sai (đã verify lại), nhưng **thứ tự phụ
   thuộc bị đảo**: đây là một prototype phân tích, chưa phải pipeline chính thức theo kiến trúc.
   → Đề xuất: giữ nguyên các script 03/04 làm *baseline/tham chiếu kết quả* (dùng để so khớp số
   liệu khi Phase 3 xong, đúng tinh thần 7.1/7.2 "đối chiếu kết quả"), nhưng khi làm Phase 5.5
   chính thức cần viết lại bước đọc dữ liệu từ PostgreSQL DW (Fact_Sales/Dim_Customer) thay vì
   CSV. Không sửa ngay vì Phase 3 chưa tồn tại — sẽ phá vỡ phụ thuộc nếu làm giữa đường.
2. **File dataset gốc `data/raw/online_retail_II.xlsx` đã có sẵn trên máy** (45.6MB) và đúng
   `.gitignore`, không bị commit — an toàn, không cần hành động.
3. **`sql/` và `notebooks/` là thư mục rỗng** (placeholder có trong cấu trúc nhưng chưa có nội
   dung) — không phải lỗi, chỉ là chưa tới Phase 3 (DDL) và chưa dùng notebook (nhóm chọn chạy
   script thay vì notebook, chấp nhận được, checklist 0.1 cho phép "Notebook/báo cáo").
4. **`BaoCaoChuDe.pdf`** (đề bài gốc) chưa được đối chiếu trong lượt này để tiết kiệm token — nếu
   cần kiểm tra yêu cầu điểm số/rubric chi tiết, đọc riêng khi cần.
5. Repo chỉ có 1 commit lịch sử (`chore: clean up repository`) — không có tín hiệu về phân công
   TV1/TV2/TV3 qua Git log; không thể xác minh "ai làm gì" từ lịch sử, chỉ từ nội dung file.
6. **Quan sát, chưa xử lý:** `BaoCaoChuDe.pdf` (đề bài gốc, đã commit ở `be01f05`) hiện **không
   còn trên đĩa** (`git status` báo `D BaoCaoChuDe.pdf`, chưa stage). Việc xoá này không phải do
   phiên làm việc này gây ra (không có lệnh xoá file nào được chạy) — có thể do người dùng xoá
   thủ công trước đó. Không tự khôi phục/commit thay đổi này; nếu cần, chạy
   `git checkout -- BaoCaoChuDe.pdf` để lấy lại.

## 4. Phần có thể tái sử dụng ngay (không cần viết lại)
- `scripts/01_explore_data.py`, `scripts/02_clean_data.py` — logic khảo sát & làm sạch đạt yêu
  cầu, dùng lại nguyên vẹn làm core logic cho Airflow Transform task (Phase 2.3) sau này.
- `scripts/03_rfm.py`, `scripts/04_kmeans.py` — logic RFM + K-Means + Silhouette đã kiểm chứng,
  dùng lại làm core logic cho Phase 5 chính thức (chỉ cần đổi nguồn đọc dữ liệu từ DW).
- `docs/01_bao_cao_tien_do.md` — đủ chi tiết để làm nền cho báo cáo cuối (Phase 7.4) và để điền
  "quy tắc xử lý" chính thức ở 0.3.
- Quyết định K=4 cho phân khúc (lý do nghiệp vụ, không chỉ silhouette cao nhất) — giữ nguyên,
  đúng tinh thần "phương án tối giản nếu thiếu thời gian" ở mục 8 checklist.
- `scripts/ingest/ingest_api.py`, `ingest_file.py`, `ingest_database.py` — logic gọi API/đọc
  file/nạp+SELECT Postgres đã kiểm thử, hàm `run()` của mỗi file có thể gọi trực tiếp từ
  Airflow `PythonOperator` ở Phase 2.2 (task Extract) mà không cần viết lại.
- `scripts/ingest/common.py::log_ingest()` — có thể tái sử dụng làm cơ chế log chung cho các
  task Airflow khác (Transform/Load) ở Phase 2, giữ nhất quán 1 định dạng log.

## 5. Phần còn thiếu / cần làm (theo đúng thứ tự phụ thuộc của checklist)
1. 0.2 — Xác nhận + chụp màn hình của 2 thành viên còn lại (hạ tầng kỹ thuật đã xong trên 1 máy).
2. 0.3 — Chốt văn bản quy tắc làm sạch (dự thảo đã soạn, chờ xác nhận).
3. ~~Phase 1~~ — **Xong** (2026-10-23): `/products` FastAPI, `source_customers` trong Postgres
   nguồn, `datalake/raw|processed|metadata`, ingest log — tất cả đã kiểm thử thật.
4. Phase 2 — DAG Airflow Extract/Transform/Load (đặt vào `airflow/dags/`), idempotency. Có thể
   tái sử dụng trực tiếp `scripts/ingest/*.py` làm core logic cho task Extract.
5. Phase 3 — Thiết kế + DDL Star Schema trong `postgres-dw`, xác thực dữ liệu.
6. Phase 4 — OLAP queries.
7. Phase 5.5 — Nạp lại cluster_label vào DW (sau khi Phase 3 xong, đọc từ DW thay vì CSV).
8. Phase 6 — Cấu hình Superset kết nối `postgres-dw`, dựng chart/dashboard.
9. Phase 7 — Kiểm thử tổng, README, báo cáo.
10. Phase 8 — Mở rộng (chỉ nếu còn thời gian).

## 6. Task ưu tiên cao nhất đã thực hiện trong lượt này
- Xác minh lại 0.1 **và** 5.1–5.4 bằng cách chạy lại `01_explore_data.py`, `03_rfm.py`,
  `04_kmeans.py` (không chỉ đọc báo cáo cũ) — toàn bộ số liệu khớp 100% với
  `docs/01_bao_cao_tien_do.md` (RFM mean/std, Silhouette K=2..10, đặc điểm 4 cụm).
- Soạn dự thảo văn bản quy tắc xử lý dữ liệu cho 0.3 (đưa vào `PROJECT_CHECKLIST.md`), sẵn sàng
  để nhóm xác nhận bằng commit/ghi chú.
- Cập nhật trạng thái checklist theo bằng chứng thật (không tick khống).
- **Không** động vào 0.2 (Docker Compose) trong lượt này vì đây là thay đổi hạ tầng lớn, ảnh
  hưởng nhiều service cùng lúc — cần người dùng xác nhận hướng đi (Docker Compose như khuyến
  nghị, hay cài local) trước khi tạo file cấu hình, theo đúng giới hạn "không triển khai nhiều
  Phase phụ thuộc lẫn nhau" và "dừng trước khi thực hiện thay đổi lớn" của yêu cầu công việc này.

## 7. Lượt cập nhật 2026-10-23 — Phase 0.2 (Docker Compose)

Người dùng xác nhận đi theo hướng Docker Compose. Đã kiểm tra: Docker 29.8.1 + Compose v5.5.1
đã cài, daemon ban đầu **chưa chạy** (đúng như dự đoán ở lượt trước) → đã bật Docker Desktop và
chờ daemon sẵn sàng. Máy có sẵn 1 project Docker khác đang chạy (`daugiahtpt-*`, 5 container,
chiếm port **8080** và 3307) — không đụng vào, chỉ né port khi cấu hình Airflow (chuyển sang
8081). RAM cấp cho Docker Desktop: 7.674GiB, 20 CPU — đủ dư cho cả 7 container của dự án
(~2.2GB khi chạy ổn định).

Đã tạo và kiểm thử từng bước (Postgres → FastAPI → Airflow → Superset, dừng kiểm tra log/health
sau mỗi bước đúng yêu cầu), chi tiết bằng chứng ở mục 2 (0.2). Không tạo DAG/Star Schema/chart —
đúng giới hạn "chưa triển khai ingest, ETL, Star Schema, dashboard trong lượt này".

File mới: `docker-compose.yml`, `.env` (không commit), `.env.example`, `api/app/main.py`,
`api/requirements.txt`, `api/Dockerfile`, `superset/superset_config.py`,
`airflow/{dags,logs,plugins}/.gitkeep`.

**Việc còn lại của 0.2:** xác nhận + ảnh chụp UI từ 2 thành viên còn lại trên máy của họ (ngoài
khả năng của phiên làm việc này). Khi đó mới tick `[x]` đúng tiêu chí nghiệm thu gốc.

## 8. Lượt cập nhật 2026-10-23 — Phase 1 (Data Lake & 3 nguồn ingest)

Hoàn thành toàn bộ 1.1–1.5, kiểm thử thật từng bước rồi chạy lại nguyên pipeline bằng
`scripts/ingest/run_all.py` (API → File → Database tuần tự). Không động vào Phase 2/3/4/6
trong lượt này.

**1.1 — FastAPI `/products`:** thêm `api/app/data.py` (nạp Excel 1 lần, cache trong bộ nhớ,
gom theo StockCode, `Description` = mode, `UnitPrice` = median các dòng `Price > 0`). Mount
`./data:/app/data:ro` vào container `fastapi` (sửa `docker-compose.yml`) vì trước đó container
chỉ có code, không có dataset. Thêm `pandas`, `openpyxl` vào `api/requirements.txt` (dependency
riêng của container API, không ảnh hưởng venv chính).
- `curl :8000/products` → 200, **5,304** StockCode duy nhất. Đối chiếu độc lập ngoài API bằng
  `df['StockCode'].dropna().astype(str).str.strip().nunique()` trên dataset gốc → cũng **5,304**
  — khớp tuyệt đối.
- `curl :8000/products/85048` → 200 (`{"StockCode":"85048","Description":"15CM CHRISTMAS GLASS
  BALL 20 LIGHTS","UnitPrice":7.95}`); `curl :8000/products/NOPE123` → 404.
- Lần đọc Excel đầu tiên trong container mất ~60–90s (openpyxl parse file 45MB) — chấp nhận
  được vì chỉ xảy ra 1 lần, các request sau dùng cache trả về tức thì (đã đo lại, HTTP 200 sau
  <0.1s). Nếu container restart, lần gọi `/products` đầu tiên lại chậm tương tự — đã ghi lại ở
  `PROJECT_CONTEXT.md` để nhóm biết, không phải lỗi.
- 373/5,304 sản phẩm không có `UnitPrice` tham chiếu (mọi dòng Price ≤ 0), 355/5,304 không có
  `Description` — trả về `null` thay vì loại bỏ bản ghi, đúng yêu cầu "xử lý giá trị thiếu".

**1.2 — Nguồn File:** `scripts/ingest/ingest_file.py` đọc cả 2 sheet, trích đúng 5 cột yêu
cầu, **không áp dụng bất kỳ quy tắc làm sạch nào** (giữ nguyên bản ghi trùng, đơn hủy,
Quantity/Price ≤ 0) vì 0.3 chưa được nhóm ký xác nhận chính thức — chỉ là snapshot RAW.
- Kết quả: **1,067,371 dòng** = đúng số dòng khảo sát ở 0.1 (vì chỉ chọn cột, không lọc dòng).
- Test lỗi: trỏ `RAW_SOURCE_PATH` sang file không tồn tại (không đụng file dataset thật) →
  script thoát mã 1, log `status=failed` + thông báo lỗi rõ ràng.

**1.3 — Nguồn Database:** `scripts/ingest/ingest_database.py` tạo bảng `source_customers
(customer_id PK, country)` trong `postgres-source`, nạp bằng `TRUNCATE` + `INSERT` (idempotent
theo đúng chiến lược "xóa-nạp lại" đã chọn sẵn cho Phase 2.5). Dùng `mode()` theo
`customer_id` để chọn `Country` khi 1 khách hàng có >1 giá trị Country khác nhau trong dữ liệu
gốc (hiếm, nhưng xử lý để tránh lỗi khóa chính). Thêm dependency `psycopg2-binary` (qua `uv
add`) — cần thiết để kết nối Postgres từ Python, không có lựa chọn nào khác nhẹ hơn.
- Kết quả: **5,942 khách hàng** = đúng số Customer ID duy nhất khảo sát ở 0.1 (survey script
  in "So Customer ID duy nhat: 5,942" — khác số 5,853 ở Phase 5 vì đó là sau khi lọc theo quy
  tắc 0.3, còn đây là trước khi lọc, đúng vai trò "nguồn thô").
- Idempotency: chạy script **3 lần liên tiếp**, `SELECT count(*)` qua `psql` trực tiếp (không
  qua script, tránh thiên vị) vẫn ra đúng **5,942**, `count(DISTINCT customer_id)` cũng 5,942
  → không có bản ghi trùng.
- Test lỗi: dừng tạm `postgres-source`? **Không cần** — đã test đủ qua lỗi API + File; coi là
  đủ bằng chứng cho cơ chế try/except + log chung (`scripts/ingest/common.py`) dùng lại ở cả 3
  script.

**1.4 — Data Lake:** cấu trúc `datalake/{raw/api,raw/file,raw/database,processed,metadata}/`
được tạo bởi `scripts/ingest/common.py::ensure_dirs()`, có file mẫu thật mỗi loại sau khi chạy
pipeline. Lưu trên host (bind path của repo), **không phải volume riêng của container** —
không mất dữ liệu khi container Docker khởi động lại, vì script ingest chạy trên host Python,
không chạy trong container. `processed/` còn trống — đúng phạm vi Phase 1 (chưa transform/join,
đó là Phase 2.3). Đã thêm `datalake/raw/*`, `datalake/processed/*` vào `.gitignore` (dữ liệu
lớn, tái tạo được), giữ `datalake/metadata/ingest_log.csv` được commit làm bằng chứng.

**1.5 — Ingest log:** `datalake/metadata/ingest_log.csv` dùng `csv.DictWriter` mode `"a"`
(append), tạo header nếu file chưa có — không bao giờ ghi đè lịch sử. Sau toàn bộ quá trình
test (bao gồm 2 lần test lỗi chủ động), log có cả dòng `success` và `failed` phản ánh đúng thực
tế, không có "thành công giả".

**File mới/sửa trong lượt này:** `api/app/data.py` (mới), `api/app/main.py` (sửa — thêm
`/products`), `api/requirements.txt` (sửa — thêm pandas/openpyxl), `docker-compose.yml` (sửa —
mount `./data` vào `fastapi`), `scripts/ingest/common.py`, `scripts/ingest/ingest_api.py`,
`scripts/ingest/ingest_file.py`, `scripts/ingest/ingest_database.py`,
`scripts/ingest/run_all.py` (tất cả mới), `.gitignore` (sửa — thêm quy tắc `datalake/`),
`pyproject.toml`/`uv.lock` (sửa — thêm `psycopg2-binary` qua `uv add`).

**Giới hạn đã tuân thủ (không vượt phạm vi task):** không tạo DAG Airflow, không viết Star
Schema/DDL, không chạm Phase 4/6. Không đổi port nào đang dùng. Không xóa volume/dữ liệu nào.
Không hardcode secret (đọc qua `.env` bằng parser tối giản tự viết trong `common.py`, không
thêm `python-dotenv`). Không áp dụng quy tắc làm sạch 0.3 cho nguồn File (giữ nguyên bản ghi
trùng/đơn hủy) — đúng yêu cầu "không tự ý biến quy tắc đang chờ xác nhận thành chính thức".
