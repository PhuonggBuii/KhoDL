# Gap Analysis — Đối chiếu PROJECT_CHECKLIST.md với hiện trạng repo

Ngày đối chiếu: 2026-10-23. Phương pháp: đọc trực tiếp code/script/docs, chạy lại script để xác
minh số liệu còn đúng (không suy đoán từ tên file).

## 1. Tóm tắt hiện trạng

| Phase | Trạng thái thực tế | Ghi chú |
|---|---|---|
| 0 — Khảo sát & môi trường | **0.1 xong**, 0.2 đã dựng+kiểm thử (chờ xác nhận nhóm), 0.3 chưa chính thức hóa | Xem mục 2 |
| 1 — Data Lake & 3 nguồn ingest | **Xong, đã kiểm thử thật** | `api/`, `scripts/ingest/`, `datalake/` — xem mục 8 |
| 2 — ETL Airflow | **Toàn bộ 2.1–2.5 xong, kiểm thử thật đầu-cuối** | `airflow/dags/retail_etl_dag.py` + `scripts/transform/`, `scripts/load/` — xem mục 12. DAG run đầy đủ SUCCESS |
| 3 — Data Warehouse | **3.1/3.2/3.3 xong, kiểm thử thật** | `fact_sales` có 1,003,670 dòng dữ liệu thật, đã xác thực đầy đủ (mục 12) |
| 4 — OLAP | **4.1+4.2+4.3+4.4 xong hoàn toàn, kiểm thử thật** | `sql/002_olap_views.sql` — 13 view khớp tuyệt đối với `fact_sales`, idempotent, xem mục 13 |
| 5 — Data Mining (RFM + K-Means) | **5.1–5.5 xong, kiểm thử thật (2026-10-24)** | RFM/K-Means chính thức đọc từ DW (`scripts/mining/rfm_dw.py`, `cluster_dw.py`), nạp `customer_segment` vào DW (`sql/003_customer_segment.sql`, `load_segments_dw.py`) — xem `docs/05_rfm_kmeans_dw.md`. Baseline CSV (`scripts/03_rfm.py`/`04_kmeans.py`) giữ nguyên không sửa, dùng làm tham chiếu đối chiếu |
| 6 — Dashboard Superset | **Đã làm qua REST API (2026-10-24), kiểm thử thật** | Database+5 dataset+6 chart+1 dashboard, số liệu khớp DW — xem `docs/06_superset_dashboard.md`; chưa xác nhận bằng mắt trong browser |
| 7 — Kiểm thử tổng/báo cáo | **Phần lớn xong (2026-10-24)** | `docs/00_bao_cao_do_an.md` — còn thiếu xác nhận browser + tên thật thành viên + chữ ký nhóm 0.3 |
| 8 — Mở rộng | **Chưa làm** | Đúng kế hoạch (làm sau cùng) |

**Kết luận chính (cập nhật 2026-10-24, sau khi hoàn thiện toàn bộ Phase 4):** Phase 0, Phase 1,
toàn bộ Phase 2 (2.1–2.5), Phase 3 (3.1–3.3), và giờ **toàn bộ Phase 4 (4.1 Roll-up/Drill-down
Năm→Quý→Tháng→Ngày, 4.2 Slice+Dice, 4.3 Pivot, 4.4 tổng hợp)** đã hoàn thành và kiểm thử thật
— pipeline ETL đầu-cuối đã chạy SUCCESS, `fact_sales` có 1.003.670 dòng dữ liệu thật đã xác
thực, và 13 view OLAP đối soát khớp tuyệt đối với `fact_sales`, chạy idempotent.

**Cập nhật 2026-10-24 (sau khi hoàn thiện Phase 5):** Phase 5.1 (RFM) và 5.5 (nạp cluster vào
DW) đã chuyển sang pipeline chính thức đọc từ DW — `scripts/mining/rfm_dw.py` (đọc `fact_sales
JOIN dim_customer`, đối chiếu baseline khớp tuyệt đối) và `scripts/mining/cluster_dw.py` +
`sql/003_customer_segment.sql` + `scripts/mining/load_segments_dw.py` (bảng `customer_segment`
riêng, UPSERT, 5.853 dòng, 0 orphan FK, idempotent đã kiểm thử). Chi tiết:
`docs/05_rfm_kmeans_dw.md`. Rủi ro #1 ở mục 3 dưới đây coi như đã giải quyết (xem cập nhật kèm
theo). **Chỉ còn Phase 6 (Superset chart) chưa làm** — không còn bị chặn kỹ thuật vì đã có đủ
view OLAP (Phase 4) và bảng `customer_segment` (Phase 5.5) làm nguồn chart sẵn.

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
   **Cập nhật 2026-10-24 — ĐÃ GIẢI QUYẾT:** pipeline chính thức mới
   `scripts/mining/rfm_dw.py` + `scripts/mining/cluster_dw.py` đọc trực tiếp từ DW như đề xuất,
   KHÔNG sửa `scripts/03_rfm.py`/`04_kmeans.py` (vẫn giữ làm baseline tham chiếu đúng như đề
   xuất ban đầu). Đối chiếu thật: RFM-từ-DW khớp tuyệt đối baseline (0 khách hàng lệch), K=4
   silhouette khớp tuyệt đối (0.3611). Chi tiết: `docs/05_rfm_kmeans_dw.md`.
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
4. ~~Phase 2 (2.1–2.5) + Phase 3.1/3.2/3.3~~ — **Toàn bộ Xong** (2026-10-23): DAG
   `retail_online_ii_etl` chạy đầu-cuối SUCCESS (Extract → Transform → Load → Validate),
   `fact_sales` = 1,003,670 dòng dữ liệu thật, đã xác thực đầy đủ — xem mục 10, 11, 12. 5 quyết
   định nghiệp vụ (mục 9) đã code thật, chờ nhóm xem xét/nghiệm thu chính thức.
5. ~~Phase 4 (4.1–4.4)~~ — **Xong hoàn toàn** (2026-10-24): 13 view OLAP — roll-up Năm/Quý/
   Tháng/Ngày, slice quốc gia/sản phẩm/khách hàng + top 10, dice Quốc gia×Năm, pivot Quốc
   gia×Năm, KPI — xem mục 13. Chỉ còn giới hạn hẹp: chưa pivot Thời gian × Sản phẩm.
6. ~~Phase 5.1 chính thức + Phase 5.5~~ — **Xong** (2026-10-24): RFM đọc từ DW, bảng
   `customer_segment` đã tạo và nạp (UPSERT, idempotent) — xem `docs/05_rfm_kmeans_dw.md`.
7. Phase 6 — Cấu hình Superset kết nối `postgres-dw`, dựng chart/dashboard từ các view OLAP
   đã có sẵn (`sql/002_olap_views.sql`).
8. Phase 7 — Kiểm thử tổng, báo cáo (README đã viết ở lượt trước).
9. Phase 8 — Mở rộng (chỉ nếu còn thời gian).

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

## 9. Lượt cập nhật 2026-10-23 — Phase 3.1 + 3.2 (Star Schema & DDL)

Thiết kế dựa trên khảo sát lại thực tế dataset trong lượt này (không dùng số liệu nhớ lại từ
báo cáo cũ): `Quantity` **không bao giờ = 0** (0/1,067,371 dòng), `Price < 0` chỉ **5 dòng**,
có **42,638/1,021,424** cặp `(Invoice, StockCode)` xuất hiện nhiều hơn 1 dòng → xác nhận cần
surrogate key riêng cho `fact_sales`, không thể dùng `(Invoice, StockCode)` làm khóa tự nhiên.

**Thiết kế chính** (chi tiết + lý do đầy đủ: `docs/03_star_schema.md`):
- 4 dimension (`dim_date`, `dim_customer`, `dim_product`, `dim_country`) dùng surrogate PK
  `BIGINT GENERATED ALWAYS AS IDENTITY` + khóa tự nhiên `UNIQUE` riêng.
- `fact_sales`: grain 1 dòng/dòng hàng hóa đơn, 4 FK tới dimension (`customer_key` **nullable**
  — xem quyết định mở #2), measures `quantity`/`unit_price` (`NUMERIC`, không `FLOAT`),
  `revenue` là **cột `GENERATED ALWAYS AS (ROUND(quantity * unit_price, 2)) STORED`** — Postgres
  tự tính, loại bỏ hoàn toàn khả năng lệch công thức.
- `country_key` trên `fact_sales` **độc lập** với `dim_customer` (lấy trực tiếp từ cột
  `Country` của dòng giao dịch, không suy từ khách hàng) — quyết định kiến trúc ghi rõ lý do
  và gắn cờ cần nhóm xác nhận.
- `CHECK (quantity <> 0)` và `CHECK (unit_price >= 0)` — cả hai đã xác minh **không loại bỏ
  bất kỳ dòng hợp lệ nào** của dataset gốc (quantity không bao giờ 0; giá âm là lỗi nhập liệu
  hiếm, không phải chính sách nghiệp vụ đang tranh luận).

**Kiểm thử thật đã chạy (2026-10-23), tất cả trên `khodl-postgres-dw` / db `retail_dw`:**
1. Chạy `sql/001_create_star_schema.sql` lần 1 → `CREATE TABLE`/`CREATE INDEX` không lỗi.
2. `\dt` → đúng 5 bảng; `\d+ fact_sales`, `\d dim_*` → đúng PK/FK/CHECK/kiểu dữ liệu (`numeric(10,2)`, `numeric(14,2)` cho tiền tệ, không có `float`).
3. Chạy lại DDL **lần 2** → chỉ `NOTICE: ... already exists, skipping`, exit code 0, vẫn 5 bảng.
4. **Test ràng buộc thật trong 1 transaction rồi `ROLLBACK`** (không để lại dữ liệu trong DW):
   insert đủ 4 dimension + 1 fact hợp lệ → `revenue` tự tính đúng `6 × 2.55 = 15.30`;
   `quantity=0` → bị `CHECK` chặn đúng; `product_key` không tồn tại → bị FK chặn đúng;
   `customer_key = NULL` → được chấp nhận, `revenue` vẫn tính đúng `3 × 4.00 = 12.00`.
5. Sau `ROLLBACK`, xác nhận `dim_date`/`fact_sales` vẫn **0 dòng** — DW không bị nạp dữ liệu
   test, đúng yêu cầu "DW vẫn rỗng dữ liệu nghiệp vụ là trạng thái hợp lệ".
6. Toàn bộ 7 container vẫn `healthy` sau khi test, project `daugiahtpt-*` trên máy không bị
   ảnh hưởng.

**5 quyết định thiết kế đã có đề xuất triển khai (cập nhật Phase 2.1, 2026-10-23 — xem mục
10 bên dưới), chưa phải nhóm chính thức xác nhận**, chi tiết ở `docs/03_star_schema.md` mục 6:
(1) `Country` lấy theo dòng giao dịch (không theo khách hàng), (2) dòng thiếu Customer ID giữ
`NULL` trong `fact_sales` nhưng loại khỏi RFM, (3) đơn hủy/trả hàng loại khỏi `fact_sales`
(không xóa khỏi Data Lake), (4) MVP chỉ `dim_date`, không `dim_time`, (5) `description` đại
diện = mô tả phổ biến nhất, đồng hạng theo thứ tự chữ cái, chuẩn hóa `strip()`+gộp khoảng trắng.

**File mới:** `docs/03_star_schema.md`, `sql/001_create_star_schema.sql`. Không sửa file nào
khác ngoài tài liệu (`PROJECT_CHECKLIST.md`, `PROJECT_CONTEXT.md`, file này). Không chạm
`scripts/03_rfm.py`/`04_kmeans.py`, không đổi trạng thái Phase 2/4/5.1/6.

## 10. Lượt cập nhật 2026-10-23 — Phase 2.1 (Thiết kế DAG Airflow)

**5 quyết định nghiệp vụ** đã chuyển từ "còn mở" sang "có đề xuất triển khai cụ thể" (chi tiết
mục 9 đã sửa lại ở trên, đầy đủ ở `docs/03_star_schema.md` mục 6) — **vẫn chưa phải nhóm chính
thức xác nhận**, chỉ là hướng triển khai đề xuất để code Phase 2.2+ có cơ sở.

**DAG mới:** [`airflow/dags/retail_etl_dag.py`](../airflow/dags/retail_etl_dag.py) —
`retail_online_ii_etl`: `start → extract{api,file,database} → transform → load → validate →
end`. `transform`/`load` chủ động `AirflowSkipException` (không ghi dữ liệu); `validate` kết
nối thật `postgres-dw` đếm `fact_sales` rồi skip (không báo thành công giả).

**Thay đổi hạ tầng kèm theo (`docker-compose.yml`):** thêm mount
`./scripts:/opt/airflow/scripts:ro` và 3 biến `PG_DW_DB/USER/PASSWORD` vào `x-airflow-common`
— cần thiết để `extract` import được `scripts/ingest/*.py` và `validate` kết nối DW thật. Đã
`docker compose up -d airflow-init airflow-webserver airflow-scheduler` để áp dụng, **không**
đụng `postgres-source/dw/airflow`, `fastapi`, `superset`, và **không** ảnh hưởng project
`daugiahtpt-*`/port 8080 (đã verify lại bằng `curl` sau khi recreate).

**Kiểm thử thật đã chạy, phân biệt rõ 3 loại (đúng yêu cầu task):**

1. **Cú pháp/cấu trúc DAG:** `airflow dags list-import-errors` → không lỗi; `airflow dags
   list` → DAG xuất hiện, `is_paused=True`; `airflow tasks list --tree` → đúng cấu trúc fan-
   out/fan-in; `airflow dags details` → `schedule_interval: None`, "Never, external triggers
   only" → xác nhận không tự chạy backfill. **PASS.**
2. **Task TODO không ghi dữ liệu:** `airflow tasks test ... transform` và `... load` → cả hai
   `Marking task as SKIPPED` kèm log lý do rõ ràng, không có lệnh ghi file/DB nào trong code
   được thực thi (xác nhận bằng đọc log, không chỉ đọc source). **PASS.**
3. **Validate (kết nối DW thật):** `airflow tasks test ... validate` → in
   `[validate] fact_sales hien co 0 dong` (đúng kỳ vọng — DW rỗng từ Phase 3), rồi `SKIPPED`
   với lý do rõ ràng. **PASS**, và là bằng chứng DAG có thể kết nối `postgres-dw` thật từ trong
   container (dùng tên service `postgres-dw`, không phải `localhost`).
4. **Extract (tích hợp `scripts/ingest/*.py`):** **THẤT BẠI THẬT**, đã verify bằng log chi
   tiết, không phải suy đoán:
   - `extract.extract_api` → `AirflowException` (exit code 1); log gốc:
     `urlopen error [Errno 111] Connection refused` khi gọi `http://localhost:8000/products` —
     vì `localhost` trong container Airflow **không** trỏ tới container `fastapi` (khác network
     namespace).
   - `extract.extract_file`, `extract.extract_database` → cùng lỗi: `Khong tim thay dataset tai
     /opt/airflow/data/raw/online_retail_II.xlsx` — vì `./data` chưa được mount vào container
     Airflow (chỉ mount `./scripts`).
   - (Ngoài ra, nếu vượt qua được lỗi đường dẫn, `extract.extract_file`/`extract_database` còn
     sẽ gặp `ModuleNotFoundError: openpyxl` — đã verify package này **không có** trong image
     `apache/airflow:2.10.5-python3.12`, dù `pandas`/`psycopg2` đã có sẵn.)
   - **Đây là giới hạn đã biết, KHÔNG tự sửa trong Phase 2.1** (đúng giới hạn "chỉ thiết kế
     DAG, không tự chuyển Phase 2.2"). Nguyên nhân gốc: `scripts/ingest/*.py` (Phase 1) được
     viết và kiểm thử cho **máy host** (kết nối `localhost`, đường dẫn `data/raw/` trên host),
     chưa được thiết kế để chạy trong container Airflow.
5. **Không kiểm thử/tuyên bố "pipeline đầu cuối"** — đúng yêu cầu, vì Transform/Load chưa triển
   khai và Extract chưa chạy được thật trong container.
6. Xác nhận **không có dữ liệu nào bị ghi sai chỗ**: `datalake/metadata/ingest_log.csv` thật
   trên host **không có dòng mới** từ các lần `airflow tasks test` (container tự tạo
   `/opt/airflow/datalake/` riêng trong filesystem của container, không mount ra host — không
   ảnh hưởng file bằng chứng đã commit).

**Việc cần làm ở Phase 2.2 (chưa làm, chỉ ghi nhận):**
- Làm cho `scripts/ingest/*.py` chạy được trong container: tham số hóa hostname (biến môi
  trường thay cho `"localhost"` hardcode trong `ingest_api.py`/`ingest_database.py`), mount
  `./data` vào Airflow, thêm `openpyxl` vào image Airflow. **Đây là quyết định kỹ thuật cần
  người dùng/nhóm xác nhận hướng đi** (sửa trực tiếp script để nhận biến môi trường, hay giữ
  script nguyên vẹn và chỉ dùng chúng trên host rồi để Airflow gọi qua cơ chế khác) — chưa tự
  quyết trong lượt này vì vượt phạm vi Phase 2.1.
- Áp dụng 5 quyết định nghiệp vụ (mục 9) vào code Transform thật (Phase 2.3).

## 11. Lượt cập nhật 2026-10-23 — Phase 2.2 (Extract chạy thật trong Airflow)

Giải quyết đúng 3 nguyên nhân đã xác định ở mục 10 (giữ nguyên hành vi chạy trên host — Phase
1 — không bị ảnh hưởng):

1. **Tham số hóa hostname** (`scripts/ingest/ingest_api.py`, `ingest_database.py`): đọc
   `FASTAPI_HOST`/`PG_SOURCE_HOST` từ biến môi trường, mặc định `"localhost"`. Đây là thay đổi
   code **nhỏ nhất có thể** (1 dòng mỗi file) để vừa giữ hành vi cũ trên host, vừa cho phép
   container đổi route qua tên service Docker.
2. **Mount mirror cấu trúc thư mục** trong `docker-compose.yml` (`x-airflow-common`):
   `./data:/opt/airflow/data:ro`, `./datalake:/opt/airflow/datalake` (đọc/ghi),
   `./.env:/opt/airflow/.env:ro` — đặt đúng vị trí tương đối so với `./scripts` (đã mount từ
   Phase 2.1) để `scripts/ingest/common.py::ROOT_DIR` (tính từ `Path(__file__).resolve()
   .parent.parent.parent`) tự trỏ đúng, **không cần sửa logic tính đường dẫn trong code**.
   Đây là lựa chọn thay cho việc thêm biến môi trường riêng cho từng đường dẫn — ít thay đổi
   code hơn, tận dụng đúng cách script đã viết sẵn.
3. **`openpyxl`**: thêm qua `_PIP_ADDITIONAL_REQUIREMENTS: "openpyxl==3.1.5"` (biến môi trường
   chính thức của image `apache/airflow`, cài lúc container khởi động) — không tạo Dockerfile
   riêng vì dự án hiện dùng image gốc trực tiếp, thêm 1 Dockerfile chỉ để cài 1 package nhỏ là
   over-engineering ở quy mô hiện tại. Đánh đổi: mỗi lần container khởi động/recreate sẽ chậm
   hơn vài giây (cài lại package, không persist vào image).

**Kiểm thử thật đã chạy, đúng thứ tự task yêu cầu (2026-10-23):**
1. Build/dependency: xác nhận `_PIP_ADDITIONAL_REQUIREMENTS` cài `openpyxl==3.1.5` thành công
   qua log container (`Successfully installed ... openpyxl-3.1.5`) + `import openpyxl` OK.
2. Container thấy Excel: `ls /opt/airflow/data/raw/` → thấy `online_retail_II.xlsx` (45.6MB,
   đúng file gốc, mount `:ro`).
3. Kết nối API bằng tên service: `urllib.request.urlopen('http://fastapi:8000/health')` từ
   trong container → `{"status":"healthy"}`.
4. Kết nối Postgres nguồn bằng tên service: `socket.create_connection(('postgres-source',
   5432))` từ trong container → thành công.
5. Chạy từng Extract task thật:
   - `airflow tasks test ... extract.extract_file` → **SUCCESS**, đọc **1,067,371 dòng** (2
     sheet), khớp 100% khảo sát 0.1.
   - `airflow tasks test ... extract.extract_database` → **SUCCESS**, trích **5,942** khách
     hàng, nạp vào `postgres-source.source_customers`.
   - `airflow tasks test ... extract.extract_api` → **SUCCESS**, gọi `http://fastapi:8000/
     products`, nhận **5,304** sản phẩm.
   - **DAG run đầy đủ qua scheduler** (`dags trigger` + tạm `dags unpause`, `dags pause` lại
     ngay sau khi xong — DAG mặc định `paused`, `schedule=None` nên không có rủi ro backfill
     khi tạm unpause): `start` → 3 `extract.*` SUCCESS (song song) → `transform`/`load`/
     `validate` SKIPPED đúng thiết kế → `end` SUCCESS. Không task nào FAILED.
6. Output + số dòng: xác nhận bằng cách đọc trực tiếp log task (không chỉ nhìn trạng thái
   SUCCESS) — 3 số liệu trên đều khớp số liệu đã biết từ Phase 1 (chạy trên host).
7. **Dữ liệu nguồn không đổi**: `data/raw/online_retail_II.xlsx` giữ nguyên kích thước
   45,622,278 byte và mtime gốc `Nov 20 2019` (mount `:ro` + không có lệnh ghi nào trong
   script động vào file này).
8. **DAG vẫn parse tốt, Transform/Load vẫn không ghi dữ liệu**: `airflow dags
   list-import-errors` → "No data found" (sau khi sửa code); `tasks test ... transform`/
   `... load` → vẫn `SKIPPED` với log lý do, `fact_sales` vẫn **0 dòng** (`psql` xác nhận trực
   tiếp từ host).

**Xác nhận an toàn khác:**
- Output thật xuất hiện trên **host** (không chỉ trong container) tại
  `datalake/raw/{api,file,database}/2026-10-23/...` — xác nhận bằng `find -newer` và đọc file
  trực tiếp từ host.
- `datalake/metadata/ingest_log.csv` trên host có thêm các dòng `success` thật (ghi
  `output_path` dạng `/opt/airflow/datalake/...` vì đó là đường dẫn script "thấy" lúc chạy
  trong container — cùng 1 file vật lý nhờ mount mirror, không phải dữ liệu ảo).
- `source_customers` vẫn idempotent: chạy qua Airflow xong, `psql` trực tiếp từ host xác nhận
  `count=5942`, `count(DISTINCT customer_id)=5942` — không trùng.
- `daugiahtpt-*`/port 8080 không bị ảnh hưởng sau khi recreate container Airflow (đã `curl`
  xác nhận lại).
- Không chạy Transform/Load thật, không đổi schema DW, không `docker compose down -v`, không
  sửa `scripts/03_rfm.py`/`04_kmeans.py`.

**File đã sửa:** `scripts/ingest/ingest_api.py`, `scripts/ingest/ingest_database.py`,
`docker-compose.yml`. File tự sinh do chạy thật: `datalake/metadata/ingest_log.csv` (thêm
dòng), `datalake/raw/{api,file,database}/2026-10-23/*` (không commit, đã có trong
`.gitignore`).

**Còn tồn tại (tại thời điểm viết mục 11):** Phase 2.3 (Transform thật), 2.4 (Load thật), 2.5,
Phase 3.3, Phase 4, Phase 6 — **đã hoàn thành ở lượt tiếp theo, xem mục 12**.

## 12. Lượt cập nhật 2026-10-23 — Phase 2.3 + 2.4 + 2.5 + 3.3 (cụm ETL hoàn chỉnh)

Hoàn thành toàn bộ cụm Transform → Load → idempotency → Validate, tích hợp vào DAG hiện có,
giữ nguyên 3 task Extract đã hoạt động (Phase 2.2). Không sửa `scripts/03_rfm.py`/
`04_kmeans.py`, không đổi schema DW, không `docker compose down -v`.

### 2.3 — Transform

**File mới:** [`scripts/transform/transform.py`](../scripts/transform/transform.py).

**Quyết định nguồn dữ liệu (quan trọng, cần hiểu rõ):** Transform đọc **trực tiếp**
`data/raw/online_retail_II.xlsx` (toàn bộ 8 cột), **không join lại 3 "nguồn" mô phỏng** trong
Data Lake. Lý do — phát hiện trong lượt này, không phải giả định trước: nguồn "File" (Phase
1.2) chỉ mang 5 cột giao dịch (`Invoice, StockCode, Quantity, InvoiceDate, Price`), **không có
Customer ID/Country**; nguồn "Database" (Phase 1.3) chỉ có Customer ID+Country **đã khử trùng
theo khách hàng** (`ingest_database.py` dùng `groupby("Customer ID")` — đúng yêu cầu Phase 1.3
khi đó), **không còn liên kết Invoice**. Hai nguồn này **không có khóa chung** để join lại
thành 1 dòng giao dịch đầy đủ — đây là hạn chế cấu trúc của thiết kế "3 nguồn mô phỏng từ 1
dataset" (chính `PROJECT_CHECKLIST.md` đã dự đoán vấn đề này ở mục 3, cột Database: "Customer
ID, Country, **và có thể Invoice liên kết**" — nhưng Phase 1.3 khi đó không cần dùng tới nên đã
bỏ qua, theo đúng chỉ dẫn lúc đó "tránh lưu dữ liệu dư thừa không có mục đích"). Vì 3 "nguồn"
đều là mô phỏng từ CHÍNH dataset này (giả định đã ghi từ đầu `PROJECT_CHECKLIST.md`), Transform
đọc trực tiếp dataset gốc là hợp lý và trung thực, **đồng thời vẫn xác thực lineage**: kiểm tra
`datalake/raw/file`, `raw/database`, `raw/api` (output của Extract) tồn tại và số dòng khớp
trước khi tính toán — giữ ý nghĩa "ETL đọc từ Data Lake" ở mức kiểm tra, không bỏ qua Extract.

**5 quy tắc áp dụng đúng thứ tự yêu cầu, log số dòng từng bước (kết quả thật, 2026-10-23):**

| Bước | Số dòng còn lại | Loại bỏ |
|---|---|---|
| Raw (đã xác thực khớp `raw/file`) | 1,067,371 | — |
| Bỏ trùng lặp chính xác | 1,033,036 | 34,335 |
| Loại đơn hủy (`Invoice` bắt đầu `C`) | 1,013,932 | 19,104 |
| Loại `Quantity<=0` hoặc `Price<=0` | 1,007,913 | 6,019 |
| Loại mã phi sản phẩm (`NON_PRODUCT_CODES`) | 1,003,670 | 4,243 |
| **Giữ** dòng thiếu Customer ID (226,813 dòng) | **1,003,670** (không đổi) | 0 (không loại) |

Dimension xây từ dữ liệu đã lọc: `dim_date=604` ngày, `dim_customer=5,853` khách hàng,
`dim_product=4,905` sản phẩm, `dim_country=43` quốc gia. `Description` đại diện: `strip()` +
gộp khoảng trắng liên tiếp, `mode()` (tie-break alphabet tự nhiên của pandas — đúng quyết định
#4 ở mục 9). Output: `datalake/processed/<ngày>/{dim_date,dim_customer,dim_product,
dim_country,fact_sales}.csv`.

**Kiểm thử:** chạy qua `uv run` trên host, qua `airflow tasks test` trong container, và qua 1
DAG run đầy đủ qua scheduler — **cả 3 lần ra đúng cùng số liệu trên**.

### 2.4 — Load

**File mới:** [`scripts/load/load.py`](../scripts/load/load.py).

Nạp đúng thứ tự `dim_date → dim_customer → dim_product → dim_country → fact_sales`. 4
dimension dùng **UPSERT** (`ON CONFLICT (khóa tự nhiên) DO NOTHING`, riêng `dim_product` dùng
`DO UPDATE SET description=...` để cập nhật mô tả nếu đổi) — **không bao giờ xóa dimension**.
`fact_sales` dùng **TRUNCATE (chỉ bảng này) + INSERT lại toàn bộ** — đúng chiến lược "xóa-nạp
lại theo batch" đã thiết kế sẵn từ đầu dự án (`PROJECT_CHECKLIST.md` Phase 2.5), đã kiểm thử an
toàn tương tự ở Phase 1.3 (`source_customers`). Surrogate key tra cứu qua `pandas.merge`/`map`
sau khi upsert dimension; `customer_key` cho phép `NULL`.

**Lỗi thật phát hiện và sửa:** lần đầu chạy `load.py` trong container Airflow (host =
`postgres-dw`) bị `Connection refused`. Nguyên nhân: `PG_DW_PORT` trong `.env` = `5433` — đây
là port **publish ra host** (chọn khác 5432 để không trùng `postgres-source` trên host), nhưng
khi kết nối qua tên service Docker nội bộ, port **thật của container Postgres luôn là 5432**.
Sửa bằng cách thêm biến `PG_DW_PORT_INTERNAL` (mặc định = `PG_DW_PORT` để không đổi hành vi
trên host), đặt `PG_DW_PORT_INTERNAL=5432` trong `docker-compose.yml` cho Airflow. Đã verify
lại: Load SUCCESS cả trên host và trong container sau khi sửa.

### 2.5 — Idempotency

Chạy `load.py` **4 lần** với cùng input (host×2 trước khi sửa DAG, `airflow tasks test`×1, DAG
run đầy đủ×1): `fact_sales` luôn đúng **1,003,670** dòng, 4 dimension luôn đúng số lượng ban
đầu — không tăng, không trùng. Xác nhận qua `psql` trực tiếp (không chỉ qua log script).

### 3.3 — Validate (tích hợp vào task `validate` của DAG)

`airflow/dags/retail_etl_dag.py::validate_dw()` được nâng cấp: nếu `fact_sales` rỗng → `SKIP`
(giữ hành vi cũ); nếu có dữ liệu → chạy đủ bộ kiểm tra và **FAIL thật** (`AirflowException`)
nếu có kiểm tra không đạt — không còn luôn luôn `SKIP`.

**Kết quả kiểm tra thật (qua cả `psql` trực tiếp và task `validate` trong Airflow — khớp
nhau):**
- `orphan_date=0`, `orphan_product=0`, `orphan_country=0`, `orphan_customer(non-null)=0`.
- `cancelled_invoices_in_fact=0` (không còn đơn hủy nào trong fact).
- `revenue_formula_mismatch=0` (mọi dòng `revenue = round(quantity*unit_price,2)`).
- `customer_key IS NULL` = 226,813 dòng — khớp 100% số liệu Transform.
- Tổng revenue DW = **19,659,453.86**, khớp **tuyệt đối** với tính độc lập bằng pandas trực
  tiếp từ `fact_sales.csv` (không qua Load) — `(quantity*unit_price).round(2).sum()`.
- Dimension: mọi dòng `dim_date`/`dim_customer`/`dim_product`/`dim_country` đều được fact tham
  chiếu (`count(DISTINCT key)` trong fact = đúng số dòng từng dimension) — không có dimension
  "mồ côi".
- Đối chiếu baseline Phase 0/5 (776,857 dòng, 5,853 khách hàng): `dim_customer=5,853` khớp
  đúng (baseline cũng loại null-Customer-ID nên số khách hàng trùng), nhưng tổng dòng khác
  nhau (776,857 vs 1,003,670) — **đúng như dự kiến vì quy tắc làm sạch khác mục đích** (baseline
  cho RFM loại cả null-Customer-ID, `fact_sales` giữ lại để đo doanh thu đầy đủ).

### Tích hợp DAG — chạy đầu-cuối thật

Trigger 1 DAG run đầy đủ qua scheduler (`dags trigger`, tạm `dags unpause` rồi `dags pause`
lại ngay sau khi xong — an toàn vì `schedule=None`): `start` → 3 `extract.*` → `transform` →
`load` → `validate` → `end`, **tất cả `SUCCESS`**, không task nào `FAILED`/`SKIPPED`.

### An toàn đã xác nhận

Dataset gốc không đổi (size 45,622,278 byte, mtime gốc `Nov 20 2019`); DDL
(`sql/001_create_star_schema.sql`) không bị sửa; `scripts/03_rfm.py`/`04_kmeans.py` không bị
sửa (`git diff` rỗng); `daugiahtpt-*`/port 8080 không ảnh hưởng; không `docker compose down
-v`, không reset volume/database; không commit/push.

**File đã sửa/tạo:** mới `scripts/transform/transform.py`, `scripts/load/load.py`; sửa
`airflow/dags/retail_etl_dag.py` (`transform`/`load` gọi module thật, `validate` nâng cấp),
`docker-compose.yml` (`PG_DW_HOST`, `PG_DW_PORT_INTERNAL`). File tự sinh:
`datalake/processed/<ngày>/*.csv` (không commit), `datalake/metadata/ingest_log.csv` (thêm
dòng, có commit).

**Còn tồn tại (tại thời điểm viết mục 12):** Phase 4 (OLAP), Phase 5.5 + 5.1 chính thức (RFM
đọc từ DW), Phase 6 (Superset) — **4.1/4.2 đã hoàn thành ở lượt tiếp theo, xem mục 13**.

## 13. Phase 4 — OLAP: toàn bộ 4.1–4.4 (Roll-up, Slice, Dice, Pivot)

> Cập nhật 2026-10-24 (2 lượt): lượt 1 làm 4.1 (Roll-up tháng/quý/năm) + 4.2 phần Slice (9
> view); lượt 2 (nội dung chính của mục này) bổ sung Drill-down Ngày, Dice, Pivot, tách rõ
> phần khách hàng thiếu Customer ID — hoàn thiện **toàn bộ Phase 4** (13 view).

Triển khai 13 view SQL trên Star Schema đã có dữ liệu thật (`fact_sales` = 1.003.670 dòng):
Roll-up/Drill-down đủ **Năm → Quý → Tháng → Ngày**, Slice theo quốc gia/sản phẩm/khách hàng
(+ top 10), Dice Quốc gia × Năm, Pivot Quốc gia × Năm, KPI tổng quan.

**File:** [`sql/002_olap_views.sql`](../sql/002_olap_views.sql) (13 view),
[`docs/04_olap.md`](04_olap.md) (thiết kế + bằng chứng kiểm thử đầy đủ + giải thích nghiệp vụ
từng phép OLAP, viết cho người chưa biết dự án).

**Nguyên tắc thiết kế:** dùng đúng `fact_sales.revenue` (cột `GENERATED`, không tính lại công
thức khác); mọi view báo cáo **cả** `fact_row_count` (số dòng fact) và `invoice_count`
(`COUNT DISTINCT invoice_no`) để không nhầm 2 khái niệm; view thời gian/quốc gia/sản phẩm/KPI
tính trên **toàn bộ** `fact_sales`; chỉ view khách hàng **loại** dòng `customer_key IS NULL`.

**Kiểm thử thật (2026-10-24), đối soát bằng SQL trực tiếp trên `postgres-dw`/`retail_dw`:**

1. **Đối soát tổng doanh thu** — `sum(total_revenue)`/`sum(fact_row_count)` của
   `v_olap_sales_yearly`, `v_olap_sales_monthly`, `v_olap_sales_quarterly`,
   `v_olap_sales_by_country`, `v_olap_product_revenue` đều = **19.659.453,86** /
   **1.003.670** — khớp **tuyệt đối** với `SELECT sum(revenue), count(*) FROM fact_sales`
   trực tiếp (không qua view). Không có sai lệch nào cần sửa.
2. **Đối soát view khách hàng** — `v_olap_customer_revenue` tổng
   **17.082.085,77**/**776.857** dòng; cộng với phần bị loại (`customer_key IS NULL`:
   **2.577.368,09**/**226.813** dòng, tính riêng) = đúng **19.659.453,86**/**1.003.670** —
   khớp tuyệt đối, xác nhận việc loại trừ đúng và chỉ đúng phần cần loại.
3. **Kiểm tra JOIN không nhân bản dòng:** `SELECT count(*) FROM fact_sales JOIN dim_date/
   dim_product/dim_country ...` đều = **1.003.670** (bằng đúng `count(*)` của `fact_sales`),
   xác nhận mỗi FK tham chiếu đúng 1 dòng dimension duy nhất, không fan-out.
4. **Spot-check thủ công (tính lại bằng SQL riêng, không qua view):** top 1 sản phẩm (`22423`)
   và top 1 khách hàng (`18102`) — kết quả khớp **100%** với view.
5. **Phát hiện đối chiếu chéo (không phải lỗi):** số dòng sau khi loại NULL Customer ID
   (776.857) trùng khớp hoàn toàn với số dòng baseline Phase 0
   (`data/processed/transactions_clean.csv`) — 2 pipeline độc lập (Pandas vs SQL view) ra
   cùng kết quả.
6. Sản phẩm `23843` (PAPER CRAFT, LITTLE BIRDIE) có `fact_row_count=1` nhưng
   `total_quantity=80.995` — dòng dữ liệu cực trị thật của dataset gốc (khớp khảo sát Phase
   0.1: `Quantity` max = 80.995), không phải lỗi.
7. **Drill-down Ngày (mới):** `v_olap_sales_daily` tổng khớp tuyệt đối 19.659.453,86/
   1.003.670 trên 604 ngày có giao dịch; đối chiếu Ngày→Tháng cho 01/2010 = 612.605,50 khớp
   cả 2 cách tính.
8. **Dice Quốc gia × Năm (mới):** tổng theo năm từ `v_olap_dice_country_year` khớp tuyệt đối
   với `v_olap_sales_yearly` cho cả 3 năm (2009: 798.732,03; 2010: 9.382.375,63; 2011:
   9.478.346,20). JOIN đồng thời 2 dimension vẫn giữ đúng 1.003.670 dòng.
9. **Pivot Quốc gia × Năm (mới):** đối chiếu tổng hàng — 0/43 quốc gia sai tổng
   (`revenue_2009+2010+2011 ≠ total_revenue`); đối chiếu tổng cột — khớp tuyệt đối với
   `v_olap_sales_yearly` từng năm + tổng toàn kho.
10. **Slice khách hàng, tách rõ phần NULL (mới):** `v_olap_sales_missing_customer`
    (2.577.368,09/226.813 dòng) + `v_olap_customer_revenue` (17.082.085,77/776.857) = đúng
    tổng toàn kho — không mất, không trùng dữ liệu khi phân tích theo khách hàng.
11. **Idempotency:** chạy nguyên khối `sql/002_olap_views.sql` lần 2 — 13×`CREATE OR REPLACE
    VIEW` thành công, không lỗi, không tạo đối tượng trùng.

**Không phát hiện sai lệch nào cần điều tra/sửa trong cả 2 lượt** — mọi đối soát đều khớp
ngay từ lần chạy đầu.

**Chưa làm (phạm vi hẹp, không phải thiếu sót của 4.1–4.4):**
- Pivot Thời gian × Sản phẩm (chỉ làm Quốc gia × Năm) — 4.905 sản phẩm quá nhiều cột cho 1
  pivot đọc được; nếu cần, nên pivot Top N sản phẩm × Năm/Quý thay vì toàn bộ sản phẩm.

**An toàn đã xác nhận (cả 2 lượt):** không đổi DDL `fact_sales`/`dim_*` (chỉ `CREATE OR
REPLACE VIEW`, không `DROP`/`TRUNCATE`); không sửa dataset gốc; không sửa
`scripts/03_rfm.py`/`04_kmeans.py`; không `docker compose down -v`, không truncate/nạp lại
`fact_sales`; `daugiahtpt-*`/port 8080 không ảnh hưởng; không commit/push; không tự xác nhận
thay nhóm quyết định nghiệp vụ nào (Phase 4 không có quyết định nghiệp vụ mới — chỉ dùng lại
quy ước `revenue` đã có từ Phase 3.1).
