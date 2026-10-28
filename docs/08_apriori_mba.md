# Phase 8.1 — Market Basket Analysis (luật kết hợp Apriori)

> Hạng mục mở rộng (Phase 8, `PROJECT_CHECKLIST.md` mục 8.1), làm sau khi Phase 0–7 đã hoàn
> thành phần lõi. **Không sửa** bất kỳ file nào của ETL/Airflow/Star Schema/OLAP/RFM/K-Means/
> Superset/Docker Compose hiện có — module này chỉ **thêm mới**.
>
> Cập nhật lần cuối: **2026-10-28**. Toàn bộ số liệu ở mục 6 lấy từ **chạy thật**
> `scripts/mining/apriori_mba.py --source csv`. **Cập nhật (cùng ngày, lượt kiểm thử thứ 2):**
> đã khởi động `postgres-dw` thật (Docker Desktop hoạt động trong lượt này), nạp Star Schema +
> `fact_sales` thật, và **đã chạy thật + xác nhận cả `--source dw` lẫn tích hợp DW
> (`sql/004_market_basket_rules.sql` + `scripts/mining/load_apriori_dw.py`)** — xem mục 8/9,
> không còn là "thiết kế chưa kiểm thử" nữa.

---

## 1. Mục tiêu và cách biểu diễn giỏ hàng

**Mục tiêu:** tìm các sản phẩm thường được mua cùng nhau trong 1 hóa đơn, phục vụ gợi ý bán
chéo (cross-sell)/sắp xếp trưng bày — một góc nhìn khai phá dữ liệu **khác** RFM/K-Means (phân
khúc theo khách hàng) và OLAP (tổng hợp theo thời gian/quốc gia/sản phẩm).

**Biểu diễn giao dịch (transaction):**
- **1 hóa đơn (`Invoice`) = 1 giỏ hàng (basket).**
- **1 mã sản phẩm (`StockCode`) = 1 item.** Nếu 1 hóa đơn có nhiều dòng cùng `StockCode` (ví
  dụ tách lô), các dòng đó được **gộp thành 1 item duy nhất** trong basket — Apriori chuẩn chỉ
  quan tâm sự **có mặt** của sản phẩm trong hóa đơn, không quan tâm số lượng/số dòng lặp lại.

## 2. Phạm vi dữ liệu và quy tắc làm sạch

Dùng **đúng** dữ liệu đã qua Transform (Phase 2.3) / đã nạp vào `fact_sales` (Phase 2.4) —
**không** tự định nghĩa quy tắc làm sạch mới cho Apriori:

- **Loại đơn hủy** (`Invoice` bắt đầu bằng `C`) — đã loại bởi `transform.py`.
- **Loại `Quantity`/`Price` ≤ 0** và **mã phí/điều chỉnh sổ sách** (`POST, DOT, D, M,
  BANK CHARGES, ...`) — đã loại bởi `transform.py`.
- **KHÔNG loại dòng thiếu `Customer ID`** — khác RFM (Phase 5.1): MBA phân tích sản phẩm mua
  cùng nhau **trong 1 hóa đơn**, không cần biết danh tính khách hàng mua hóa đơn đó. Dùng toàn
  bộ hóa đơn hợp lệ của `fact_sales` (1.003.670 dòng, 39.536 hóa đơn — khớp `docs/03_star_schema.md`).

`scripts/mining/apriori_mba.py::validate_transactions()` **kiểm tra lại** các điều kiện trên
(không có đơn hủy, không có `Quantity ≤ 0`) trước khi chạy — nếu vi phạm, báo lỗi rõ ràng thay
vì âm thầm lọc lại, để người dùng biết nguồn dữ liệu đầu vào chưa qua Transform/Load đúng cách.

**Hóa đơn có nhiều sản phẩm:** xử lý tự nhiên bởi việc nhóm theo `invoice_no` — 1 hóa đơn có
bao nhiêu `StockCode` duy nhất thì basket có bấy nhiêu item (basket trung bình **~25 sản phẩm
duy nhất/hóa đơn** trên dữ liệu thật, tối đa 1.109 — một vài hóa đơn bán buôn/số lượng lớn,
không loại bỏ vì đây là dữ liệu hợp lệ, chỉ ghi nhận như một đặc điểm phân phối lệch phải).

## 3. Thuật toán — tự cài đặt Apriori, không one-hot encoding

**Không có thư viện Apriori tương đương sẵn trong môi trường** (`mlxtend`, `efficient-apriori`,
`apyori` đều chưa cài — đã kiểm tra `import` thất bại cả 3 trước khi quyết định). Theo đúng
nguyên tắc "không thêm dependency nếu không cần", module **tự cài đặt thuật toán Apriori cổ
điển** bằng Python/Pandas thuần (`scripts/mining/apriori_mba.py`):

- Mỗi basket là 1 `frozenset[str]` (tập StockCode). **Không dựng ma trận one-hot** hóa đơn ×
  sản phẩm (nếu dựng dense sẽ ~39.536 × 4.905 ≈ **194 triệu ô** cho dữ liệu đầy đủ) — bộ nhớ sử
  dụng tỉ lệ với số cặp (hóa đơn, sản phẩm) **thực sự xuất hiện** (~1 triệu dòng), không phải
  kích thước ma trận đầy đủ.
- **Đếm tầng 1** (`find_frequent_itemsets`, hàm thuần không phụ thuộc I/O — dễ unit test): đếm
  trực tiếp từng item trong từng basket bằng `collections.Counter`.
- **Sinh ứng viên tầng k** (`_generate_candidates`): bước "join + prune" chuẩn của Apriori — ghép
  2 itemset tần suất cao (k-1) phần tử có chung (k-2) phần tử đầu (sau khi sắp xếp) thành ứng
  viên k phần tử, rồi loại ứng viên có bất kỳ tập con (k-1) phần tử nào **không** phổ biến
  (tính chất downward closure: mọi tập con của 1 itemset phổ biến đều phải phổ biến).
- **Đếm tầng k:** chỉ quét các basket đã thu hẹp về tập item liên quan đến ứng viên (loại bỏ
  item không nằm trong bất kỳ ứng viên nào trước khi sinh tổ hợp) — tránh quét toàn bộ basket
  lớn không liên quan.
- **Sinh luật** (`generate_rules`): với mỗi itemset phổ biến kích thước ≥ 2, sinh mọi luật
  `A -> B` (A, B là 2 phần hoạch không rỗng của itemset), tính đúng công thức chuẩn:
  - `support(A->B) = support(A ∪ B)`
  - `confidence(A->B) = support(A ∪ B) / support(A)`
  - `lift(A->B) = confidence(A->B) / support(B)`

**Đã kiểm tra đúng bằng tay:** 5 basket nhỏ `{A,B,C}, {A,B}, {A,C}, {B,C}, {A,B,D}` →
`support(A)=support(B)=0.8`, `support(AB)=0.6`, `confidence(A→B)=0.75`, `lift=0.9375` — khớp
100% giữa code và tính tay (xem `scripts/mining/test_apriori_mba.py::TestFindFrequentItemsets`,
`TestGenerateRules`).

## 4. Chọn ngưỡng `min_support`/`min_confidence` — dựa trên khảo sát thực tế

Trước khi chọn ngưỡng, đã khảo sát thực tế phân phối support của 4.905 sản phẩm trên 39.536
hóa đơn (script khảo sát tạm, không lưu lại — số liệu dưới đây lấy từ phiên chạy thật):

| `min_support` | Số sản phẩm đạt ngưỡng (frequent-1) |
|---|---|
| 0.01 (1%) | 706 |
| 0.02 (2%) | 265 |
| 0.03 (3%) | 105 |
| 0.05 (5%) | 28 |
| 0.08 (8%) | 3 |

**Lựa chọn mặc định: `min_support=0.02` (2%, ≈ 790/39.536 hóa đơn), `min_confidence=0.30`
(30%), `max_len=2` (chỉ xét cặp sản phẩm).**

**Giải thích ngắn gọn:**
- `min_support=0.02` cho **265 sản phẩm "phổ biến"** — đủ nhỏ để liệt kê/đọc được trong báo
  cáo, đủ lớn để có ý nghĩa nghiệp vụ (không phải vài sản phẩm bán chạy nhất lấn át toàn bộ như
  ở 0.08, cũng không quá rời rạc như ở 0.01 với 706 sản phẩm).
- `min_confidence=0.30` là ngưỡng điển hình cho MBA bán lẻ (ít nhất 30% hóa đơn có A thì cũng
  có B) — đủ chặt để loại luật ngẫu nhiên, đủ lỏng để không bỏ sót luật có ý nghĩa trên dữ liệu
  có nhiều sản phẩm đuôi dài (long-tail).
- `max_len=2` (chỉ cặp sản phẩm) được chọn làm **mặc định** vì lý do hiệu năng/tính thực dụng
  (xem mục 5) — **có thể cấu hình `--max-len 3` trở lên**, đã kiểm thử tăng tốc độ chạy chấp
  nhận được (xem mục 5), nhưng ở ngưỡng mặc định hầu như không sinh thêm itemset 3 phần tử nào
  (dữ liệu thực tế: các cặp sản phẩm phổ biến là các cặp "chặt" — ví dụ bộ tách trà cùng mẫu
  khác màu — không nhất thiết đi thành bộ ba ở ngưỡng 2%).

Cả 2 ngưỡng đều **cấu hình được** qua CLI (`--min-support`, `--min-confidence`, `--max-len`) —
không hard-code, người dùng có thể thử ngưỡng khác tùy mục đích báo cáo.

## 5. Hiệu năng thực tế (đã đo trên máy phát triển)

| Cấu hình | Frequent itemsets | Số luật | Thời gian chạy |
|---|---|---|---|
| `min_support=0.02, min_confidence=0.30, max_len=2` (**mặc định**) | 323 ({1: 265, 2: 58}) | 101 | **4,2–4,7s** |
| `min_support=0.02, max_len=3` | 323 (không sinh thêm itemset 3 phần tử nào ở ngưỡng này) | 101 | 4,7s |
| `min_support=0.01, min_confidence=0.20, max_len=3` | 1.336 ({1: 706, 2: 523, 3: 107}) | 1.340 | 13,2s |
| `min_support=0.99` (biên trên, kiểm thử trường hợp rỗng) | 0 | 0 | 2,5s, không lỗi |

Không cần one-hot encoding, không cần thư viện ngoài, không ảnh hưởng/làm chậm pipeline chính
(script độc lập, không được gọi từ DAG Airflow `retail_online_ii_etl`).

## 6. Kết quả thực tế (đã chạy thật, KHÔNG phải số liệu minh họa)

Chạy `uv run scripts/mining/apriori_mba.py --source csv` (tham số mặc định) trên dữ liệu
`datalake/processed/2026-10-23/fact_sales.csv` (1.003.670 dòng — chính là dữ liệu đã nạp vào
`postgres-dw.fact_sales`, xem mục 7):

- **39.536 hóa đơn, 4.905 sản phẩm duy nhất.**
- **323 frequent itemset** (265 đơn, 58 cặp), **101 luật kết hợp** đạt `confidence ≥ 0.30`.
- **101/101 luật có `lift > 1`** (liên hệ dương theo thống kê — **không** suy diễn quan hệ
  nhân quả, chỉ là đồng xuất hiện).

**Top 5 luật theo `lift`** (dữ liệu thật, xem đầy đủ 101 luật tại
`data/processed/apriori_association_rules.csv`):

| Luật (A → B) | Mô tả sản phẩm | support | confidence | lift |
|---|---|---|---|---|
| 22697 → 22698 | GREEN REGENCY TEACUP AND SAUCER → PINK REGENCY TEACUP AND SAUCER | 0.0219 | 0.639 | **24.44** |
| 22698 → 22697 | PINK REGENCY TEACUP AND SAUCER → GREEN REGENCY TEACUP AND SAUCER | 0.0219 | 0.836 | **24.44** |
| 22698 → 22699 | PINK REGENCY TEACUP AND SAUCER → ROSES REGENCY TEACUP AND SAUCER | 0.0205 | 0.786 | **21.82** |
| 22697 → 22699 | GREEN REGENCY TEACUP AND SAUCER → ROSES REGENCY TEACUP AND SAUCER | 0.0262 | 0.764 | **21.22** |
| 22726 → 22727 | ALARM CLOCK BAKELIKE GREEN → ALARM CLOCK BAKELIKE RED | 0.0211 | 0.657 | 19.05 |

**Nhận xét (diễn giải, không khẳng định nhân quả):** các luật có lift cao nhất đều là **các biến
thể màu sắc khác nhau của cùng 1 bộ sản phẩm** ("Regency Teacup and Saucer" — xanh/hồng/hoa
hồng) — hợp lý về mặt nghiệp vụ: khách mua bộ tách trà có xu hướng mua thêm biến thể màu khác để
làm bộ sưu tập/quà tặng theo set. Đây là dấu hiệu thuật toán hoạt động đúng trên dữ liệu thật,
không phải số liệu ngẫu nhiên.

File đầu ra:
- `data/processed/apriori_frequent_itemsets.csv` — 323 dòng (itemset, support, support_count, mô tả sản phẩm).
- `data/processed/apriori_association_rules.csv` — 101 dòng (antecedent, consequent, support,
  confidence, lift, `is_positive_association` = `lift > 1`).

## 7. Nguồn dữ liệu: DW (mặc định) hoặc CSV (độc lập, không cần Docker)

Script hỗ trợ 2 nguồn (`--source`):

- **`dw` (mặc định, kiến trúc chính thức):** `SELECT invoice_no, stock_code FROM fact_sales JOIN
  dim_product ...` — giống hệt pattern kết nối của `scripts/mining/rfm_dw.py` (biến môi trường
  `PG_DW_HOST`/`PG_DW_PORT_INTERNAL`/`PG_DW_DB`/`PG_DW_USER`/`PG_DW_PASSWORD`). **Chưa chạy
  được trong phiên tạo module này vì Docker Desktop không chạy** (xem mục 9).
- **`csv` (độc lập, dùng để phát triển/tái lập không cần Docker):** đọc
  `datalake/processed/<ngày>/{fact_sales,dim_product}.csv` — **chính là output thật** của
  `scripts/transform/transform.py` (Phase 2.3), cùng nội dung với những gì `scripts/load/load.py`
  đã nạp vào `postgres-dw.fact_sales` (đã xác nhận trùng số dòng 1.003.670 — xem
  `PROJECT_CONTEXT.md` mục 3.3.f). **Toàn bộ số liệu ở mục 6 chạy bằng nguồn này.**

## 8. Tích hợp vào Data Warehouse — ĐÃ CHẠY THẬT và xác nhận (2026-10-28, lượt kiểm thử thứ 2)

Theo đúng nguyên tắc "không ALTER bảng đang hoạt động" đã áp dụng cho `customer_segment`
(`sql/003_customer_segment.sql`), thiết kế 1 bảng **riêng, mới**:

- **`sql/004_market_basket_rules.sql`** — bảng `market_basket_rules`, khóa tự nhiên
  `(antecedent_items, consequent_items)` (danh sách StockCode đã sắp xếp, nối bằng `|` — không
  dùng FK tới `dim_product` vì antecedent/consequent là 1 **tập** sản phẩm, không phải 1 khóa
  đơn), các cột `support/confidence/lift/antecedent_support/consequent_support` +
  `min_support_used/min_confidence_used` (ghi lại ngưỡng của lần chạy sinh ra dòng đó) +
  `computed_at`. `CHECK` ràng buộc support/confidence trong (0,1], lift ≥ 0.
- **`scripts/mining/load_apriori_dw.py`** — đọc `data/processed/apriori_association_rules.csv`,
  **UPSERT** theo khóa tự nhiên (`ON CONFLICT ... DO UPDATE`, không `TRUNCATE`) — idempotent,
  giống hệt pattern của `scripts/mining/load_segments_dw.py`.

**Quy trình nạp (đã chạy thật, các lệnh dưới đây đã được xác nhận hoạt động đúng):**
```bash
docker compose up -d postgres-dw
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -v ON_ERROR_STOP=1 < sql/001_create_star_schema.sql
uv run scripts/load/load.py                       # nap fact_sales/dim_product that vao DW
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -v ON_ERROR_STOP=1 < sql/004_market_basket_rules.sql
uv run scripts/mining/apriori_mba.py --source dw
uv run scripts/mining/load_apriori_dw.py
```

**Bằng chứng kiểm thử thật (postgres-dw sạch, nạp lại từ đầu trong phiên kiểm thử 2026-10-28):**

- `sql/001_create_star_schema.sql` + `uv run scripts/load/load.py` trên `postgres-dw` sạch (mới
  tạo container) → `fact_sales=1.003.670` dòng, `revenue=19.659.453,86`, `dim_date=604,
  dim_customer=5.853, dim_product=4.905, dim_country=43` — khớp 100% số liệu đã công bố trước
  đây (`PROJECT_CONTEXT.md` mục 3.3.f).
- `uv run scripts/mining/apriori_mba.py --source dw` (lần đầu tiên chạy nhánh `dw` thật) →
  **kết quả giống hệt tuyệt đối** nhánh `--source csv`: 39.536 hóa đơn, 323 frequent itemset
  ({1: 265, 2: 58}), 101 luật, 101/101 lift > 1, cùng chính xác top-15 luật theo lift.
- `sql/004_market_basket_rules.sql` chạy lần 1: `CREATE TABLE` + 2 `CREATE INDEX` không lỗi.
  **Chạy lại lần 2** (idempotency DDL): `NOTICE: ... already exists, skipping` cho cả bảng lẫn
  2 index, **không lỗi**, dữ liệu đã nạp (101 dòng) **không bị mất**.
  `\d market_basket_rules` xác nhận đúng PK/UNIQUE/4 CHECK constraint/2 index như thiết kế.
- `uv run scripts/mining/load_apriori_dw.py` lần 1 → UPSERT đúng **101/101** dòng,
  `SELECT count(*) FROM market_basket_rules` = 101.
  **Chạy lại lần 2 (idempotency UPSERT)** với cùng input → vẫn **101** dòng (không tăng, không
  trùng) — đúng chiến lược `ON CONFLICT (antecedent_items, consequent_items) DO UPDATE`.
- **Kiểm tra tính hợp lệ dữ liệu trên DW thật** (`SELECT ... FROM market_basket_rules`):
  0 luật tự tham chiếu (`antecedent_items = consequent_items`), 0 item rỗng/NULL, 0 vi phạm
  `confidence < support` (xác nhận 2 cột không bị nhầm lẫn — vì `confidence =
  support/antecedent_support` và `antecedent_support ≤ 1` nên về lý thuyết `confidence ≥
  support` luôn đúng), 0 dòng support/confidence ngoài khoảng (0,1], 0 lift âm, **0 cặp
  `(antecedent_items, consequent_items)` trùng lặp**. `support ∈ [0,0201; 0,0361]`
  (khớp ngưỡng `min_support=0.02`), `confidence ∈ [0,3289; 0,8364]` (khớp `min_confidence=0.30`),
  `lift ∈ [2,92; 24,44]` (toàn bộ dương).
- **Trường hợp không có luật nào (biên):** chạy `--source dw --min-support 0.99` →
  `0` frequent itemset, `0` luật, file CSV output 0 dòng, **không lỗi** (exit code 0). Chạy
  `load_apriori_dw.py` với file 0 dòng này → phát hiện `df.empty`, in cảnh báo rõ ràng, **không
  gọi tới DB, không xóa 101 dòng đã nạp trước đó** (xác nhận lại `SELECT count(*)` vẫn = 101 sau
  bước này) — đúng thiết kế "không có gì để nạp thì dừng sớm, không phá dữ liệu cũ".
- Sau khi test xong, chạy lại `apriori_mba.py --source dw` với tham số mặc định để phục hồi
  `data/processed/apriori_*.csv` về đúng 101 luật (tránh để lại artefact từ test biên 0 luật).

## 9. Giới hạn đã biết (không còn mục "chưa kiểm thử" cho phần lõi)

- `max_len ≥ 3` với `min_support` thấp (ví dụ 0.01) chạy được (13,2s, xem mục 5) nhưng **chưa
  thử nghiệm với `min_support` rất thấp hơn nữa** (ví dụ 0.005) — có thể chậm hơn đáng kể do vài
  hóa đơn bán buôn cực lớn (tối đa 1.109 sản phẩm/hóa đơn); nếu cần, nên thêm tùy chọn giới hạn
  kích thước basket tối đa (chưa triển khai, vì ở ngưỡng mặc định không cần thiết).
- Chưa thêm chart Superset cho `market_basket_rules` — ngoài phạm vi yêu cầu Phase 8.1 (chỉ cần
  luật kết hợp với support/confidence/lift hợp lý, không bắt buộc có dashboard). Có thể làm sau
  theo đúng pattern `scripts/superset/provision_superset.py` nếu nhóm muốn hiển thị.
- Dữ liệu dùng để kiểm thử (`datalake/processed/2026-10-23/fact_sales.csv` nạp lại vào
  `postgres-dw` sạch trong phiên kiểm thử) là snapshot đã có sẵn trên máy, không chạy lại
  `scripts/transform/transform.py` trong lượt kiểm thử này — không cần thiết vì mục tiêu là xác
  nhận Apriori/tích hợp DW hoạt động đúng trên dữ liệu đã qua Transform, không phải kiểm thử lại
  Transform (đã kiểm thử ở Phase 2.3, không đổi).
- Container `postgres-dw` dùng cho kiểm thử **đã dừng (`docker compose stop`) sau khi hoàn tất**
  để không chiếm tài nguyên nền — dữ liệu vẫn còn trong volume Docker (`khodl_pg_dw_data`),
  chạy lại `docker compose up -d postgres-dw` sẽ thấy đầy đủ dữ liệu đã nạp (không mất).

## 10. Lệnh chạy (tóm tắt)

```bash
# Khong can Docker (doc snapshot datalake/processed/ moi nhat):
uv run scripts/mining/apriori_mba.py --source csv

# Tuy chinh nguong (vi du nhieu luat hon, cho phep itemset 3 san pham):
uv run scripts/mining/apriori_mba.py --source csv --min-support 0.01 --min-confidence 0.2 --max-len 3

# Kien truc chinh thuc (can Docker + postgres-dw dang chay):
uv run scripts/mining/apriori_mba.py --source dw

# Unit test (khong can Docker, khong can du lieu that):
uv run scripts/mining/test_apriori_mba.py -v

# Tich hop DW (can Docker dang chay - da chay that + xac nhan idempotent, xem muc 8):
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -f - < sql/004_market_basket_rules.sql
uv run scripts/mining/apriori_mba.py --source dw
uv run scripts/mining/load_apriori_dw.py
```

**Tái lập kết quả mục 6:** chạy đúng lệnh đầu tiên (`--source csv`, không tham số khác — dùng
toàn bộ giá trị mặc định `min_support=0.02, min_confidence=0.30, max_len=2`) trên
`datalake/processed/2026-10-23/fact_sales.csv` (hoặc thư mục `datalake/processed/<ngày>/` mới
nhất nếu đã chạy lại Transform) sẽ cho đúng 323 frequent itemset / 101 luật như mục 6.

## 11. Dashboard Superset — ĐÃ TẠO và xác nhận qua API (2026-10-28, lượt kiểm thử thứ 3)

Dashboard **riêng**, tách biệt với dashboard "Online Retail II - Tong quan kinh doanh" (Phase
6) — không sửa/không xóa dashboard đó. Tạo qua REST API bằng
[`scripts/superset/provision_superset.py`](../scripts/superset/provision_superset.py) (đã mở
rộng, tái sử dụng connection `Retail DW` + pattern idempotent có sẵn), kiểm thử bằng
[`scripts/superset/verify_charts.py`](../scripts/superset/verify_charts.py) (đã mở rộng để hỗ
trợ `query_mode=aggregate` và `echarts_timeseries_scatter`).

**Dataset:** đăng ký trực tiếp bảng `market_basket_rules` (không qua view trung gian — bảng đã
có đủ cột cần thiết).

**5 chart (tất cả đã xác minh qua `/api/v1/chart/data` thật, số liệu khớp `psql` trực tiếp):**

| # | Tên chart | Loại (`viz_type`) | Nội dung | Kết quả xác minh thật |
|---|---|---|---|---|
| 1 | Apriori - KPI tong quan | `table` (aggregate) | Tổng số luật, số luật lift>1, confidence TB, lift TB | `101 / 101 / 0,4856 / 9,179` |
| 2 | Apriori - Top 10 luat theo Lift | `table` (raw, sort theo `lift` giảm dần) | antecedent/consequent/support/confidence/lift | Top 1: `22697→22698`, lift 24,44 — khớp mục 6 |
| 3 | Apriori - Top 10 luat theo Confidence | `table` (raw, sort theo `confidence` giảm dần) | Như trên | Top 1: `22698→22697`, confidence 0,836 |
| 4 | Apriori - Scatter Support vs Confidence | `echarts_timeseries_scatter` | X=`support` (Generic X-Axis, không phải cột thời gian), Y=AVG(`confidence`) | 54 điểm (xem ghi chú dưới) |
| 5 | Apriori - Bang chi tiet luat ket hop | `table` (raw, sort theo `lift` giảm dần) | Toàn bộ 5 cột, `row_limit=200` | **101/101 dòng**, đúng tổng số luật |

**Ghi chú thiết kế chart Scatter:** Superset 4.1.1 (bản đang dùng) **không có viz_type nào hỗ
trợ đồng thời 3 chiều liên tục độc lập** (X, Y, kích thước bubble theo `lift`) — đã kiểm tra
thực tế, không giả định. `echarts_timeseries_scatter` thuộc họ "timeseries" của Superset: trục X
là khóa GROUP BY (có thể là cột bất kỳ nhờ tính năng "Generic X-Axis", không bắt buộc cột thời
gian — đã xác minh hoạt động đúng qua `/api/v1/chart/data`), (các) metric là trục Y, **không có
tham số bubble-size cho chiều thứ 3**. Vì vậy:
- Chart dùng X=`support`, Y=`AVG(confidence)` — đúng yêu cầu "support so với confidence".
- `lift` **không** được mã hóa trực quan trong chart này (đã thử nhiều phương án, không có dạng
  nào trong bản Superset hiện tại hỗ trợ cả 3 chiều cùng lúc) — nhưng vẫn xem được đầy đủ qua
  chart #1 (KPI, lift trung bình) và chart #2/#5 (giá trị lift cụ thể từng luật).
- **54 điểm thay vì 101:** một vài luật có `support` trùng giá trị sau khi làm tròn 6 chữ số
  thập phân (`NUMERIC(8,6)`) — các luật đó được Superset GROUP BY gộp lại, hiển thị
  `AVG(confidence)` tại điểm X chung đó (tính đúng theo công thức, không bịa số liệu, nhưng mất
  một phần độ chi tiết so với xem từng luật riêng lẻ — bù lại bằng chart #5 "Bảng chi tiết").
- Đã xác nhận `"echarts_timeseries_scatter"` là key viz **thật sự tồn tại** trong bundle JS của
  Superset image đang chạy (`grep` thấy trong
  `/app/superset/static/assets/8986.*.entry.js` bên trong container) — không phải key tự đặt.

**Idempotency đã xác nhận thật:** chạy `provision_superset.py` 2 lần liên tiếp → lần 2 toàn bộ
in `Da cap nhat ...` (PUT, không POST mới); `GET /api/v1/dashboard`, `/api/v1/chart`,
`/api/v1/dataset` sau 2 lần chạy vẫn đúng **2 dashboard, 11 chart (6 cũ + 5 mới), 6 dataset (5
cũ + 1 mới)** — không trùng lặp. Dashboard "Online Retail II - Tong quan kinh doanh" (Phase 6)
xác nhận **không đổi** (vẫn đúng 6 chart cũ, không bị thêm/mất chart nào).

**Chưa kiểm thử (cần mở giao diện thủ công):** toàn bộ xác minh ở trên qua REST API
(`/api/v1/chart/data`, không phải qua UI) — môi trường phát triển **không có trình duyệt**
(giống hạn chế đã ghi nhận cho dashboard Phase 6, xem `docs/06_superset_dashboard.md` mục 6).
**Chưa xác nhận bằng mắt** cách dashboard/chart hiển thị thực tế trong UI (bố cục, màu sắc, chú
giải trục, tooltip) — đặc biệt là chart Scatter vì đây là viz_type phức tạp nhất. Người dùng cần
mở `http://localhost:8088/superset/dashboard/2/` để tự kiểm tra bằng mắt trước khi dùng cho báo
cáo/demo chính thức.

**Lệnh chạy:**
```bash
uv run scripts/superset/provision_superset.py   # tao/cap nhat ca 2 dashboard, idempotent
uv run scripts/superset/verify_charts.py        # doi chieu toan bo 11 chart voi du lieu that
```
