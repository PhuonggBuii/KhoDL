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

## 11. Dashboard Superset — ĐÃ TẠO, PHÁT HIỆN LỖI HIỂN THỊ VÀ ĐÃ SỬA (2026-10-28)

Dashboard **riêng**, tách biệt với dashboard "Online Retail II - Tong quan kinh doanh" (Phase
6) — không sửa/không xóa dashboard đó. Tạo qua REST API bằng
[`scripts/superset/provision_superset.py`](../scripts/superset/provision_superset.py) (đã mở
rộng, tái sử dụng connection `Retail DW` + pattern idempotent có sẵn), kiểm thử bằng
[`scripts/superset/verify_charts.py`](../scripts/superset/verify_charts.py) (đã mở rộng tương
ứng).

**Dataset:** 2 dataset trên cùng connection `Retail DW`:
- `market_basket_rules` (bảng gốc) — chỉ dùng cho chart KPI (không cần tên sản phẩm).
- `v_market_basket_rules_named` (view mới, xem mục 13) — dùng cho 4 chart còn lại, có sẵn
  `antecedent_label`/`consequent_label` (tên sản phẩm + mã) và `confidence_pct`.

> **Lượt tạo đầu tiên (xem mục 12) có lỗi: chỉ chart "KPI tong quan" hiển thị, 4 chart còn lại
> KHÔNG hiện ra trong trình duyệt dù API báo thành công.** Đã điều tra và sửa. **Lượt sau đó
> (xem mục 13) thêm tên sản phẩm thật** thay vì chỉ hiện mã StockCode.

**5 chart hiện tại (sau 2 lần sửa, tất cả đã xác minh qua `/api/v1/chart/data` thật):**

| # | Tên chart | Loại (`viz_type`) | Dataset | Nội dung | Kết quả xác minh thật |
|---|---|---|---|---|---|
| 1 | Apriori - KPI tong quan | `table` (aggregate) | `market_basket_rules` | Tổng số luật, số luật lift>1, confidence TB, lift TB | `101 / 101 / 0,4856 / 9,179` |
| 2 | Apriori - Top 10 luat theo Lift | `table` (aggregate, sort `Lift` giảm dần) | `v_market_basket_rules_named` | Tên+mã sản phẩm 2 vế, Support, **Confidence (%)**, Lift | Top 1: `PINK REGENCY TEACUP AND SAUCER (22698) → GREEN REGENCY TEACUP AND SAUCER (22697)`, confidence 83,64%, lift 24,44 |
| 3 | Apriori - Top 10 luat theo Confidence | `table` (aggregate, sort `Confidence (%)` giảm dần) | `v_market_basket_rules_named` | Như trên | Top 1 giống chart #2 (cùng luật cao nhất cả 2 chỉ số) |
| 4 | Apriori - Phan bo Support, Confidence, Lift theo luat | `table` (aggregate, sort `Lift` tăng dần) | `v_market_basket_rules_named` | Tên+mã sản phẩm 2 vế, Support, Confidence, Lift — toàn bộ 101 luật | **101/101 dòng**, lift nhỏ nhất 2,92 ở đầu bảng |
| 5 | Apriori - Bang chi tiet luat ket hop | `table` (aggregate, sort `Lift` giảm dần) | `v_market_basket_rules_named` | Như trên | **101/101 dòng**, lift lớn nhất 24,44 ở đầu bảng |

**Lưu ý:** chart #4 (trước đây dự kiến là scatter plot X=support/Y=confidence) đã đổi thành
**bảng** sau khi phát hiện `echarts_timeseries_scatter` không hiển thị được trong môi trường
này (xem mục 12) — đây là 1 trong 2 lựa chọn đã nêu từ đầu ("nếu Superset hỗ trợ... chọn loại
biểu đồ thay thế phù hợp").

**Idempotency đã xác nhận thật:** chạy `provision_superset.py` nhiều lần liên tiếp (qua cả 3
lượt sửa) → các lần sau toàn bộ in `Da cap nhat ...` (PUT, không POST mới); `GET
/api/v1/dashboard`, `/api/v1/chart`, `/api/v1/dataset` luôn đúng **2 dashboard, 11 chart (6 cũ +
5 mới), 7 dataset (5 cũ + 2 mới)** — không trùng lặp. **Ngoại lệ đã xảy ra và đã sửa:** lượt
thêm tên sản phẩm (mục 13) tạm thời tạo ra 1 chart trùng (id=12) do `lookup_name` dùng tên đã
lỗi thời — đã phát hiện và xóa ngay, xem mục 14. Dashboard "Online Retail II - Tong quan kinh
doanh" (Phase 6) xác nhận **không đổi** trong suốt cả 3 lượt sửa (vẫn đúng 6 chart cũ).

**Chưa kiểm thử (cần mở giao diện thủ công):** môi trường phát triển **không có trình duyệt** —
toàn bộ xác minh ở trên qua REST API (`/api/v1/chart/data`), không phải qua UI thật. **Chưa có
xác nhận bằng mắt** rằng cả 5 chart hiển thị đúng, bao gồm việc tên sản phẩm có hiển thị đúng vị
trí cột trong giao diện hay không. Người dùng cần **tải lại trang** (Ctrl+F5/hard refresh để
tránh cache cũ) tại `http://localhost:8088/superset/dashboard/2/` và tự kiểm tra bằng mắt trước
khi dùng cho báo cáo/demo chính thức — nếu vẫn còn vấn đề, xem mục 12 phần "Nếu vẫn còn lỗi".

**Lệnh chạy:**
```bash
uv run scripts/superset/provision_superset.py   # tao/cap nhat ca 2 dashboard, idempotent
uv run scripts/superset/verify_charts.py        # doi chieu toan bo 11 chart voi du lieu that
```

## 12. Sự cố "chỉ hiện KPI, 4 chart còn lại không hiện" — điều tra và khắc phục (2026-10-28)

### Triệu chứng (người dùng báo cáo)
Mở `http://localhost:8088/superset/dashboard/2/` chỉ thấy tiêu đề + chart "Apriori - KPI tong
quan"; 4 chart còn lại (2 bảng Top-10, Scatter, bảng chi tiết) **không hiện ra** — mặc dù lượt
tạo trước đó báo "đã tạo 5 chart" và các lệnh kiểm thử qua API đều trả kết quả thành công.

### Các bước điều tra thực tế đã làm (chỉ qua REST API + `docker exec`, không có trình duyệt)

1. **Kiểm tra association chart↔dashboard** — `GET /api/v1/dashboard/2`, `GET
   /api/v1/dashboard/2/charts`, và `GET /api/v1/chart/<id>` cho từng chart riêng lẻ (field
   `dashboards`): **cả 5 chart đều liên kết đúng với dashboard id=2**, hai chiều, không có chart
   nào bị thiếu liên kết.
2. **Kiểm tra `position_json`** — so sánh `position_json` của dashboard 2 (mới) với dashboard 1
   (Phase 6, đã chạy ổn định) từng trường một: cấu trúc `ROOT_ID → GRID → ROW → CHART` **giống
   hệt về hình dạng**, mọi tham chiếu ID đều nhất quán (không có tham chiếu treo/thiếu node),
   `meta.chartId` của cả 5 CHART node đều đúng (7, 8, 9, 10, 11). **Kết luận: layout KHÔNG lỗi**
   — nếu lỗi layout/traversal thì chart #1 (cũng nằm trong cùng 1 GRID, cùng cơ chế duyệt cây)
   cũng sẽ không hiện, nhưng thực tế chart #1 **có** hiện.
3. **Kiểm tra dữ liệu từng chart qua `/api/v1/chart/data`** (dùng đúng `params` đã lưu, giống
   cách `verify_charts.py` làm): **cả 5 chart đều trả dữ liệu đúng, không lỗi** — kể cả 4 chart
   "không hiện". Điều này loại trừ khả năng lỗi ở tầng dữ liệu/SQL.
4. **So sánh có hệ thống với 1 chart "table" CŨ đã hoạt động ổn định** (chart #6 "KPI tong quan
   kho du lieu", Phase 6): phát hiện khác biệt duy nhất giữa 3 chart bảng bị lỗi
   (`apriori_top_lift`, `apriori_top_confidence`, `apriori_detail`) và chart cũ đang hoạt động
   là chart cũ dùng `"query_mode": "raw"` + `"order_by_cols": []` (**RỖNG**), còn 3 chart lỗi
   dùng `"order_by_cols"` **có giá trị** (để sắp theo `lift`/`confidence`). Đây là manh mối cụ
   thể, không phải suy đoán chung chung.
5. **Chart Scatter (`echarts_timeseries_scatter`)** không cùng nhóm nguyên nhân trên (khác hẳn
   viz_type) — không tìm được bằng chứng xác định rõ nguyên nhân chỉ qua API (dữ liệu vẫn đúng
   khi gọi thủ công), và **không có cách nào kiểm chứng việc render phía client chỉ bằng
   API/log** (đã kiểm tra `docker logs khodl-superset` — không có log truy cập HTTP/JS error nào
   được ghi ra do cấu hình gunicorn hiện tại không bật access log).

### Nguyên nhân xác định (có bằng chứng) và chưa xác định chắc chắn (có suy luận)

- **Xác định có bằng chứng cụ thể:** 3 chart bảng dùng `query_mode: "raw"` + `order_by_cols` có
  giá trị không hiển thị, trong khi chart bảng cùng loại nhưng `order_by_cols` rỗng thì hiển thị
  bình thường — sự khác biệt duy nhất đó là `order_by_cols`.
- **Chưa xác định chắc chắn (chỉ là giả thuyết hợp lý):** chart Scatter không hiển thị **có thể**
  do cùng nhóm nguyên nhân (thiếu tham số bắt buộc trong `params` mà chỉ frontend kiểm tra, API
  backend không kiểm tra) — **không khẳng định chắc chắn được nếu không mở trình duyệt**.

### Khắc phục đã áp dụng (tối thiểu, không đổi dashboard Phase 6)

File sửa: [`scripts/superset/provision_superset.py`](../scripts/superset/provision_superset.py),
[`scripts/superset/verify_charts.py`](../scripts/superset/verify_charts.py). KHÔNG đổi
`sql/004_market_basket_rules.sql`, KHÔNG đổi dữ liệu trong `market_basket_rules`, KHÔNG đổi bất
kỳ chart/dashboard nào của Phase 6.

- 3 chart bảng (`apriori_top_lift`, `apriori_top_confidence`, `apriori_detail`): đổi
  `query_mode` từ `"raw"` sang `"aggregate"`, dùng `groupby` (antecedent_items,
  consequent_items) + `metrics` (MAX support/confidence/lift) + `timeseries_limit_metric` +
  `order_desc` để sắp xếp top-N — **đây CHÍNH LÀ cơ chế sắp xếp đã dùng thành công trong các
  chart `dist_bar` ổn định của Phase 6** ("Doanh thu theo quoc gia", "Top 10 san pham theo doanh
  thu", xem `scripts/superset/provision_superset.py::build_charts`), không phải cơ chế mới tự
  nghĩ ra.
- Chart Scatter (`echarts_timeseries_scatter`): **thay bằng bảng aggregate cùng cơ chế trên**
  (đổi tên thành "Apriori - Phan bo Support, Confidence, Lift theo luat", sắp theo `lift` tăng
  dần) — vì không có cách xác minh viz_type đó render đúng trong môi trường không có trình
  duyệt này, trong khi bảng aggregate thì **có bằng chứng cụ thể** (chart #1 dùng đúng cơ chế
  này và đã được người dùng xác nhận hiển thị). **KHÔNG tạo chart mới**: dùng tham số
  `lookup_name` mới thêm vào `ensure_chart()` để tìm đúng chart cũ theo tên cũ
  ("Apriori - Scatter Support vs Confidence") rồi `PUT` cập nhật tại chỗ (cùng `chart_id=10`) —
  đã xác nhận qua `GET /api/v1/chart/` sau khi chạy lại: vẫn đúng 11 chart, không có chart thừa.

### Đã xác minh lại qua API sau khi sửa (không chỉ dựa vào "provision thành công")

- `uv run scripts/superset/provision_superset.py`: tất cả 11 chart + 2 dashboard in
  `Da cap nhat` (không có dòng `Da tao` nào mới) → xác nhận không tạo trùng.
- `GET /api/v1/dashboard/` → vẫn đúng 2 dashboard; `GET /api/v1/chart/` → vẫn đúng 11 chart,
  chart id=10 đổi tên đúng như mong đợi, các id khác giữ nguyên.
- `uv run scripts/superset/verify_charts.py` (đã cập nhật để dựng đúng query cho
  `query_mode=aggregate` kèm `timeseries_limit_metric`/`order_desc`): **cả 11 chart trả dữ liệu
  đúng và ĐÃ SẮP XẾP ĐÚNG THỨ TỰ** — chart Top-10-Lift và bảng chi tiết trả lift giảm dần bắt đầu
  từ 24,44; chart Top-10-Confidence trả confidence giảm dần bắt đầu từ 0,836; chart "Phân bố"
  trả lift **tăng dần** bắt đầu từ 2,92 (khác chiều sắp xếp so với bảng chi tiết, đúng thiết kế).
- `GET /api/v1/chart/2`, `/api/v1/chart/3`, `/api/v1/chart/5` (dist_bar của Phase 6, tham chiếu
  cho cơ chế sắp xếp vừa áp dụng) vẫn trả `viz_type`/`params` **không đổi** — xác nhận không vô
  tình sửa nhầm chart Phase 6 khi chỉnh sửa code dùng chung (hàm `ensure_chart`).

### Nếu vẫn còn lỗi sau khi tải lại trang

Việc sửa ở trên dựa trên **suy luận có bằng chứng** (so sánh với chart đã hoạt động), **không
phải xác nhận trực tiếp bằng trình duyệt** — môi trường này không có trình duyệt. Nếu người dùng
tải lại trang (hard refresh) và **vẫn còn** chart không hiện:
1. Mở Developer Tools (F12) → tab Console/Network khi tải dashboard, tìm lỗi JS hoặc request
   `/api/v1/chart/data` trả về mã lỗi — đây là thông tin môi trường này không thu thập được.
2. Gửi lại thông báo lỗi cụ thể (tên chart, lỗi hiện trong Console nếu có) để điều tra tiếp —
   khi đó cần nguyên nhân khác ngoài `order_by_cols` (đã loại trừ qua so sánh ở mục 12).
3. Phương án dự phòng chắc chắn hoạt động: mọi dữ liệu luật kết hợp vẫn truy vấn được trực tiếp
   qua `psql`/`SELECT * FROM market_basket_rules` (xem mục 8) hoặc qua file CSV
   `data/processed/apriori_association_rules.csv` — không phụ thuộc vào Superset.

## 13. Hiển thị tên sản phẩm thật thay vì StockCode (2026-10-28, lượt sửa thứ 3)

### Yêu cầu
`antecedent_items`/`consequent_items` trong `market_basket_rules` chỉ lưu StockCode (vd `22697`)
— người xem dashboard không biết đó là sản phẩm gì. Cần hiển thị tên sản phẩm thật (từ
`dim_product.description`) kèm mã, cho mọi bảng/chart luật kết hợp.

### Khảo sát schema thật trước khi sửa (không đoán tên cột)
- `market_basket_rules.antecedent_items`/`consequent_items`: kiểu `TEXT`, lưu **danh sách
  StockCode nối bằng `"|"`, đã sắp xếp** (vd `"20724|22356"` nếu luật có 2 sản phẩm/vế — xem
  `scripts/mining/apriori_mba.py::build_baskets`/`generate_rules`). Tại thời điểm kiểm tra,
  **toàn bộ 101 luật đều có đúng 1 StockCode/vế** (`antecedent_size=1`, `consequent_size=1`
  100%, vì `apriori_mba.py` mặc định `--max-len 2`) — nhưng thiết kế xử lý vẫn phải đúng cho
  trường hợp nhiều StockCode/vế (nếu sau này chạy `--max-len 3+`), không giả định chỉ có 1 mã.
- `dim_product`: cột mã là `stock_code` (`VARCHAR(20)`, **UNIQUE**), cột tên là `description`
  (`VARCHAR(255)`, nullable — theo `docs/03_star_schema.md`, ~0,41% dòng gốc không có mô tả).
  `stock_code` UNIQUE → JOIN 1 StockCode chỉ khớp tối đa 1 dòng `dim_product`, không cần xử lý
  trường hợp "nhiều dòng cùng mã" (yêu cầu mục 3 của nhiệm vụ) — đã kiểm tra trực tiếp constraint
  `uq_dim_product_stock_code` trong `sql/001_create_star_schema.sql`, không suy đoán.

### Kiểm tra mức độ tra được tên (chạy thật trên `postgres-dw`, 2026-10-28)
```sql
-- Tach het StockCode xuat hien trong ca antecedent_items lan consequent_items,
-- LEFT JOIN dim_product, dem so ma tra duoc / khong tra duoc.
```
Kết quả thật: **45 StockCode duy nhất** xuất hiện trong 101 luật → **45/45 tra được tên (100%),
0 mã không tra được**. Không có `description` nào `NULL`/rỗng trong số 45 mã này. (Số liệu này
đặc thù cho lượt chạy Apriori hiện tại — nếu chạy lại Apriori với ngưỡng khác, bộ StockCode xuất
hiện có thể khác, nhưng cơ chế fallback "Unknown product" vẫn hoạt động đúng cho trường hợp đó.)

### Thiết kế: 1 view dùng chung, không viết lại logic cho từng chart
Thêm vào [`sql/004_market_basket_rules.sql`](../sql/004_market_basket_rules.sql) (không tạo file
mới, không sửa bảng `market_basket_rules` — chỉ thêm `CREATE OR REPLACE VIEW`):

```sql
CREATE OR REPLACE VIEW v_market_basket_rules_named AS ...
```

- Tách `antecedent_items`/`consequent_items` bằng `unnest(string_to_array(..., '|'))`, LEFT JOIN
  `dim_product`, rồi `string_agg(..., '; ' ORDER BY code)` **gộp lại đúng 1 dòng/luật** — xử lý
  đúng trường hợp nhiều sản phẩm/vế (hiển thị **đủ** các sản phẩm, không lấy riêng mã đầu tiên,
  đúng yêu cầu mục C của nhiệm vụ).
- Định dạng mỗi sản phẩm: `"<Tên mô tả> (<StockCode>)"`; nếu không tra được tên:
  `"Unknown product (StockCode: <mã>)"` — đúng định dạng đã yêu cầu, **không bịa tên**.
- Thêm cột `confidence_pct` (= `confidence * 100`, làm tròn 2 chữ số) phục vụ yêu cầu "Confidence
  (%)" ở 2 bảng Top-10 — **không sửa cột `confidence` gốc** (0..1), không sửa bất kỳ giá trị
  support/confidence/lift nào của thuật toán Apriori.
- **Đã kiểm tra thật: không nhân bản dòng.** `SELECT count(*) FROM v_market_basket_rules_named`
  = **101**, đúng bằng `SELECT count(*) FROM market_basket_rules` = 101 (xác nhận JOIN với
  `dim_product` không fan-out nhờ UNIQUE constraint trên `stock_code`, và `string_agg` gộp đúng
  theo `rule_id`).

### Thay đổi ở Superset (qua `provision_superset.py`, không chỉnh UI thủ công)
- Đăng ký thêm 1 dataset mới `v_market_basket_rules_named` (dataset `market_basket_rules` gốc
  **vẫn giữ nguyên**, chỉ còn dùng cho chart KPI không cần tên sản phẩm).
- 4 chart (Top-10 Lift, Top-10 Confidence, Phân bố, Bảng chi tiết) đổi `groupby` từ
  `antecedent_items`/`consequent_items` (StockCode thô) sang `antecedent_label`/
  `consequent_label` (tên+mã) của dataset mới — **giữ nguyên cơ chế sắp xếp
  `timeseries_limit_metric`/`order_desc`** đã sửa đúng ở mục 12, chỉ đổi dataset/cột nguồn.
- 2 bảng Top-10 dùng metric `confidence_pct` (hiển thị dạng %), bảng Phân bố/Chi tiết dùng
  `confidence` gốc (0..1) — đúng theo cách phân biệt nêu trong yêu cầu (mục A/B có "(%)", mục C
  không).

### Kiểm tra sau sửa (yêu cầu mục 4 của nhiệm vụ)
- **Số luật trước/sau:** không đổi — `market_basket_rules` = 101 dòng, `v_market_basket_rules_named`
  = 101 dòng, bảng "Bang chi tiet luat ket hop" (Superset) trả **101/101 dòng** qua
  `/api/v1/chart/data` (xem mục 11).
- **JOIN không nhân bản luật:** xác nhận bằng `count(*)` bằng nhau giữa view và bảng gốc (ở
  trên), **không phải suy diễn** từ UNIQUE constraint — đã đếm thật.
- **Tên sản phẩm hiển thị đúng StockCode:** đối chiếu trực tiếp — ví dụ luật lift cao nhất
  `22698 → 22697` hiển thị thành `"PINK REGENCY TEACUP AND SAUCER (22698)" →
  "GREEN REGENCY TEACUP AND SAUCER (22697)"`, khớp `dim_product` (`SELECT description FROM
  dim_product WHERE stock_code IN ('22697','22698')`).
- **Top 10 vẫn sắp xếp đúng:** đã xác nhận qua `/api/v1/chart/data` — Top-10-Lift trả lift giảm
  dần từ 24,4404; Top-10-Confidence trả Confidence (%) giảm dần từ 83,64.
- **API + idempotency:** chạy `provision_superset.py` lại sau khi sửa code (xem mục 14) → toàn
  bộ `Da cap nhat` (không `Da tao`); `GET /api/v1/chart/`, `/api/v1/dataset/` xác nhận đúng 11
  chart / 7 dataset, không trùng.
- **Giao diện trình duyệt:** **CHƯA kiểm tra được** (môi trường không có trình duyệt) — cần
  người dùng tự xác nhận tại `http://localhost:8088/superset/dashboard/2/` (hard refresh trước).

## 14. Sự cố tạo nhầm 1 chart trùng khi đổi dataset — phát hiện và xóa ngay (2026-10-28)

Khi áp dụng mục 13, lần chạy `provision_superset.py` **đầu tiên** vô tình **tạo thêm 1 chart
trùng tên** (`id=12`, trùng tên với chart `id=10` đã có) thay vì cập nhật chart cũ tại chỗ.

**Nguyên nhân:** code dùng `lookup_name="Apriori - Scatter Support vs Confidence"` (tên chart
**trước khi sửa lỗi hiển thị ở mục 12** — tại mục 12, chart này đã được đổi tên thành
`"Apriori - Phan bo Support, Confidence, Lift theo luat"`). Khi chạy lại ở mục 13, `find_one`
tìm theo tên CŨ không còn tồn tại → không tìm thấy → tạo chart MỚI (`id=12`) thay vì cập nhật
`id=10`. Cả 2 chart (10 và 12) cùng bị gắn vào dashboard — vi phạm đúng điều cấm "không tạo chart
trùng lặp".

**Phát hiện:** sau khi chạy, kiểm tra ngay `GET /api/v1/chart/` thấy 12 chart (thay vì 11) với 2
chart trùng tên; `GET /api/v1/dashboard/2` xác nhận cả 2 đều gắn vào dashboard — không chỉ dựa
vào log "provision thành công" của script.

**Khắc phục:** xóa `id=12` (`DELETE /api/v1/chart/12`, xác nhận `{"message": "OK"}`), sửa code
bỏ `lookup_name` đã lỗi thời (chart đã có tên đúng, không cần tìm theo tên cũ nữa), chạy lại
`provision_superset.py` — lần này `find_one` khớp đúng `id=10` theo tên hiện tại, in
`Da cap nhat chart 'Apriori - Phan bo...' (id=10)` (không tạo mới).

**Xác nhận đã sửa đúng:** `GET /api/v1/chart/` sau đó → đúng **11 chart**, không còn `id=12`,
không còn tên trùng; `GET /api/v1/chart/10` → chỉ gắn với **đúng 1 dashboard** (`id=2`); `GET
/api/v1/dashboard/2` → đúng **5 chart duy nhất** trong `position_json` và trong trường `charts`.
Chạy `provision_superset.py` thêm 1 lần nữa (lần 3) → toàn bộ `Da cap nhat`, không có `Da tao`
nào — xác nhận idempotent trở lại.

**Bài học áp dụng vào code:** tham số `lookup_name` của `ensure_chart()` chỉ an toàn khi dùng
**đúng 1 lần, ngay tại thời điểm đổi tên** — lần chạy sau đó phải bỏ `lookup_name` (để tìm theo
tên MỚI, hiện tại), không được giữ `lookup_name` trỏ về tên cũ vĩnh viễn. Đã sửa đúng trong
`scripts/superset/provision_superset.py` (xem comment tại chart `apriori_support_distribution`).
