# Phase 4 — OLAP trên Star Schema (PostgreSQL DW)

Trạng thái: **đầy đủ 4 nhóm OLAP (Roll-up/Drill-down, Slice, Dice, Pivot) đã triển khai và
kiểm thử thật trên dữ liệu thật** (2026-10-24). DDL: [`sql/002_olap_views.sql`](../sql/002_olap_views.sql)
(13 view). Chạy trên dữ liệu đã nạp ở Phase 2.4: `fact_sales` = **1.003.670 dòng**, tổng
revenue = **19.659.453,86**.

> Viết cho người chưa từng xem qua phần OLAP của dự án — mỗi mục có giải thích ý nghĩa nghiệp
> vụ trước khi đến SQL, để dùng được khi thuyết trình.

## 1. Mục tiêu

OLAP (On-Line Analytical Processing) giúp trả lời các câu hỏi kinh doanh trên Data Warehouse
mà không cần viết lại logic tính từ đầu mỗi lần — 4 phép toán kinh điển:

- **Roll-up/Drill-down**: thu nhỏ/phóng to mức chi tiết theo 1 chiều (ở đây: thời gian —
  Năm → Quý → Tháng → Ngày).
- **Slice**: "cắt" dữ liệu theo 1 chiều cụ thể (ví dụ: chỉ xem theo quốc gia).
- **Dice**: "cắt" theo **nhiều chiều cùng lúc** (ví dụ: quốc gia **và** năm).
- **Pivot**: xoay 1 chiều thành cột để ra **ma trận** dễ đọc (ví dụ: mỗi quốc gia 1 hàng, mỗi
  năm 1 cột).

Toàn bộ tính trên Star Schema đã có dữ liệu thật (Phase 3), **không tính từ file CSV baseline**
của Phase 0/5.

## 2. Quy ước dùng nhất quán trong mọi view (xem chi tiết trong comment đầu
   [`sql/002_olap_views.sql`](../sql/002_olap_views.sql))

- `revenue` lấy trực tiếp từ `fact_sales.revenue` (cột `GENERATED ALWAYS AS
  round(quantity*unit_price,2) STORED`) — **không tính lại bằng công thức khác**.
- `fact_row_count` = số dòng `fact_sales` (1 dòng = 1 dòng hàng trong 1 hóa đơn).
- `invoice_count` = `COUNT(DISTINCT invoice_no)` — **khác** `fact_row_count` vì 1 hóa đơn có
  thể có nhiều dòng hàng. Mọi view báo cáo **cả hai** để không nhầm lẫn.
- Các view theo thời gian/quốc gia/sản phẩm/KPI tính trên **toàn bộ** `fact_sales` (gồm cả
  dòng thiếu `Customer ID`, vì doanh thu các dòng này vẫn thật và hợp lệ).
- View theo **khách hàng** chỉ tính trên dòng có `customer_key IS NOT NULL`; phần bị loại có
  view riêng (`v_olap_sales_missing_customer`) để không "âm thầm làm mất dữ liệu" — xem mục 4.

## 3. Bảng/cột sử dụng (xác minh trực tiếp từ DB)

| Bảng | Cột dùng |
|---|---|
| `fact_sales` | `invoice_no`, `date_key` (NOT NULL), `customer_key` (**NULLABLE**), `product_key` (NOT NULL), `country_key` (NOT NULL), `quantity`, `unit_price`, `revenue` (GENERATED) |
| `dim_date` | `date_key`, `full_date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_name` |
| `dim_customer` | `customer_key`, `customer_id` |
| `dim_product` | `product_key`, `stock_code`, `description` |
| `dim_country` | `country_key`, `country_name` |

## 4. Roll-up / Drill-down (4.1) — phân cấp thời gian đầy đủ Năm → Quý → Tháng → Ngày

**Ý nghĩa nghiệp vụ:** xem doanh thu tổng quát ở mức Năm rồi "zoom" dần xuống Quý, Tháng, Ngày
— trả lời câu hỏi "doanh thu tăng/giảm khi nào, chi tiết đến đâu".

| View | Grain | Cột thêm |
|---|---|---|
| `v_olap_sales_yearly` | Năm | `revenue_growth_pct_yoy` (so với năm trước) |
| `v_olap_sales_quarterly` | Quý | — |
| `v_olap_sales_monthly` | Tháng | `revenue_growth_pct_mom` (so với tháng liền trước) |
| `v_olap_sales_daily` | **Ngày** (mới bổ sung lượt này) | `day_name` |

Mỗi view tính **độc lập** trực tiếp từ `fact_sales JOIN dim_date` (không cộng dồn từ view
khác), để tránh rủi ro đếm trùng `invoice_count` khi gộp.

**SQL minh họa (Ngày, bảng mới bổ sung):**
```sql
SELECT d.full_date, d.year, d.quarter, d.month, d.day, d.day_name,
       SUM(f.revenue) AS total_revenue, SUM(f.quantity) AS total_quantity,
       COUNT(*) AS fact_row_count, COUNT(DISTINCT f.invoice_no) AS invoice_count
FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.year, d.quarter, d.month, d.day, d.day_name
ORDER BY d.full_date;
```

**Kết quả kiểm thử thật (2026-10-24):**
- `v_olap_sales_daily`: tổng `revenue` = **19.659.453,86**, tổng `fact_row_count` = **1.003.670**
  trên **604 ngày có giao dịch** — khớp tuyệt đối với `fact_sales` trực tiếp.
- Đối chiếu Ngày → Tháng: tổng revenue của tháng 01/2010 tính từ `v_olap_sales_daily`
  (`WHERE year=2010 AND month=1`) = **612.605,50**, bằng đúng giá trị `v_olap_sales_monthly`
  cho cùng tháng — phân cấp nhất quán.
- `v_olap_sales_yearly`/`v_olap_sales_quarterly`/`v_olap_sales_monthly` (đã kiểm thử ở lượt
  trước, verify lại lần này): tổng đều = 19.659.453,86 / 1.003.670.

## 5. Slice (4.2 — phần 1) — cắt theo 1 chiều

| View | Chiều cắt | Lưu ý |
|---|---|---|
| `v_olap_sales_by_country` | Quốc gia | — |
| `v_olap_product_revenue` + `v_olap_top10_products` | Sản phẩm | — |
| `v_olap_customer_revenue` + `v_olap_top10_customers` | Khách hàng | **chỉ** dòng có Customer ID |
| `v_olap_sales_missing_customer` (mới bổ sung) | — | làm RÕ phần bị loại khỏi slice khách hàng |

**Cách xử lý `customer_key IS NULL` (yêu cầu làm rõ của task này):** các giao dịch không xác
định được khách hàng (22,77% dataset gốc, giữ lại từ Phase 2.3 theo quyết định đã đề xuất) vẫn
là doanh thu thật và được tính đầy đủ ở mọi view khác (thời gian, quốc gia, sản phẩm, KPI).
Khi phân tích riêng theo khách hàng thì **loại** các dòng này ra (không gán được vào ai) —
nhưng **không để mất dấu**: `v_olap_sales_missing_customer` tách riêng đúng phần bị loại
(**2.577.368,09** / 226.813 dòng), và `v_olap_customer_revenue` (**17.082.085,77** / 776.857
dòng) + phần này = đúng **19.659.453,86** / 1.003.670 — xác nhận không cộng trùng, không mất
dữ liệu, chỉ tách bạch rõ ràng.

## 6. Dice (4.2 — phần 2, mới bổ sung) — cắt theo 2 chiều kết hợp

**Ý nghĩa nghiệp vụ:** trả lời câu hỏi cụ thể hơn Slice — ví dụ "quốc gia nào tăng/giảm doanh
thu qua từng năm", không chỉ "quốc gia nào doanh thu cao nhất" (Slice) hay "năm nào doanh thu
cao nhất" (Roll-up) riêng lẻ.

**View:** `v_olap_dice_country_year` — doanh thu theo **Quốc gia × Năm** (chọn tổ hợp này vì
chỉ có 3 năm dữ liệu (2009–2011), cho ma trận gọn, dễ trình bày).

```sql
SELECT c.country_name, d.year,
       SUM(f.revenue) AS total_revenue, SUM(f.quantity) AS total_quantity,
       COUNT(*) AS fact_row_count, COUNT(DISTINCT f.invoice_no) AS invoice_count
FROM fact_sales f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.country_name, d.year
ORDER BY c.country_name, d.year;
```

**Kết quả kiểm thử thật:** tổng `total_revenue` theo từng năm từ `v_olap_dice_country_year`
(gộp theo `year`) khớp **tuyệt đối từng năm** với `v_olap_sales_yearly`:

| Năm | Dice (gộp theo năm) | Roll-up `v_olap_sales_yearly` |
|---|---|---|
| 2009 | 798.732,03 | 798.732,03 |
| 2010 | 9.382.375,63 | 9.382.375,63 |
| 2011 | 9.478.346,20 | 9.478.346,20 |

JOIN đồng thời 2 dimension (`dim_country` + `dim_date`) vẫn giữ đúng **1.003.670** dòng khi
đối chiếu với `fact_sales` (không nhân bản) — xác nhận bằng
`SELECT count(*) FROM fact_sales JOIN dim_date ... JOIN dim_country ...` = 1.003.670.

## 7. Pivot (4.3, mới bổ sung) — ma trận Quốc gia × Năm

**Ý nghĩa nghiệp vụ:** từ kết quả Dice ở trên, "xoay" chiều Năm thành cột để có 1 bảng duy
nhất, mỗi quốc gia 1 hàng — dễ đưa vào báo cáo/slide hơn là danh sách dài (quốc gia, năm,
revenue) của Dice.

**View:** `v_olap_pivot_country_by_year` — dùng `SUM(CASE WHEN ...)` (không cần extension
`tablefunc`/`crosstab`, dễ đọc, dễ sửa). **3 cột năm (2009, 2010, 2011) là giá trị thật đã
xác minh từ `dim_date`** (`SELECT DISTINCT year FROM dim_date` → đúng 3 giá trị này, dataset
chỉ có dữ liệu từ 12/2009 đến 12/2011 nên không bịa thêm năm nào khác).

```sql
SELECT c.country_name,
       SUM(CASE WHEN d.year = 2009 THEN f.revenue ELSE 0 END) AS revenue_2009,
       SUM(CASE WHEN d.year = 2010 THEN f.revenue ELSE 0 END) AS revenue_2010,
       SUM(CASE WHEN d.year = 2011 THEN f.revenue ELSE 0 END) AS revenue_2011,
       SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.country_name
ORDER BY total_revenue DESC;
```

**Mẫu kết quả thật (8 quốc gia doanh thu cao nhất):**

| country_name | revenue_2009 | revenue_2010 | revenue_2011 | total_revenue |
|---|---|---|---|---|
| United Kingdom | 727.342,62 | 8.057.767,93 | 8.020.950,06 | 16.806.060,61 |
| EIRE | 19.296,49 | 347.915,07 | 266.057,60 | 633.269,16 |
| Netherlands | 15.045,73 | 259.592,82 | 275.134,86 | 549.773,41 |
| Germany | 8.768,27 | 182.383,32 | 192.137,41 | 383.289,00 |
| France | 5.873,69 | 129.613,91 | 175.998,69 | 311.486,29 |

**Kết quả kiểm thử thật — đối chiếu tổng hàng/cột:**
- **Tổng hàng:** `0 dòng` trong `v_olap_pivot_country_by_year` có
  `revenue_2009 + revenue_2010 + revenue_2011 ≠ total_revenue` (đã kiểm tra toàn bộ 43 quốc
  gia) — mọi hàng tự đối soát đúng.
- **Tổng cột:** `SUM(revenue_2009)=798.732,03`, `SUM(revenue_2010)=9.382.375,63`,
  `SUM(revenue_2011)=9.478.346,20` — khớp tuyệt đối với `v_olap_sales_yearly` theo từng năm
  (bảng ở mục 6). `SUM(total_revenue)=19.659.453,86` — khớp tổng toàn kho.

## 8. KPI tổng quan (4.4, giữ từ lượt trước)

`v_olap_kpi_summary`: tổng revenue, tổng quantity, số dòng fact, số hóa đơn, số khách hàng
(khác NULL), số sản phẩm, số quốc gia có giao dịch, số dòng thiếu Customer ID.

## 9. Phân loại 13 view trong `sql/002_olap_views.sql`

| Nhóm | View | Mục đích |
|---|---|---|
| **Dashboard core** (bind trực tiếp 1 chart Superset) | `v_olap_sales_{daily,monthly,quarterly,yearly}`, `v_olap_sales_by_country`, `v_olap_product_revenue`, `v_olap_top10_products`, `v_olap_customer_revenue`, `v_olap_top10_customers`, `v_olap_sales_missing_customer`, `v_olap_kpi_summary` | Grain đơn giản, mỗi view = 1 chart/KPI |
| **Minh họa OLAP** (giải thích báo cáo/thuyết trình, vẫn dùng được cho Superset nếu cần) | `v_olap_dice_country_year`, `v_olap_pivot_country_by_year` | Minh họa khái niệm Dice/Pivot |

## 10. Tổng hợp kết quả kiểm thử thực tế (2026-10-24)

| # | Kiểm tra | Kết quả |
|---|---|---|
| 1 | `fact_sales` không đổi sau khi thêm view | 1.003.670 dòng, revenue 19.659.453,86 — **không đổi** |
| 2 | Roll-up Năm/Quý/Tháng/**Ngày** khớp tổng | Cả 4 mức đều = 19.659.453,86 / 1.003.670 |
| 3 | Ngày → Tháng nhất quán | Tháng 01/2010: 612.605,50 (cả 2 cách tính) |
| 4 | Slice khách hàng: NULL vs non-NULL | 17.082.085,77 (776.857 dòng) + 2.577.368,09 (226.813 dòng) = 19.659.453,86 — không mất, không trùng |
| 5 | Dice Quốc gia×Năm khớp Roll-up Năm | Khớp đúng cả 3 năm |
| 6 | Pivot — tổng hàng | 0/43 dòng sai |
| 6 | Pivot — tổng cột | Khớp đúng cả 3 năm + tổng toàn kho |
| 7 | JOIN không nhân bản dòng (kể cả JOIN 2 dimension cùng lúc cho Dice/Pivot) | `count(*)` sau JOIN = 1.003.670 đúng bằng `fact_sales` |
| 8 | Chạy lại file SQL lần 2 (idempotent) | `CREATE OR REPLACE VIEW` × 13 — không lỗi, không tạo đối tượng trùng |
| — | Spot-check thủ công (top 1 sản phẩm/khách hàng, từ lượt trước) | Khớp 100% |

**Không phát hiện sai lệch nào cần điều tra/sửa trong toàn bộ Phase 4.**

## 11. Cách chạy lại

```bash
# Tạo/cập nhật toàn bộ 13 view (an toàn chạy lại nhiều lần)
docker exec -i khodl-postgres-dw psql -U <PG_DW_USER_trong_.env> -d <PG_DW_DB_trong_.env> -v ON_ERROR_STOP=1 < sql/002_olap_views.sql

# Roll-up / Drill-down
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT * FROM v_olap_sales_yearly;"
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT * FROM v_olap_sales_daily ORDER BY full_date LIMIT 10;"

# Slice
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT * FROM v_olap_sales_by_country;"
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT * FROM v_olap_sales_missing_customer;"

# Dice
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT * FROM v_olap_dice_country_year ORDER BY total_revenue DESC LIMIT 10;"

# Pivot
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT * FROM v_olap_pivot_country_by_year LIMIT 10;"

# KPI tổng quan
docker exec khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -c "SELECT * FROM v_olap_kpi_summary;"
```

(Giá trị mặc định `.env.example`: user `dw_user`, db `retail_dw`.)

## 12. Giới hạn còn lại

- **Chưa cấu hình Superset** kết nối tới các view này (Phase 6, ngoài phạm vi Phase 4).
- Pivot hiện chỉ làm **Quốc gia × Năm** (do chỉ có 3 năm dữ liệu, phù hợp để pivot gọn). Pivot
  **Thời gian × Sản phẩm** chưa làm vì 4.905 sản phẩm là quá nhiều cột cho 1 pivot đọc được —
  nếu cần, nên pivot Top N sản phẩm (ví dụ top 10) × Năm/Quý, chưa triển khai trong lượt này.
- `revenue_growth_pct_mom` so sánh với **tháng liền trước theo thứ tự thời gian** (không phải
  "cùng kỳ năm trước") — nếu cần so sánh cùng kỳ, cần thêm cột `LAG(..., 12)` khi có yêu cầu
  cụ thể.
- Không tạo view riêng cho RFM/K-Means hoặc dashboard — đúng giới hạn phạm vi Phase 4.
