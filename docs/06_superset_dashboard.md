# Phase 6 — Dashboard Superset

Toàn bộ nội dung dưới đây được tạo qua **Superset REST API** (không thao tác UI thủ công),
chạy thật và kiểm thử thật trên môi trường Docker Compose hiện có. Script nguồn:
[`scripts/superset/provision_superset.py`](../scripts/superset/provision_superset.py).

## 1. Driver PostgreSQL cho Superset (bắt buộc trước khi kết nối DW)

Image `apache/superset:4.1.1` **không có sẵn** driver `psycopg2`. Đã thêm vào
`docker-compose.yml` (service `superset`, trong `command`):

```bash
pip install --no-cache-dir "psycopg2-binary>=2.9.13"
```

Chạy trước `superset db upgrade` mỗi khi container khởi động (idempotent — cài lại không lỗi
nếu đã có). Để áp dụng, cần **recreate** container (không phải chỉ restart):

```bash
docker compose up -d superset
```

Lệnh này chỉ tạo lại container `superset` (giữ nguyên `superset_home` volume, không đụng tới
`postgres-dw`/`postgres-source`/Airflow/`daugiahtpt`). Đã xác minh thật: `docker exec
khodl-superset python -c "import psycopg2; print(psycopg2.__version__)"` → `2.9.13`.

## 2. Kết nối Database (Retail DW)

- Tên: `Retail DW`
- `sqlalchemy_uri`: `postgresql+psycopg2://<PG_DW_USER>:<PG_DW_PASSWORD>@postgres-dw:5432/<PG_DW_DB>`
  (dùng hostname nội bộ Docker `postgres-dw` vì Superset và `postgres-dw` cùng network
  `khodl_net` — giống pattern `PG_DW_HOST=postgres-dw` đã dùng cho Airflow ở Phase 2.2/2.4).
- Đã test qua API `POST /api/v1/database/test_connection/` → `{"message": "OK"}` trước khi tạo.

## 3. Dataset (đăng ký trực tiếp OLAP view + bảng `customer_segment`)

| Dataset | Nguồn | Dùng cho chart |
|---|---|---|
| `v_olap_sales_daily` | View OLAP (Phase 4.1, roll-up thời gian) | Doanh thu theo tháng |
| `v_olap_sales_by_country` | View OLAP (Phase 4.2, Slice quốc gia) | Doanh thu theo quốc gia |
| `v_olap_top10_products` | View OLAP (Phase 4.2, Slice sản phẩm) | Top 10 sản phẩm |
| `customer_segment` | Bảng (Phase 5.5) | Số khách hàng & RFM theo phân khúc |
| `v_olap_kpi_summary` | View OLAP (Phase 4.4) | KPI tổng quan |

Superset tự nhận diện `full_date` (kiểu `DATE` trong `v_olap_sales_daily`) làm cột thời gian
chính (`is_dttm=True`, `main_dttm_col=full_date`) — không cần chỉnh tay.

## 4. Chart đã tạo (6 chart, tất cả truy vấn thật từ DW)

| # | Tên chart | Loại | Dataset | Đối chiếu số liệu thật (đã kiểm thử) |
|---|---|---|---|---|
| 1 | Doanh thu theo tháng | `echarts_timeseries_line` | `v_olap_sales_daily` | 25 tháng (12/2009–12/2011), tổng = **19.659.453,86** — khớp `fact_sales` |
| 2 | Doanh thu theo quốc gia | `dist_bar` (top 15) | `v_olap_sales_by_country` | United Kingdom cao nhất = **16.806.060,61** |
| 3 | Top 10 sản phẩm theo doanh thu | `dist_bar` | `v_olap_top10_products` | Cao nhất: REGENCY CAKESTAND 3 TIER = 330.590,32 |
| 4 | Số khách hàng theo phân khúc | `pie` | `customer_segment` | VIP 947 + trung thành 1.875 + mới/ít tương tác 1.397 + nguy cơ rời bỏ 1.634 = **5.853** |
| 5 | RFM trung bình theo phân khúc | `dist_bar` (2 metric) | `customer_segment` | Khớp đúng bảng cụm ở `docs/05_rfm_kmeans_dw.md` (VIP: Monetary≈12.819,5, Frequency≈22,2) |
| 6 | KPI tổng quan kho dữ liệu | `table` | `v_olap_kpi_summary` | revenue=19.659.453,86, fact_row_count=1.003.670, customer_count=5.853, product_count=4.905, country_count=43 |

**Cách kiểm thử đã chạy thật:** [`scripts/superset/verify_charts.py`](../scripts/superset/verify_charts.py)
đọc lại `params` đã lưu của từng chart (không giả định), tự dựng `query_context` tương ứng,
gọi `POST /api/v1/chart/data` (API mà Explore/Dashboard Superset dùng để vẽ chart), rồi in kết
quả. Toàn bộ số liệu khớp với truy vấn `psql` trực tiếp trên `postgres-dw` đã chạy ở các phase
trước — không có số liệu giả định/minh họa.

## 5. Dashboard tổng hợp

- Tên: **"Online Retail II - Tong quan kinh doanh"**, `id=1`,
  `http://localhost:8088/superset/dashboard/1/`
- Bố cục (4 hàng): Hàng 1 = Doanh thu theo tháng (full-width) · Hàng 2 = Doanh thu theo quốc
  gia + Top 10 sản phẩm · Hàng 3 = Số khách hàng theo phân khúc + RFM trung bình theo phân
  khúc · Hàng 4 = KPI tổng quan (full-width).
- **Bộ lọc (native filter)** đã cấu hình trong `json_metadata`:
  - "Khoang thoi gian" (`filter_time`) trên `v_olap_sales_daily.full_date`.
  - "Quoc gia" (`filter_select`, multi-select) trên `v_olap_sales_by_country.country_name`.
- Idempotent: script `provision_superset.py` kiểm tra theo tên trước khi tạo (database/
  dataset/chart/dashboard) — đã chạy lại 2 lần, lần 2 chỉ "cập nhật" (PUT), không tạo trùng
  object nào (đã xác minh: vẫn đúng 1 database, 5 dataset, 6 chart, 1 dashboard).

## 6. Phần cần bạn làm thủ công (không thể tự động hóa an toàn từ môi trường này)

Môi trường này **không có browser/trình duyệt** để mở Superset UI và chụp ảnh minh chứng trực
quan (đã thử `WebFetch` — không hỗ trợ `localhost`/ứng dụng cần đăng nhập session). Toàn bộ
kiểm thử trên đã xác nhận **đúng qua REST API** (số liệu, cấu trúc dashboard, idempotency),
nhưng **chưa có xác nhận bằng mắt** là dashboard hiển thị đẹp/đúng bố cục trong trình duyệt.

Bạn cần:
1. Mở `http://localhost:8088`, đăng nhập (`SUPERSET_ADMIN_USER`/`SUPERSET_ADMIN_PASSWORD`
   trong `.env`).
2. Vào **Dashboards → "Online Retail II - Tong quan kinh doanh"**.
3. Kiểm tra trực quan: 6 chart hiển thị đúng dữ liệu, 2 bộ lọc ("Khoang thoi gian", "Quoc gia")
   hoạt động (đổi quốc gia → chart "Doanh thu theo quốc gia" lọc theo đúng lựa chọn).
4. Nếu muốn chỉnh bố cục/màu/tên hiển thị tiếng Việt có dấu (hiện dùng không dấu để tránh lỗi
   encode qua API) — sửa trực tiếp trên UI, **không cần chạy lại script** (script chỉ tạo mới,
   không ghi đè nếu bạn đã sửa tay — tuy nhiên chạy lại `provision_superset.py` sẽ PUT đè
   `params`/`position_json`/`json_metadata` về bản gốc, nên **không chạy lại script sau khi đã
   tùy biến tay** trừ khi muốn reset).
5. Chụp ảnh minh chứng (Phase 7.2 cần "ảnh chụp màn hình minh chứng") cho báo cáo/demo.

## 7. Lệnh tái lập/tái kiểm thử

```bash
# Dam bao driver Postgres co trong container Superset (1 lan, sau khi sua docker-compose.yml)
docker compose up -d superset

# Tao/cap nhat toan bo database+dataset+chart+dashboard (idempotent)
uv run scripts/superset/provision_superset.py

# Kiem tra lai moi chart tra ve du lieu thuc, khop so lieu DW
uv run scripts/superset/verify_charts.py
```

## 8. Giới hạn đã biết

- Tên chart/dashboard/segment không dùng tiếng Việt có dấu — tránh rủi ro mã hóa khi gửi qua
  REST API trong môi trường này; có thể đổi tay qua UI nếu cần cho bản demo cuối.
- Dashboard được tạo với layout grid cơ bản (4 hàng), chưa tinh chỉnh màu sắc/theme — đủ để
  trình bày, có thể chỉnh thêm qua UI nếu muốn đẹp hơn khi bảo vệ.
- Chưa thêm chart riêng cho Dice (Quốc gia×Năm)/Pivot (`v_olap_dice_country_year`,
  `v_olap_pivot_country_by_year`) — 2 view này được thiết kế để minh họa khái niệm OLAP cho
  báo cáo/thuyết trình (xem `docs/04_olap.md`), không bắt buộc phải lên dashboard; có thể thêm
  nếu giảng viên yêu cầu minh họa trực tiếp trên Superset.
