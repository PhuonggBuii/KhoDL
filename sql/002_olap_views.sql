-- Phase 4 — OLAP views tren Star Schema da co du lieu thuc (fact_sales, dim_*).
-- Thiet ke day du + ket qua kiem thu: xem docs/04_olap.md.
--
-- An toan chay lai: toan bo dung CREATE OR REPLACE VIEW, KHONG DROP/TRUNCATE bang nao,
-- KHONG sua DDL cua fact_sales/dim_* (sql/001_create_star_schema.sql).
--
-- Quy uoc dung nhat quan trong toan bo view:
--   - revenue lay truc tiep tu fact_sales.revenue (cot GENERATED = round(quantity*unit_price,2)),
--     KHONG tinh lai theo cong thuc khac.
--   - "fact_row_count" = so dong fact_sales (grain: 1 dong hang trong 1 hoa don).
--   - "invoice_count"  = so hoa don DUY NHAT (COUNT DISTINCT invoice_no) - khac fact_row_count
--     vi 1 hoa don co the co nhieu dong hang.
--   - Phan tich theo khach hang (v_olap_customer_revenue, v_olap_top10_customers) CHI tinh
--     tren cac dong co customer_key IS NOT NULL (loai giao dich thieu Customer ID) - cac view
--     khac (thoi gian, quoc gia, san pham, KPI tong) van tinh tren TOAN BO fact_sales, bao
--     gom ca dong thieu Customer ID, vi doanh thu cac dong nay la thuc va hop le.
--
-- Phan loai view trong file nay (xem docs/04_olap.md muc 3):
--   - "Dashboard core" (muc 1-9): grain don gian, dung truc tiep lam Superset dataset/chart
--     (Roll-up thoi gian Nam/Quy/Thang/Ngay, Slice quoc gia/san pham/khach hang, KPI tong).
--   - "View minh hoa OLAP" (Dice, Pivot o cuoi file): minh hoa khai niem Dice/Pivot cho bao
--     cao/thuyet trinh, van co the dung lam Superset dataset neu can nhung muc dich chinh la
--     giai thich nghiep vu, khong phai 1 chart don gian.
--
-- Chay: docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -f - < sql/002_olap_views.sql

-- =====================================================================
-- 1. Doanh thu / so luong / so dong fact / so hoa don theo THANG — grain
--    nho nhat cho roll-up thoi gian. Co cot so sanh voi thang truoc (lien
--    tiep theo thu tu year,month - khong phai "cung ky nam truoc").
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_sales_monthly AS
SELECT
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count,
    LAG(SUM(f.revenue)) OVER (ORDER BY d.year, d.month) AS prev_month_revenue,
    ROUND(
        100.0 * (SUM(f.revenue) - LAG(SUM(f.revenue)) OVER (ORDER BY d.year, d.month))
        / NULLIF(LAG(SUM(f.revenue)) OVER (ORDER BY d.year, d.month), 0)
    , 2)                            AS revenue_growth_pct_mom
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.month, d.month_name
ORDER BY d.year, d.month;

COMMENT ON VIEW v_olap_sales_monthly IS
    'OLAP roll-up doanh thu/so luong/so dong fact/so hoa don theo THANG, kem % tang truong so voi thang lien truoc. Tinh tren toan bo fact_sales (ca dong thieu Customer ID).';

-- =====================================================================
-- 2. Cung roll-up o muc QUY (doc lap voi view thang, tranh loi dem trung
--    invoice_count khi cong don tu view khac).
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_sales_quarterly AS
SELECT
    d.year,
    d.quarter,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;

COMMENT ON VIEW v_olap_sales_quarterly IS
    'OLAP roll-up doanh thu/so luong/so dong fact/so hoa don theo QUY. Tinh tren toan bo fact_sales.';

-- =====================================================================
-- 3. Cung roll-up o muc NAM, kem % tang truong so voi nam truoc (YoY).
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_sales_yearly AS
SELECT
    d.year,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count,
    LAG(SUM(f.revenue)) OVER (ORDER BY d.year) AS prev_year_revenue,
    ROUND(
        100.0 * (SUM(f.revenue) - LAG(SUM(f.revenue)) OVER (ORDER BY d.year))
        / NULLIF(LAG(SUM(f.revenue)) OVER (ORDER BY d.year), 0)
    , 2)                            AS revenue_growth_pct_yoy
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;

COMMENT ON VIEW v_olap_sales_yearly IS
    'OLAP roll-up doanh thu/so luong/so dong fact/so hoa don theo NAM, kem % tang truong YoY. Tinh tren toan bo fact_sales.';

-- =====================================================================
-- 4. Doanh thu theo QUOC GIA (Slice) — quoc gia lay theo tung dong giao
--    dich (fact_sales.country_key), khong suy tu khach hang.
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_sales_by_country AS
SELECT
    c.country_name,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count
FROM fact_sales f
JOIN dim_country c ON f.country_key = c.country_key
GROUP BY c.country_name
ORDER BY total_revenue DESC;

COMMENT ON VIEW v_olap_sales_by_country IS
    'OLAP slice doanh thu theo quoc gia (lay tu tung dong giao dich). Tinh tren toan bo fact_sales.';

-- =====================================================================
-- 5. Doanh thu theo SAN PHAM (xep hang day du; "Top 10" la view rieng
--    phia duoi, LIMIT tu view nay de de doi size top N sau).
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_product_revenue AS
SELECT
    p.stock_code,
    p.description,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.stock_code, p.description
ORDER BY total_revenue DESC;

COMMENT ON VIEW v_olap_product_revenue IS
    'OLAP doanh thu theo san pham, xep hang giam dan. Tinh tren toan bo fact_sales.';

CREATE OR REPLACE VIEW v_olap_top10_products AS
SELECT * FROM v_olap_product_revenue
ORDER BY total_revenue DESC
LIMIT 10;

COMMENT ON VIEW v_olap_top10_products IS 'Top 10 san pham theo doanh thu (tu v_olap_product_revenue).';

-- =====================================================================
-- 6. Doanh thu theo KHACH HANG — CHI tinh tren dong co customer_key
--    IS NOT NULL (loai giao dich thieu Customer ID khoi phan tich khach
--    hang, dung yeu cau nghiep vu; INNER JOIN dim_customer da tu loai
--    NULL, WHERE ben duoi ghi lai de ro rang khong phu thuoc ngam dinh).
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_customer_revenue AS
SELECT
    cu.customer_id,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count
FROM fact_sales f
JOIN dim_customer cu ON f.customer_key = cu.customer_key
WHERE f.customer_key IS NOT NULL
GROUP BY cu.customer_id
ORDER BY total_revenue DESC;

COMMENT ON VIEW v_olap_customer_revenue IS
    'OLAP doanh thu theo khach hang, xep hang giam dan. CHI tinh tren giao dich co Customer ID (loai dong customer_key IS NULL).';

CREATE OR REPLACE VIEW v_olap_top10_customers AS
SELECT * FROM v_olap_customer_revenue
ORDER BY total_revenue DESC
LIMIT 10;

COMMENT ON VIEW v_olap_top10_customers IS 'Top 10 khach hang theo doanh thu (tu v_olap_customer_revenue, da loai giao dich thieu Customer ID).';

-- =====================================================================
-- 7. KPI tong quan toan kho (tinh tren toan bo fact_sales, bao gom ca
--    dong thieu Customer ID cho doanh thu/so luong; so khach hang chi
--    dem cac customer_key khac NULL vi COUNT(DISTINCT ...) tu bo qua NULL).
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_kpi_summary AS
SELECT
    SUM(f.revenue)                  AS total_revenue,
    SUM(f.quantity)                 AS total_quantity,
    COUNT(*)                        AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)    AS invoice_count,
    COUNT(DISTINCT f.customer_key)  AS customer_count,
    COUNT(DISTINCT f.product_key)   AS product_count,
    COUNT(DISTINCT f.country_key)   AS country_count,
    SUM(CASE WHEN f.customer_key IS NULL THEN 1 ELSE 0 END) AS fact_rows_missing_customer
FROM fact_sales f;

COMMENT ON VIEW v_olap_kpi_summary IS
    'KPI tong quan toan kho: tong doanh thu, tong so luong, so dong fact, so hoa don, so khach hang (khac NULL), so san pham, so quoc gia co giao dich.';

-- =====================================================================
-- 8. Drill-down xuong muc NGAY — hoan thien phan cap Nam -> Quy -> Thang
--    -> Ngay (4.1). Doc lap voi cac view thang/quy/nam o tren (tinh truc
--    tiep tu fact_sales, khong cong don tu view khac) de tranh rui ro dem
--    trung invoice_count.
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_sales_daily AS
SELECT
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.day,
    d.day_name,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.year, d.quarter, d.month, d.day, d.day_name
ORDER BY d.full_date;

COMMENT ON VIEW v_olap_sales_daily IS
    'OLAP drill-down muc NGAY (grain min cua phan cap thoi gian Nam->Quy->Thang->Ngay). Tinh tren toan bo fact_sales.';

-- =====================================================================
-- 9. Bo sung rieng cho dong thieu Customer ID — lam RO (khong chi ngam
--    dinh qua INNER JOIN) phan doanh thu/so dong BI LOAI khoi
--    v_olap_customer_revenue, de doi chieu: v_olap_customer_revenue +
--    view nay = v_olap_kpi_summary (toan kho).
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_sales_missing_customer AS
SELECT
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count
FROM fact_sales f
WHERE f.customer_key IS NULL;

COMMENT ON VIEW v_olap_sales_missing_customer IS
    'Doanh thu/so dong cua cac giao dich THIEU Customer ID (customer_key IS NULL) - phan bi loai khoi v_olap_customer_revenue. Cong voi v_olap_customer_revenue se ra dung tong toan kho (v_olap_kpi_summary).';

-- =====================================================================
-- DICE (4.2) — ket hop 2 chieu: QUOC GIA x NAM. Day la view MINH HOA
-- khai niem Dice (khac "dashboard core" o tren vi it dung truc tiep cho
-- 1 chart don, thuong dung de giai thich bao cao/thuyet trinh); van co
-- the dung lam Superset dataset nếu can.
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_dice_country_year AS
SELECT
    c.country_name,
    d.year,
    SUM(f.revenue)                 AS total_revenue,
    SUM(f.quantity)                AS total_quantity,
    COUNT(*)                       AS fact_row_count,
    COUNT(DISTINCT f.invoice_no)   AS invoice_count
FROM fact_sales f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.country_name, d.year
ORDER BY c.country_name, d.year;

COMMENT ON VIEW v_olap_dice_country_year IS
    'OLAP Dice: doanh thu theo QUOC GIA x NAM (ket hop 2 chieu). View minh hoa, dung de giai thich bao cao - gom toan bo fact_sales.';

-- =====================================================================
-- PIVOT (4.3) — ma tran doanh thu QUOC GIA (hang) x NAM (cot), dung
-- SUM(CASE WHEN ...) de khong phu thuoc extension "tablefunc"/crosstab.
-- Cac nam 2009/2010/2011 la GIA TRI THUC xac minh tu dim_date (SELECT
-- DISTINCT year FROM dim_date -> dung dung 3 gia tri nay, khong bia).
-- Day la view MINH HOA khai niem Pivot (xem ghi chu Dice o tren).
-- =====================================================================
CREATE OR REPLACE VIEW v_olap_pivot_country_by_year AS
SELECT
    c.country_name,
    SUM(CASE WHEN d.year = 2009 THEN f.revenue ELSE 0 END) AS revenue_2009,
    SUM(CASE WHEN d.year = 2010 THEN f.revenue ELSE 0 END) AS revenue_2010,
    SUM(CASE WHEN d.year = 2011 THEN f.revenue ELSE 0 END) AS revenue_2011,
    SUM(f.revenue)                                          AS total_revenue
FROM fact_sales f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.country_name
ORDER BY total_revenue DESC;

COMMENT ON VIEW v_olap_pivot_country_by_year IS
    'OLAP Pivot: ma tran doanh thu QUOC GIA (hang) x NAM 2009/2010/2011 (cot), dung SUM(CASE WHEN). total_revenue = tong 3 cot nam, dung de doi chieu hang. View minh hoa, gom toan bo fact_sales.';
