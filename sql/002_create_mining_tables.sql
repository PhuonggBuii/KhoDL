-- Phase 5/6 - Bang ket qua khai pha du lieu (RFM + K-Means) va view phuc vu Superset.
-- Chay SAU sql/001_create_star_schema.sql. An toan chay lai: IF NOT EXISTS / CREATE OR REPLACE,
-- KHONG DROP bang/du lieu nao.
--
-- Chay (PowerShell, thu muc goc repo):
--   docker cp sql\002_create_mining_tables.sql khodl-postgres-dw:/tmp/mining.sql
--   docker exec khodl-postgres-dw psql -U dw_user -d retail_dw -v ON_ERROR_STOP=1 -f /tmp/mining.sql

-- =====================================================================
-- CUSTOMER_SEGMENTS - ket qua RFM + nhan cum cho TUNG khach hang.
-- Chi gom khach co Customer ID (giao dich customer_key = NULL khong vao RFM - quyet dinh #1).
-- cluster_best : cum theo K tot nhat theo Silhouette (xem silhouette_scores.is_best)
-- cluster_k4   : cum theo K = 4 (K nghiep vu da chot), segment_name la nhan dien giai cua nhom.
-- =====================================================================
CREATE TABLE IF NOT EXISTS customer_segments (
    customer_key   BIGINT PRIMARY KEY REFERENCES dim_customer (customer_key),
    customer_id    INTEGER        NOT NULL,
    recency        INTEGER        NOT NULL,   -- so ngay tu lan mua gan nhat den ngay tham chieu
    frequency      INTEGER        NOT NULL,   -- so hoa don duy nhat
    monetary       NUMERIC(14, 2) NOT NULL,   -- tong doanh thu cua khach
    cluster_best   SMALLINT       NOT NULL,
    cluster_k4     SMALLINT       NOT NULL,
    segment_name   VARCHAR(40)    NOT NULL,
    created_at     TIMESTAMP      NOT NULL DEFAULT now()
);

COMMENT ON TABLE customer_segments IS 'Ket qua RFM + K-Means cho tung khach hang (chi khach co Customer ID).';
COMMENT ON COLUMN customer_segments.segment_name IS 'Ten cum K=4 do nhom dien giai theo dac diem trung binh R/F/M, khong co san trong du lieu.';

-- =====================================================================
-- SILHOUETTE_SCORES - diem Silhouette theo K (danh gia chat luong phan cum).
-- =====================================================================
CREATE TABLE IF NOT EXISTS silhouette_scores (
    k            SMALLINT      PRIMARY KEY,
    score        NUMERIC(8, 4) NOT NULL,
    is_best      BOOLEAN       NOT NULL DEFAULT FALSE,  -- K co Silhouette cao nhat
    is_business  BOOLEAN       NOT NULL DEFAULT FALSE,  -- K duoc chon theo y nghia nghiep vu (4)
    created_at   TIMESTAMP     NOT NULL DEFAULT now()
);

COMMENT ON TABLE silhouette_scores IS 'Silhouette Score cho K = 2..10 cua lan chay K-Means moi nhat.';

-- =====================================================================
-- VIEW cho Superset
-- =====================================================================

-- Doanh thu, so don, so khach theo thang (tinh ca giao dich khong co Customer ID).
CREATE OR REPLACE VIEW vw_monthly_revenue AS
SELECT make_date(d.year::int, d.month::int, 1)  AS month_start,
       SUM(f.revenue)                           AS revenue,
       COUNT(DISTINCT f.invoice_no)             AS orders,
       COUNT(DISTINCT f.customer_key)           AS customers
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY make_date(d.year::int, d.month::int, 1);

-- Tong hop theo nhom khach hang (K = 4): so khach, ty le, R/F/M trung binh.
CREATE OR REPLACE VIEW vw_segment_summary AS
SELECT segment_name,
       COUNT(*)                                                AS customers,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)      AS pct_customers,
       ROUND(AVG(recency), 1)                                  AS avg_recency,
       ROUND(AVG(frequency), 1)                                AS avg_frequency,
       ROUND(AVG(monetary), 1)                                 AS avg_monetary,
       ROUND(SUM(monetary), 2)                                 AS total_monetary
FROM customer_segments
GROUP BY segment_name;
