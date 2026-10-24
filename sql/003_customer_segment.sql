-- Phase 5.5 — Bang ket qua phan cum RFM/K-Means cho PostgreSQL Data Warehouse
-- (postgres-dw / retail_dw). Thiet ke day du: xem docs/05_rfm_kmeans_dw.md.
--
-- Quyet dinh: bang RIENG "customer_segment" (KHONG ALTER dim_customer) — tranh
-- thay doi Star Schema da kiem thu o Phase 3/4 (sql/001_create_star_schema.sql
-- KHONG bi sua boi file nay).
--
-- An toan chay lai: toan bo dung IF NOT EXISTS, KHONG DROP bang/du lieu nao.
-- Nap du lieu thuc te do scripts/mining/load_segments_dw.py thuc hien (UPSERT
-- theo customer_key), KHONG lam o file SQL nay.
--
-- Chay: docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -f - < sql/003_customer_segment.sql

-- =====================================================================
-- CUSTOMER_SEGMENT — grain: 1 dong = 1 khach hang DA duoc tinh RFM va phan cum
-- (customer_key IS NOT NULL trong fact_sales). Khach hang thieu Customer ID
-- khong co dong trong bang nay (dung quy dinh loai khoi RFM theo khach hang -
-- xem docs/03_star_schema.md muc 6, quyet dinh #1).
-- =====================================================================
CREATE TABLE IF NOT EXISTS customer_segment (
    customer_key   BIGINT        PRIMARY KEY REFERENCES dim_customer (customer_key),
    recency        INTEGER       NOT NULL,
    frequency      INTEGER       NOT NULL,
    monetary       NUMERIC(14, 2) NOT NULL,
    k_used         SMALLINT      NOT NULL,
    cluster_label  SMALLINT      NOT NULL,
    segment_name   VARCHAR(50)   NOT NULL,
    computed_at    TIMESTAMP     NOT NULL DEFAULT now(),
    CONSTRAINT chk_customer_segment_recency_nonneg CHECK (recency >= 0),
    CONSTRAINT chk_customer_segment_frequency_pos CHECK (frequency > 0),
    CONSTRAINT chk_customer_segment_monetary_nonneg CHECK (monetary >= 0),
    CONSTRAINT chk_customer_segment_cluster_label_nonneg CHECK (cluster_label >= 0)
);

COMMENT ON TABLE customer_segment IS 'Phase 5.5 - Ket qua RFM + K-Means (nguon: fact_sales JOIN dim_customer, scripts/mining/rfm_dw.py + cluster_dw.py). 1 dong/khach hang da phan cum.';
COMMENT ON COLUMN customer_segment.k_used IS 'So cum (K) duoc dung de tao cluster_label nay (business_k - xem scripts/mining/cluster_dw.py). Co the doi giua cac lan chay lai mo hinh.';
COMMENT ON COLUMN customer_segment.cluster_label IS 'Nhan cum tu KMeans (chi so 0..k_used-1). KHONG co thu tu co dinh giua cac lan chay - dung segment_name de dien giai.';
COMMENT ON COLUMN customer_segment.segment_name IS 'Ten doc hieu suy ra tu so lieu R/F/M tuong doi giua cac cum (heuristic - xem docs/05_rfm_kmeans_dw.md), vi du: Khach hang VIP, Khach hang trung thanh, Nguy co roi bo, Khach hang moi / it tuong tac.';
COMMENT ON COLUMN customer_segment.computed_at IS 'Thoi diem UPSERT ban ghi nay (cap nhat moi lan chay lai scripts/mining/load_segments_dw.py).';

CREATE INDEX IF NOT EXISTS idx_customer_segment_cluster_label ON customer_segment (cluster_label);
CREATE INDEX IF NOT EXISTS idx_customer_segment_segment_name ON customer_segment (segment_name);
