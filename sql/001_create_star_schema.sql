-- Phase 3.2 — Star Schema cho PostgreSQL Data Warehouse (postgres-dw / retail_dw).
-- Thiet ke day du: xem docs/03_star_schema.md (grain, ly do thiet ke, quyet dinh cho nhom xac nhan).
--
-- An toan chay lai: toan bo dung IF NOT EXISTS, KHONG DROP bang/du lieu nao.
-- Chua nap du lieu nghiep vu o buoc nay (Phase 2.4 se lam o Airflow Load task).
--
-- Chay: docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -f - < sql/001_create_star_schema.sql
-- (hoac psql -f truc tiep neu co ket noi psql tu host toi localhost:5433)

-- =====================================================================
-- DIM_DATE — grain: 1 dong = 1 ngay lich.
-- day_of_week theo chuan ISO 8601 (1 = Thu Hai ... 7 = Chu Nhat).
-- =====================================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_date     DATE        NOT NULL,
    year          SMALLINT    NOT NULL,
    quarter       SMALLINT    NOT NULL,
    month         SMALLINT    NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    day           SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,
    day_name      VARCHAR(20) NOT NULL,
    week_of_year  SMALLINT    NOT NULL,
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date),
    CONSTRAINT chk_dim_date_quarter CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT chk_dim_date_month CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_dim_date_day CHECK (day BETWEEN 1 AND 31),
    CONSTRAINT chk_dim_date_dow CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_dim_date_week CHECK (week_of_year BETWEEN 1 AND 53)
);

COMMENT ON TABLE dim_date IS 'Chieu ngay, grain = 1 dong/ngay lich. Phuc vu rollup Nam->Quy->Thang->Ngay (Phase 4.1).';
COMMENT ON COLUMN dim_date.day_of_week IS 'ISO 8601: 1=Thu Hai ... 7=Chu Nhat.';

-- =====================================================================
-- DIM_CUSTOMER — chi dung Customer ID thuc te tu dataset, khong them
-- thuoc tinh gia (ten/tuoi/gioi tinh khong co trong Online Retail II).
-- =====================================================================
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id   INTEGER NOT NULL,
    CONSTRAINT uq_dim_customer_customer_id UNIQUE (customer_id)
);

COMMENT ON TABLE dim_customer IS 'Chieu khach hang. customer_id = cot "Customer ID" goc (12346-18287 trong dataset khao sat).';

-- =====================================================================
-- DIM_PRODUCT — StockCode + Description thuc te. description nullable
-- vi 0.41% dong goc khong co mo ta; thuat toan chon mo ta dai dien khi
-- 1 StockCode co nhieu mo ta khac nhau se quyet dinh o Phase 2.3.
-- =====================================================================
CREATE TABLE IF NOT EXISTS dim_product (
    product_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stock_code   VARCHAR(20)  NOT NULL,
    description  VARCHAR(255),
    CONSTRAINT uq_dim_product_stock_code UNIQUE (stock_code)
);

COMMENT ON TABLE dim_product IS 'Chieu san pham. stock_code = cot "StockCode" goc (toi da 12 ky tu da khao sat).';
COMMENT ON COLUMN dim_product.description IS 'Nullable: ~0.41% dong goc khong co Description.';

-- =====================================================================
-- DIM_COUNTRY — ten quoc gia tu du lieu nguon (Country), 43 gia tri
-- duy nhat da khao sat thuc te tren dataset.
-- =====================================================================
CREATE TABLE IF NOT EXISTS dim_country (
    country_key   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country_name  VARCHAR(50) NOT NULL,
    CONSTRAINT uq_dim_country_country_name UNIQUE (country_name)
);

COMMENT ON TABLE dim_country IS 'Chieu quoc gia, lay truc tiep tu cot Country cua tung dong giao dich (xem docs/03_star_schema.md muc 5).';

-- =====================================================================
-- FACT_SALES — grain: 1 dong = 1 dong hang trong 1 hoa don (KHONG phai
-- 1 hoa don tong hop). sale_key la surrogate rieng vi da xac minh thuc
-- te co 42,638 cap (Invoice, StockCode) xuat hien >1 dong trong dataset
-- goc -> (Invoice, StockCode) khong phai khoa duy nhat o muc dong.
--
-- customer_key duoc phep NULL: 22.77% dong goc khong co Customer ID va
-- quy tac xu ly chinh thuc (loai khoi fact_sales hay giu NULL) chua
-- duoc nhom xac nhan (xem docs/03_star_schema.md muc 6, quyet dinh #2).
-- =====================================================================
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    invoice_no        VARCHAR(20) NOT NULL,
    invoice_datetime  TIMESTAMP   NOT NULL,
    date_key          BIGINT      NOT NULL REFERENCES dim_date (date_key),
    customer_key      BIGINT          NULL REFERENCES dim_customer (customer_key),
    product_key       BIGINT      NOT NULL REFERENCES dim_product (product_key),
    country_key       BIGINT      NOT NULL REFERENCES dim_country (country_key),
    quantity          INTEGER       NOT NULL,
    unit_price        NUMERIC(10, 2) NOT NULL,
    revenue           NUMERIC(14, 2) GENERATED ALWAYS AS (ROUND(quantity * unit_price, 2)) STORED,
    CONSTRAINT chk_fact_sales_quantity_not_zero CHECK (quantity <> 0),
    CONSTRAINT chk_fact_sales_unit_price_nonneg CHECK (unit_price >= 0)
);

COMMENT ON TABLE fact_sales IS 'Grain: 1 dong = 1 dong hang trong 1 hoa don. Measures: quantity, unit_price, revenue (cot tinh san).';
COMMENT ON COLUMN fact_sales.quantity IS 'So luong; co the am (don huy/tra hang) - xem docs/03_star_schema.md muc 6 quyet dinh #3. Khong bao gio = 0 (da xac minh tren dataset goc).';
COMMENT ON COLUMN fact_sales.unit_price IS 'Gia tham chieu tai thoi diem giao dich; khong am (gia am la loi nhap lieu, khong co y nghia nghiep vu).';
COMMENT ON COLUMN fact_sales.revenue IS 'Cot GENERATED: luon bang ROUND(quantity * unit_price, 2), khong the luu gia tri lech cong thuc.';
COMMENT ON COLUMN fact_sales.customer_key IS 'NULLABLE: cho phep dong thieu Customer ID (22.77% dataset goc) - quyet dinh cuoi thuoc Phase 2.3.';

-- Index phuc vu join/aggregation tu fact_sales toi cac dimension va theo invoice.
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key ON fact_sales (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key ON fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON fact_sales (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_country_key ON fact_sales (country_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_invoice_no ON fact_sales (invoice_no);
