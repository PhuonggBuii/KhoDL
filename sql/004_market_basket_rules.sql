-- Phase 8.1 — Bang luat ket hop Apriori (Market Basket Analysis) cho PostgreSQL
-- Data Warehouse (postgres-dw / retail_dw). Thiet ke day du: xem
-- docs/08_apriori_mba.md.
--
-- Quyet dinh: bang RIENG "market_basket_rules" (KHONG dung ALTER len bat ky
-- bang nao cua Star Schema/customer_segment hien co) — cung nguyen tac da ap
-- dung cho customer_segment (sql/003_customer_segment.sql): khong dong cham
-- cac bang dang hoat dong, chi THEM moi.
--
-- An toan chay lai: toan bo dung IF NOT EXISTS, KHONG DROP bang/du lieu nao.
-- Nap du lieu thuc te do scripts/mining/load_apriori_dw.py thuc hien (UPSERT
-- theo (antecedent_items, consequent_items)), KHONG lam o file SQL nay.
--
-- CHUA DUOC CHAY THAT tren postgres-dw tai thoi diem viet file nay (Docker
-- Desktop khong chay trong phien lam viec nay — xem docs/08_apriori_mba.md
-- muc "Gioi han/Chua kiem thu"). Cu phap da duoc doi chieu can than theo dung
-- pattern cua sql/003_customer_segment.sql (da kiem thu that).
--
-- Chay: docker exec -i khodl-postgres-dw psql -U <PG_DW_USER> -d <PG_DW_DB> -f - < sql/004_market_basket_rules.sql

-- =====================================================================
-- MARKET_BASKET_RULES — grain: 1 dong = 1 luat ket hop A -> B (antecedent ->
-- consequent) tu 1 lan chay Apriori (scripts/mining/apriori_mba.py). Khoa tu
-- nhien: cap (antecedent_items, consequent_items) — moi StockCode trong tap
-- duoc sap xep + noi bang '|' de dam bao duy nhat bat ke thu tu dau vao.
-- KHONG dung FK toi dim_product vi antecedent/consequent la MOT TAP san pham
-- (co the nhieu StockCode khi --max-len >= 3), khong phai 1 khoa don.
-- =====================================================================
CREATE TABLE IF NOT EXISTS market_basket_rules (
    rule_id              BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    antecedent_items     TEXT          NOT NULL,
    consequent_items     TEXT          NOT NULL,
    antecedent_size      SMALLINT      NOT NULL,
    consequent_size      SMALLINT      NOT NULL,
    support              NUMERIC(8, 6) NOT NULL,
    confidence           NUMERIC(8, 6) NOT NULL,
    lift                 NUMERIC(10, 4) NOT NULL,
    antecedent_support   NUMERIC(8, 6) NOT NULL,
    consequent_support   NUMERIC(8, 6) NOT NULL,
    min_support_used     NUMERIC(8, 6) NOT NULL,
    min_confidence_used  NUMERIC(8, 6) NOT NULL,
    computed_at          TIMESTAMP     NOT NULL DEFAULT now(),
    CONSTRAINT uq_market_basket_rules_pair UNIQUE (antecedent_items, consequent_items),
    CONSTRAINT chk_mbr_support_range CHECK (support > 0 AND support <= 1),
    CONSTRAINT chk_mbr_confidence_range CHECK (confidence > 0 AND confidence <= 1),
    CONSTRAINT chk_mbr_lift_nonneg CHECK (lift >= 0),
    CONSTRAINT chk_mbr_sizes_pos CHECK (antecedent_size > 0 AND consequent_size > 0)
);

COMMENT ON TABLE market_basket_rules IS 'Phase 8.1 - Luat ket hop Apriori (nguon: fact_sales JOIN dim_product, scripts/mining/apriori_mba.py). 1 dong/luat A->B tu 1 lan chay.';
COMMENT ON COLUMN market_basket_rules.antecedent_items IS 'Danh sach StockCode cua ve trai luat (A), da sap xep, noi boi "|". Vi du: "20724|22356".';
COMMENT ON COLUMN market_basket_rules.consequent_items IS 'Danh sach StockCode cua ve phai luat (B), da sap xep, noi boi "|".';
COMMENT ON COLUMN market_basket_rules.lift IS 'confidence(A->B) / support(B). lift>1 = lien he duong (thong ke), lift=1 = doc lap, lift<1 = lien he am. KHONG suy dien nhan qua.';
COMMENT ON COLUMN market_basket_rules.min_support_used IS 'Nguong min_support dung khi chay Apriori sinh ra dong nay (--min-support) - de biet luat nay thuoc lan chay nao.';
COMMENT ON COLUMN market_basket_rules.computed_at IS 'Thoi diem UPSERT ban ghi nay (cap nhat moi lan chay lai scripts/mining/load_apriori_dw.py).';

CREATE INDEX IF NOT EXISTS idx_market_basket_rules_lift ON market_basket_rules (lift DESC);
CREATE INDEX IF NOT EXISTS idx_market_basket_rules_antecedent ON market_basket_rules (antecedent_items);
