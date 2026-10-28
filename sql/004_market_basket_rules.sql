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
-- DA CHAY THAT tren postgres-dw (xem docs/08_apriori_mba.md muc 8) - 101 dong
-- UPSERT thanh cong, DDL + UPSERT da xac nhan idempotent chay lai nhieu lan.
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

-- =====================================================================
-- V_MARKET_BASKET_RULES_NAMED — view noi StockCode trong antecedent_items/
-- consequent_items sang ten san pham thuc (dim_product.description), dung
-- CHUNG cho moi chart/bang Superset hien thi luat ket hop (khong viet lai
-- logic noi ten rieng cho tung chart). KHONG sua gia tri support/confidence/
-- lift goc - view nay CHI THEM cot hien thi, khong tinh lai bat ky chi so
-- nao cua thuat toan Apriori.
--
-- antecedent_items/consequent_items la danh sach StockCode noi boi "|" (co
-- the nhieu StockCode/ve neu --max-len >= 3 - hien tai du lieu that chi co
-- 1 StockCode/ve vi max_len=2 mac dinh, nhung view xu ly dung CA HAI truong
-- hop, khong gia dinh chi co 1 ma). Dung unnest(string_to_array(...)) +
-- string_agg de GOM LAI dung 1 dong/luat sau khi JOIN dim_product - da kiem
-- thu thuc te KHONG lam nhan ban dong (101 luat -> van dung 101 dong).
--
-- Ma khong tra duoc ten (dim_product khong co dong tuong ung, hoac co dong
-- nhung description NULL) hien thi "Unknown product (StockCode: <ma>)" -
-- KHONG bia ten, chi ghi ro ma goc. Tai thoi diem tao view nay, da kiem tra
-- thuc te: ca 45 StockCode duy nhat xuat hien trong market_basket_rules deu
-- co trong dim_product va co description (0 ma "Unknown") - xem
-- docs/08_apriori_mba.md muc 13.
--
-- dim_product.stock_code co UNIQUE constraint (sql/001_create_star_schema.sql)
-- nen JOIN 1-1, khong can xu ly rieng truong hop dim_product co nhieu dong
-- cung ma.
-- =====================================================================
CREATE OR REPLACE VIEW v_market_basket_rules_named AS
WITH antecedent_names AS (
    SELECT
        r.rule_id,
        string_agg(
            COALESCE(dp.description || ' (' || code || ')', 'Unknown product (StockCode: ' || code || ')'),
            '; ' ORDER BY code
        ) AS antecedent_label
    FROM market_basket_rules r
    CROSS JOIN LATERAL unnest(string_to_array(r.antecedent_items, '|')) AS code
    LEFT JOIN dim_product dp ON dp.stock_code = code
    GROUP BY r.rule_id
),
consequent_names AS (
    SELECT
        r.rule_id,
        string_agg(
            COALESCE(dp.description || ' (' || code || ')', 'Unknown product (StockCode: ' || code || ')'),
            '; ' ORDER BY code
        ) AS consequent_label
    FROM market_basket_rules r
    CROSS JOIN LATERAL unnest(string_to_array(r.consequent_items, '|')) AS code
    LEFT JOIN dim_product dp ON dp.stock_code = code
    GROUP BY r.rule_id
)
SELECT
    r.rule_id,
    r.antecedent_items,
    r.consequent_items,
    an.antecedent_label,
    cn.consequent_label,
    r.antecedent_size,
    r.consequent_size,
    r.support,
    r.confidence,
    ROUND(r.confidence * 100, 2) AS confidence_pct,
    r.lift,
    r.antecedent_support,
    r.consequent_support,
    r.computed_at
FROM market_basket_rules r
JOIN antecedent_names an ON an.rule_id = r.rule_id
JOIN consequent_names cn ON cn.rule_id = r.rule_id;

COMMENT ON VIEW v_market_basket_rules_named IS 'Phase 8.1 - market_basket_rules kem ten san pham that (dim_product.description), dung chung cho Superset. 1 dong/luat, khong nhan ban, khong sua gia tri Apriori goc.';
COMMENT ON COLUMN v_market_basket_rules_named.antecedent_label IS 'Ten + ma tung san pham ve trai, dang "Ten (StockCode)", noi boi "; " neu nhieu san pham. "Unknown product (StockCode: ...)" neu khong tra duoc ten.';
COMMENT ON COLUMN v_market_basket_rules_named.consequent_label IS 'Nhu antecedent_label, cho ve phai luat.';
COMMENT ON COLUMN v_market_basket_rules_named.confidence_pct IS 'confidence * 100, lam tron 2 chu so - chi de hien thi dang %, KHONG thay the cot confidence goc (0..1).';
