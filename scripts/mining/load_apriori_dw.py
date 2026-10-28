"""Phase 8.1 - Nap ket qua luat ket hop (scripts/mining/apriori_mba.py) vao bang
market_basket_rules trong PostgreSQL DW (sql/004_market_basket_rules.sql).

Idempotent: UPSERT theo khoa (antecedent_items, consequent_items) (ON CONFLICT
... DO UPDATE) - chay lai nhieu lan voi CUNG tham so KHONG tang so dong, chi
cap nhat lai cac chi so neu chay lai Apriori tren du lieu moi hon. KHONG
TRUNCATE (khac fact_sales) - bang nay nho, UPSERT tung dong la du.

CHUA DUOC CHAY THAT tren postgres-dw (Docker Desktop khong chay trong phien
lam viec nay) - xem docs/08_apriori_mba.md muc "Gioi han/Chua kiem thu". Script
nay da duoc kiem tra logic/cu phap can than theo dung pattern da kiem thu that
cua scripts/mining/load_segments_dw.py.

Nguon: data/processed/apriori_association_rules.csv (scripts/mining/apriori_mba.py).

Chay: uv run scripts/mining/load_apriori_dw.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from common import ensure_dirs, load_env_file, log_ingest  # noqa: E402

PROCESSED_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "processed"
INPUT_PATH = PROCESSED_DIR / "apriori_association_rules.csv"

# Phai khop DEFAULT_MIN_SUPPORT/DEFAULT_MIN_CONFIDENCE trong apriori_mba.py -
# file CSV dau vao KHONG tu luu lai 2 tham so nay, nen o day ghi nhan gia tri
# mac dinh de dien vao cot min_support_used/min_confidence_used. Neu chay
# apriori_mba.py voi --min-support/--min-confidence khac mac dinh, truyen lai
# gia tri tuong ung qua bien moi truong APRIORI_MIN_SUPPORT/APRIORI_MIN_CONFIDENCE
# truoc khi chay script nay (xem docs/08_apriori_mba.md).
DEFAULT_MIN_SUPPORT = 0.02
DEFAULT_MIN_CONFIDENCE = 0.30


def _connection_params() -> dict:
    return {
        "host": os.environ.get("PG_DW_HOST", "localhost"),
        "port": os.environ.get("PG_DW_PORT_INTERNAL", os.environ["PG_DW_PORT"]),
        "dbname": os.environ["PG_DW_DB"],
        "user": os.environ["PG_DW_USER"],
        "password": os.environ["PG_DW_PASSWORD"],
    }


def run() -> int:
    load_env_file()
    ensure_dirs()

    min_support_used = float(os.environ.get("APRIORI_MIN_SUPPORT", DEFAULT_MIN_SUPPORT))
    min_confidence_used = float(os.environ.get("APRIORI_MIN_CONFIDENCE", DEFAULT_MIN_CONFIDENCE))

    conn = None
    try:
        if not INPUT_PATH.exists():
            raise FileNotFoundError(f"Chua co {INPUT_PATH}. Chay scripts/mining/apriori_mba.py truoc.")

        df = pd.read_csv(INPUT_PATH)
        print(f"[load_apriori_dw] Doc {len(df):,} dong tu {INPUT_PATH.name}")

        if df.empty:
            print("[load_apriori_dw] File rong (khong co luat nao dat nguong) - khong co gi de nap. "
                  "Day KHONG phai loi (xem docs/08_apriori_mba.md ve truong hop khong co frequent itemset).")
            log_ingest("load_apriori_dw", "success", 0, str(INPUT_PATH))
            return 0

        required_cols = {"antecedent", "consequent", "support", "confidence", "lift",
                          "antecedent_support", "consequent_support"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Thieu cot bat buoc trong {INPUT_PATH.name}: {sorted(missing)}")

        if df.duplicated(subset=["antecedent", "consequent"]).any():
            raise AssertionError("Phat hien cap (antecedent, consequent) trung lap trong file dau vao - dung nap.")

        records = [
            (
                str(row.antecedent),
                str(row.consequent),
                int(str(row.antecedent).count("|") + 1),
                int(str(row.consequent).count("|") + 1),
                float(row.support),
                float(row.confidence),
                float(row.lift),
                float(row.antecedent_support),
                float(row.consequent_support),
                min_support_used,
                min_confidence_used,
            )
            for row in df.itertuples(index=False)
        ]

        conn = psycopg2.connect(**_connection_params())
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO market_basket_rules
                    (antecedent_items, consequent_items, antecedent_size, consequent_size,
                     support, confidence, lift, antecedent_support, consequent_support,
                     min_support_used, min_confidence_used, computed_at)
                VALUES %s
                ON CONFLICT (antecedent_items, consequent_items) DO UPDATE SET
                    antecedent_size = EXCLUDED.antecedent_size,
                    consequent_size = EXCLUDED.consequent_size,
                    support = EXCLUDED.support,
                    confidence = EXCLUDED.confidence,
                    lift = EXCLUDED.lift,
                    antecedent_support = EXCLUDED.antecedent_support,
                    consequent_support = EXCLUDED.consequent_support,
                    min_support_used = EXCLUDED.min_support_used,
                    min_confidence_used = EXCLUDED.min_confidence_used,
                    computed_at = EXCLUDED.computed_at
                """,
                [(*r, pd.Timestamp.now("UTC").to_pydatetime()) for r in records],
                page_size=2000,
            )
        conn.commit()
        print(f"[load_apriori_dw] Da UPSERT {len(records):,} luat vao market_basket_rules "
              f"(min_support_used={min_support_used}, min_confidence_used={min_confidence_used}).")

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM market_basket_rules;")
            n_in_db = cur.fetchone()[0]
        print(f"[load_apriori_dw] Xac nhan trong DW: market_basket_rules={n_in_db:,} dong.")

        log_ingest("load_apriori_dw", "success", len(records), str(INPUT_PATH))
        return 0
    except Exception as exc:  # noqa: BLE001
        log_ingest("load_apriori_dw", "failed", None, str(INPUT_PATH), error_message=str(exc))
        print(f"[LOI] load_apriori_dw that bai: {exc}")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(run())
