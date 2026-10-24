"""Phase 5.5 - Nap ket qua phan cum (scripts/mining/cluster_dw.py) vao bang
customer_segment trong PostgreSQL DW (sql/003_customer_segment.sql).

Idempotent: UPSERT theo customer_key (ON CONFLICT ... DO UPDATE) - chay lai
nhieu lan KHONG tang so dong, chi cap nhat lai cac cot neu mo hinh doi nhan cum.
KHONG TRUNCATE, KHONG xoa toan bo roi nap lai (khac fact_sales - bang nay nho,
UPSERT tung dong la du va an toan hon).

Nguon: data/processed/customer_segments_from_dw.csv (cot Cluster_business,
segment_name - xem scripts/mining/cluster_dw.py). Dung Cluster_business (khong
dung Cluster_best) vi day la K duoc chon de dien giai nghiep vu, giong quyet
dinh da ap dung cho baseline (docs/01_bao_cao_tien_do.md).

Chay: uv run scripts/mining/load_segments_dw.py
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
INPUT_PATH = PROCESSED_DIR / "customer_segments_from_dw.csv"


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

    conn = None
    try:
        if not INPUT_PATH.exists():
            raise FileNotFoundError(f"Chua co {INPUT_PATH}. Chay scripts/mining/cluster_dw.py truoc.")

        df = pd.read_csv(INPUT_PATH)
        print(f"[load_segments_dw] Doc {len(df):,} dong tu {INPUT_PATH.name}")

        if df["Customer ID"].duplicated().any():
            raise AssertionError("Phat hien Customer ID trung lap trong file dau vao - dung nap.")

        k_used = int(df["Cluster_business"].nunique())

        conn = psycopg2.connect(**_connection_params())
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id, customer_key FROM dim_customer;")
            customer_key_map = dict(cur.fetchall())

        df["customer_key"] = df["Customer ID"].map(customer_key_map)
        n_orphan = int(df["customer_key"].isna().sum())
        if n_orphan:
            raise AssertionError(
                f"{n_orphan} Customer ID trong file phan cum khong tim thay customer_key "
                "tuong ung trong dim_customer - kiem tra lai nguon du lieu."
            )

        records = [
            (
                int(row.customer_key),
                int(row.Recency),
                int(row.Frequency),
                float(row.Monetary),
                k_used,
                int(row.Cluster_business),
                str(row.segment_name),
            )
            for row in df.itertuples(index=False)
        ]

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO customer_segment
                    (customer_key, recency, frequency, monetary, k_used, cluster_label, segment_name, computed_at)
                VALUES %s
                ON CONFLICT (customer_key) DO UPDATE SET
                    recency = EXCLUDED.recency,
                    frequency = EXCLUDED.frequency,
                    monetary = EXCLUDED.monetary,
                    k_used = EXCLUDED.k_used,
                    cluster_label = EXCLUDED.cluster_label,
                    segment_name = EXCLUDED.segment_name,
                    computed_at = EXCLUDED.computed_at
                """,
                [(*r, pd.Timestamp.now("UTC").to_pydatetime()) for r in records],
                page_size=2000,
            )
        conn.commit()
        print(f"[load_segments_dw] Da UPSERT {len(records):,} dong vao customer_segment (k_used={k_used}).")

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM customer_segment;")
            n_in_db = cur.fetchone()[0]
        print(f"[load_segments_dw] Xac nhan trong DW: customer_segment={n_in_db:,} dong.")

        log_ingest("load_segments_dw", "success", len(records), str(INPUT_PATH))
        return 0
    except Exception as exc:  # noqa: BLE001
        log_ingest("load_segments_dw", "failed", None, str(INPUT_PATH), error_message=str(exc))
        print(f"[LOI] load_segments_dw thua bai: {exc}")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(run())
