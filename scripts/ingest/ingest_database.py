"""Ingest nguon 'Database' — Phase 1.3 (nap bang source_customers vao
postgres-source) + Phase 1.4 (SELECT lai tu Postgres, luu snapshot vao Data
Lake raw/database/) + Phase 1.5 (ghi ingest log).

Idempotent: TRUNCATE + INSERT lai toan bo bang moi lan chay -> chay lai nhieu
lan khong tao ban ghi trung (giong chien luoc "xoa-nap lai theo batch" da
chon san cho Phase 2.5).

Yeu cau: service postgres-source dang chay va healthy (docker compose up -d
postgres-source), bien moi truong doc tu .env o goc repo. Host ket noi Postgres
doc tu PG_SOURCE_HOST (.env hoac moi truong), mac dinh "localhost" de chay dung
tren may host nhu truoc (Phase 1). Khi chay trong container Airflow (Phase 2.2),
docker-compose.yml dat PG_SOURCE_HOST=postgres-source (ten service Docker).

Chay: uv run scripts/ingest/ingest_database.py
"""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import RAW_DIR, ensure_dirs, load_env_file, log_ingest, today_str  # noqa: E402

RAW_SOURCE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "online_retail_II.xlsx"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS source_customers (
    customer_id INTEGER PRIMARY KEY,
    country VARCHAR(100) NOT NULL
);
"""


def _connection_params() -> dict:
    return {
        "host": os.environ.get("PG_SOURCE_HOST", "localhost"),
        "port": os.environ["PG_SOURCE_PORT"],
        "dbname": os.environ["PG_SOURCE_DB"],
        "user": os.environ["PG_SOURCE_USER"],
        "password": os.environ["PG_SOURCE_PASSWORD"],
    }


def _extract_customers() -> pd.DataFrame:
    sheets = pd.read_excel(RAW_SOURCE_PATH, sheet_name=None)
    df = pd.concat(sheets.values(), ignore_index=True)
    df = df.dropna(subset=["Customer ID", "Country"])
    df["Customer ID"] = df["Customer ID"].astype(int)

    # Mot Customer ID ly thuyet co the xuat hien voi >1 Country khac nhau (vd
    # loi nhap lieu) -> chon Country xuat hien nhieu nhat cho moi khach hang,
    # vi day la bang khach hang (khoa chinh customer_id, 1 dong/khach).
    customers = (
        df.groupby("Customer ID")["Country"]
        .agg(lambda s: s.mode().iloc[0])
        .reset_index()
        .rename(columns={"Customer ID": "customer_id", "Country": "country"})
    )
    return customers


def run() -> int:
    load_env_file()
    ensure_dirs()
    out_dir = RAW_DIR / "database" / today_str()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "customers_from_db.csv"

    conn = None
    try:
        if not RAW_SOURCE_PATH.exists():
            raise FileNotFoundError(f"Khong tim thay dataset tai {RAW_SOURCE_PATH}")

        customers = _extract_customers()
        n_customers = len(customers)
        print(f"Trich duoc {n_customers:,} khach hang duy nhat (Customer ID + Country) tu dataset.")

        conn = psycopg2.connect(**_connection_params())
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute("TRUNCATE TABLE source_customers;")
            execute_values(
                cur,
                "INSERT INTO source_customers (customer_id, country) VALUES %s",
                list(customers.itertuples(index=False, name=None)),
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM source_customers;")
            n_in_db = cur.fetchone()[0]
            cur.execute("SELECT customer_id, country FROM source_customers ORDER BY customer_id LIMIT 3;")
            sample = cur.fetchall()
        print(f"Da nap vao postgres-source.source_customers: {n_in_db:,} dong. Mau: {sample}")

        if n_in_db != n_customers:
            raise AssertionError(
                f"So dong trong DB ({n_in_db}) khong khop so khach hang trich duoc ({n_customers})"
            )

        # Mo phong buoc "Airflow task chay SQL SELECT" (Phase 1.4) — doc lai tu
        # Postgres (khong doc lai Excel) de luu snapshot vao Data Lake.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)  # pandas warn ve psycopg2 connection, khong anh huong ket qua
            snapshot = pd.read_sql(
                "SELECT customer_id, country FROM source_customers ORDER BY customer_id", conn
            )
        snapshot.to_csv(out_path, index=False)
        print(f"Da luu snapshot SELECT tu DB: {out_path} ({len(snapshot):,} dong).")

        log_ingest("database", "success", len(snapshot), str(out_path))
        return 0
    except Exception as exc:  # noqa: BLE001
        log_ingest("database", "failed", None, str(out_path), error_message=str(exc))
        print(f"[LOI] Ingest nguon Database thua bai: {exc}")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(run())
