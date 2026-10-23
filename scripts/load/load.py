"""Phase 2.4 - Load: nap du lieu da Transform (Phase 2.3, datalake/processed/)
vao PostgreSQL DW (Star Schema: dim_date, dim_customer, dim_product, dim_country,
fact_sales - xem sql/001_create_star_schema.sql).

Idempotent (Phase 2.5):
  - 4 dimension: UPSERT theo khoa tu nhien (ON CONFLICT ... DO NOTHING/DO UPDATE)
    - KHONG BAO GIO xoa dimension.
  - fact_sales: TRUNCATE (CHI bang nay - khong dong den dimension, khong dong den
    postgres-dw o muc database/volume) + INSERT lai toan bo tu processed moi
    nhat. Day la "xoa-nap lai theo batch" da duoc thiet ke va rui ro hoa tu dau
    du an (PROJECT_CHECKLIST.md Phase 2.5), va da kiem thu an toan voi
    source_customers o Phase 1.3 (TRUNCATE + INSERT, chay lai nhieu lan khong
    trung). KHONG truncate toan bo DW, KHONG dung docker compose down -v.

Thu tu nap (dung yeu cau Phase 2.4): dim_date -> dim_customer -> dim_product ->
dim_country -> fact_sales. Surrogate key cua fact duoc tra cuu lai tu dimension
sau khi upsert (khoa tu nhien -> surrogate) qua pandas merge; customer_key duoc
phep NULL cho dong thieu Customer ID (khong lam fact insert that bai).

Host ket noi postgres-dw doc tu PG_DW_HOST (bien moi truong), mac dinh
"localhost" (dung khi chay tren host); doi thanh "postgres-dw" khi chay trong
container Airflow (xem docker-compose.yml, giong pattern FASTAPI_HOST/
PG_SOURCE_HOST da dung o Phase 2.2).

Chay: uv run scripts/load/load.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from common import PROCESSED_DIR, ensure_dirs, load_env_file, log_ingest  # noqa: E402

DIM_TABLES_IN_ORDER = ("dim_date", "dim_customer", "dim_product", "dim_country")


def _connection_params() -> dict:
    # PG_DW_PORT (.env) la port PUBLISH ra host (5433, chon rieng de khong trung
    # postgres-source tren host). Khi ket noi qua ten service Docker noi bo
    # (PG_DW_HOST=postgres-dw), phai dung port NOI BO cua container Postgres
    # (luon la 5432 chuan) - doc tu PG_DW_PORT_INTERNAL, mac dinh = PG_DW_PORT de
    # van dung khi chay tren host (ca 2 deu tro ve cung 1 gia tri trong truong
    # hop nay vi postgres-source co host port = container port, nhung postgres-dw
    # thi KHAC nhau nen can bien rieng).
    return {
        "host": os.environ.get("PG_DW_HOST", "localhost"),
        "port": os.environ.get("PG_DW_PORT_INTERNAL", os.environ["PG_DW_PORT"]),
        "dbname": os.environ["PG_DW_DB"],
        "user": os.environ["PG_DW_USER"],
        "password": os.environ["PG_DW_PASSWORD"],
    }


def _find_latest_processed_dir() -> Path | None:
    if not PROCESSED_DIR.exists():
        return None
    dirs = sorted((d for d in PROCESSED_DIR.iterdir() if d.is_dir()), reverse=True)
    return dirs[0] if dirs else None


def _upsert_dim_date(cur, df: pd.DataFrame) -> None:
    rows = list(
        df[
            ["full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name", "week_of_year"]
        ].itertuples(index=False, name=None)
    )
    execute_values(
        cur,
        """
        INSERT INTO dim_date (full_date, year, quarter, month, month_name, day, day_of_week, day_name, week_of_year)
        VALUES %s
        ON CONFLICT (full_date) DO NOTHING
        """,
        rows,
    )


def _upsert_dim_customer(cur, df: pd.DataFrame) -> None:
    rows = list(df[["customer_id"]].itertuples(index=False, name=None))
    execute_values(
        cur,
        "INSERT INTO dim_customer (customer_id) VALUES %s ON CONFLICT (customer_id) DO NOTHING",
        rows,
    )


def _upsert_dim_product(cur, df: pd.DataFrame) -> None:
    work = df.where(pd.notna(df), None)
    rows = list(work[["stock_code", "description"]].itertuples(index=False, name=None))
    execute_values(
        cur,
        """
        INSERT INTO dim_product (stock_code, description) VALUES %s
        ON CONFLICT (stock_code) DO UPDATE SET description = EXCLUDED.description
        """,
        rows,
    )


def _upsert_dim_country(cur, df: pd.DataFrame) -> None:
    rows = list(df[["country_name"]].itertuples(index=False, name=None))
    execute_values(
        cur,
        "INSERT INTO dim_country (country_name) VALUES %s ON CONFLICT (country_name) DO NOTHING",
        rows,
    )


def run() -> int:
    load_env_file()
    ensure_dirs()

    processed_dir = _find_latest_processed_dir()
    conn = None
    try:
        if processed_dir is None:
            raise FileNotFoundError(
                f"Khong tim thay thu muc processed nao trong {PROCESSED_DIR} - "
                "chay scripts/transform/transform.py truoc."
            )
        print(f"[load] Doc du lieu da Transform tu: {processed_dir}")

        dim_date = pd.read_csv(processed_dir / "dim_date.csv")
        dim_customer = pd.read_csv(processed_dir / "dim_customer.csv")
        dim_product = pd.read_csv(processed_dir / "dim_product.csv")
        dim_country = pd.read_csv(processed_dir / "dim_country.csv")
        fact = pd.read_csv(processed_dir / "fact_sales.csv")
        fact["invoice_datetime"] = pd.to_datetime(fact["invoice_datetime"])
        fact["invoice_date"] = pd.to_datetime(fact["invoice_date"]).dt.date
        fact["customer_id"] = fact["customer_id"].astype("Int64")

        print(f"[load] Nguon: dim_date={len(dim_date):,}, dim_customer={len(dim_customer):,}, "
              f"dim_product={len(dim_product):,}, dim_country={len(dim_country):,}, "
              f"fact_sales={len(fact):,}")

        conn = psycopg2.connect(**_connection_params())

        with conn.cursor() as cur:
            _upsert_dim_date(cur, dim_date)
            _upsert_dim_customer(cur, dim_customer)
            _upsert_dim_product(cur, dim_product)
            _upsert_dim_country(cur, dim_country)
        conn.commit()
        print("[load] Da UPSERT xong 4 dimension (khong xoa dong cu nao).")

        maps = {}
        with conn.cursor() as cur:
            for table, key_col, nat_col in (
                ("dim_date", "date_key", "full_date"),
                ("dim_customer", "customer_key", "customer_id"),
                ("dim_product", "product_key", "stock_code"),
                ("dim_country", "country_key", "country_name"),
            ):
                cur.execute(f"SELECT {nat_col}, {key_col} FROM {table};")
                maps[table] = dict(cur.fetchall())

        fact["date_key"] = fact["invoice_date"].map(maps["dim_date"])
        fact["customer_key"] = fact["customer_id"].map(maps["dim_customer"])
        fact["product_key"] = fact["stock_code"].map(maps["dim_product"])
        fact["country_key"] = fact["country_name"].map(maps["dim_country"])

        n_orphan_date = int(fact["date_key"].isna().sum())
        n_orphan_product = int(fact["product_key"].isna().sum())
        n_orphan_country = int(fact["country_key"].isna().sum())
        if n_orphan_date or n_orphan_product or n_orphan_country:
            raise AssertionError(
                f"Phat hien khoa ngoai mo coi khi tra cuu surrogate key: "
                f"date={n_orphan_date}, product={n_orphan_product}, country={n_orphan_country}"
            )
        # customer_key duoc phep NaN (dong thieu Customer ID) - KHONG assert o day.
        n_customer_key_null = int(fact["customer_key"].isna().sum())
        print(f"[load] Tra cuu surrogate key xong. {n_customer_key_null:,} dong co "
              f"customer_key = NULL (dung quy dinh - giu giao dich, loai khoi RFM).")

        records = [
            (
                row.invoice_no,
                row.invoice_datetime.to_pydatetime(),
                int(row.date_key),
                None if pd.isna(row.customer_key) else int(row.customer_key),
                int(row.product_key),
                int(row.country_key),
                int(row.quantity),
                float(row.unit_price),
            )
            for row in fact.itertuples(index=False)
        ]

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE fact_sales;")
            execute_values(
                cur,
                """
                INSERT INTO fact_sales
                    (invoice_no, invoice_datetime, date_key, customer_key, product_key, country_key, quantity, unit_price)
                VALUES %s
                """,
                records,
                page_size=5000,
            )
        conn.commit()
        print(f"[load] Da TRUNCATE + INSERT fact_sales: {len(records):,} dong.")

        with conn.cursor() as cur:
            cur.execute("SELECT count(*), coalesce(sum(revenue), 0) FROM fact_sales;")
            n_in_db, revenue_in_db = cur.fetchone()
        print(f"[load] Xac nhan trong DW: fact_sales={n_in_db:,} dong, "
              f"tong revenue={float(revenue_in_db):,.2f}")

        if n_in_db != len(records):
            raise AssertionError(f"So dong trong DB ({n_in_db}) khong khop so dong da nap ({len(records)})")

        log_ingest("load", "success", n_in_db, str(processed_dir))
        return 0
    except Exception as exc:  # noqa: BLE001
        log_ingest("load", "failed", None, str(processed_dir) if processed_dir else "", error_message=str(exc))
        print(f"[LOI] Load thua bai: {exc}")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(run())
