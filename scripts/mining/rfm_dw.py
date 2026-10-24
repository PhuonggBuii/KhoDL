"""Phase 5.1 (nguon DW) - Tinh RFM (Recency, Frequency, Monetary) cho tung khach
hang truc tiep tu PostgreSQL Data Warehouse (fact_sales JOIN dim_customer), thay
vi tu data/processed/transactions_clean.csv (baseline scripts/03_rfm.py).

KHONG sua scripts/03_rfm.py, KHONG ghi de data/processed/rfm.csv - day la pipeline
MOI, doc lap, dung de doi chieu voi baseline (xem docs/05_rfm_kmeans_dw.md).

Cach tinh (giu nguyen cong thuc cua baseline, chi doi nguon du lieu):
  - Chi lay giao dich co customer_key KHONG NULL (dong thieu Customer ID da duoc
    Load giu lai trong fact_sales voi customer_key = NULL theo quyet dinh
    docs/03_star_schema.md muc 6 - PHAI loai khoi RFM, giong cach
    scripts/02_clean_data.py da dropna Customer ID truoc khi scripts/03_rfm.py
    tinh RFM tren tap da loc).
  - Recency   = so ngay tu lan mua gan nhat (invoice_datetime) cua khach hang den
    ngay tham chieu. Ngay tham chieu = invoice_datetime lon nhat TRONG TAP DA LOC
    (customer_key IS NOT NULL) + 1 ngay - dung cach tinh nhu scripts/03_rfm.py
    (df da duoc dropna Customer ID truoc khi lay .max()).
  - Frequency = so invoice_no duy nhat cua khach hang.
  - Monetary  = tong revenue (cot GENERATED = ROUND(quantity*unit_price,2)) cua
    khach hang.

Dau ra: data/processed/rfm_from_dw.csv (cot: Customer ID, Recency, Frequency,
Monetary - giong ten cot baseline de scripts/mining/cluster_dw.py tai su dung
logic cua scripts/04_kmeans.py ma khong can sua).

Ket noi postgres-dw: tai su dung dung pattern bien moi truong cua
scripts/load/load.py (PG_DW_HOST mac dinh "localhost", PG_DW_PORT_INTERNAL mac
dinh = PG_DW_PORT khi chay tren host, doi thanh postgres-dw/5432 khi chay trong
container Airflow qua docker-compose.yml).

Chay: uv run scripts/mining/rfm_dw.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from common import PROCESSED_DIR as DATALAKE_PROCESSED_DIR  # noqa: E402
from common import ensure_dirs, load_env_file, log_ingest  # noqa: E402

LOCAL_PROCESSED_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "processed"
OUTPUT_PATH = LOCAL_PROCESSED_DIR / "rfm_from_dw.csv"
BASELINE_PATH = LOCAL_PROCESSED_DIR / "rfm.csv"

QUERY = """
    SELECT c.customer_id AS customer_id,
           f.invoice_no AS invoice_no,
           f.invoice_datetime AS invoice_datetime,
           f.revenue AS revenue
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    WHERE f.customer_key IS NOT NULL
"""


def _connection_params() -> dict:
    # Giong scripts/load/load.py: PG_DW_PORT (.env) la port PUBLISH ra host
    # (5433); qua ten service noi bo (PG_DW_HOST=postgres-dw) phai dung port NOI
    # BO cua container Postgres (PG_DW_PORT_INTERNAL, mac dinh = PG_DW_PORT).
    return {
        "host": os.environ.get("PG_DW_HOST", "localhost"),
        "port": os.environ.get("PG_DW_PORT_INTERNAL", os.environ["PG_DW_PORT"]),
        "dbname": os.environ["PG_DW_DB"],
        "user": os.environ["PG_DW_USER"],
        "password": os.environ["PG_DW_PASSWORD"],
    }


def _fetch_fact_customer(conn) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(QUERY)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    df["invoice_datetime"] = pd.to_datetime(df["invoice_datetime"])
    df["revenue"] = df["revenue"].astype(float)
    return df


def _compute_rfm(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp]:
    reference_date = df["invoice_datetime"].max() + pd.Timedelta(days=1)

    rfm = (
        df.groupby("customer_id")
        .agg(
            Recency=("invoice_datetime", lambda s: (reference_date - s.max()).days),
            Frequency=("invoice_no", "nunique"),
            Monetary=("revenue", "sum"),
        )
        .reset_index()
        .rename(columns={"customer_id": "Customer ID"})
    )
    return rfm, reference_date


def _compare_with_baseline(rfm: pd.DataFrame) -> None:
    if not BASELINE_PATH.exists():
        print(f"[rfm_dw] [canh bao] Khong thay baseline {BASELINE_PATH} de doi chieu.")
        return

    baseline = pd.read_csv(BASELINE_PATH)
    merged = rfm.merge(baseline, on="Customer ID", how="outer", suffixes=("_dw", "_baseline"))

    only_dw = merged[merged["Recency_baseline"].isna()]
    only_baseline = merged[merged["Recency_dw"].isna()]
    both = merged.dropna(subset=["Recency_dw", "Recency_baseline"])

    print("\n=== Doi chieu RFM-tu-DW voi baseline (data/processed/rfm.csv) ===")
    print(f"So khach hang - DW: {len(rfm):,} | Baseline: {len(baseline):,}")
    print(f"Chi co trong DW (khong co trong baseline): {len(only_dw):,}")
    print(f"Chi co trong baseline (khong co trong DW): {len(only_baseline):,}")

    if not both.empty:
        diff_recency = (both["Recency_dw"] - both["Recency_baseline"]).abs()
        diff_frequency = (both["Frequency_dw"] - both["Frequency_baseline"]).abs()
        diff_monetary = (both["Monetary_dw"] - both["Monetary_baseline"]).abs()
        print(f"Khach hang chung: {len(both):,}")
        print(f"  Lech Recency   - max: {diff_recency.max():.4f}, so dong lech>0: {(diff_recency > 0).sum():,}")
        print(f"  Lech Frequency - max: {diff_frequency.max():.4f}, so dong lech>0: {(diff_frequency > 0).sum():,}")
        print(f"  Lech Monetary  - max: {diff_monetary.max():.4f}, so dong lech>0.01: {(diff_monetary > 0.01).sum():,}")


def run() -> int:
    load_env_file()
    ensure_dirs()
    LOCAL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    conn = None
    try:
        conn = psycopg2.connect(**_connection_params())
        df = _fetch_fact_customer(conn)
        print(f"[rfm_dw] Doc {len(df):,} dong fact_sales (customer_key IS NOT NULL) tu DW, "
              f"{df['customer_id'].nunique():,} khach hang.")

        if df.empty:
            raise RuntimeError("fact_sales khong co dong nao voi customer_key IS NOT NULL.")

        rfm, reference_date = _compute_rfm(df)
        print(f"[rfm_dw] Ngay tham chieu (reference date) cho Recency: {reference_date.date()}")

        if (rfm["Recency"] < 0).any() or (rfm["Frequency"] <= 0).any() or (rfm["Monetary"] < 0).any():
            raise AssertionError("Phat hien gia tri RFM khong hop le (am hoac Frequency=0).")

        print("\n=== Thong ke RFM (tu DW) ===")
        print(rfm[["Recency", "Frequency", "Monetary"]].describe())

        _compare_with_baseline(rfm)

        rfm.to_csv(OUTPUT_PATH, index=False)
        print(f"\n[rfm_dw] Da luu {len(rfm):,} dong RFM vao: {OUTPUT_PATH}")

        log_ingest("rfm_dw", "success", len(rfm), str(OUTPUT_PATH))
        return 0
    except Exception as exc:  # noqa: BLE001
        log_ingest("rfm_dw", "failed", None, str(OUTPUT_PATH), error_message=str(exc))
        print(f"[LOI] rfm_dw thua bai: {exc}")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(run())
