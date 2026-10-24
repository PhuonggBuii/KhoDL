"""Phase 5 (chinh thuc) - RFM + K-Means doc truc tiep tu Data Warehouse (postgres-dw).

Khac voi scripts/03_rfm.py va 04_kmeans.py (doc file CSV rieng, giu nguyen lam baseline doi chieu),
script nay doc fact_sales/dim_customer trong PostgreSQL, tinh RFM, chay K-Means va GHI KET QUA
NGUOC VAO WAREHOUSE (bang customer_segments, silhouette_scores) de Superset truy van.

Phuong phap giu nguyen nhu 03_rfm.py / 04_kmeans.py:
  - Chi tinh tren khach co Customer ID (customer_key IS NOT NULL) - quyet dinh #1 cua nhom.
  - Ngay tham chieu = ngay giao dich gan nhat (cua cac giao dich co khach) + 1 ngay.
  - Recency = so ngay tu lan mua gan nhat; Frequency = so hoa don duy nhat; Monetary = tong doanh thu.
  - log1p cho Frequency, Monetary roi StandardScaler; thu K = 2..10 bang Silhouette Score.
  - Gan cum cho K tot nhat theo Silhouette (cluster_best) va K = 4 (cluster_k4, K nghiep vu da chot).

Yeu cau: da chay DAG (fact_sales co du lieu) va da tao bang: sql/002_create_mining_tables.sql.
Ket noi: doc .env o goc repo. Tren may host: PG_DW_HOST mac dinh "localhost", port PG_DW_PORT (5433).

Chay (PowerShell, thu muc goc repo):  python scripts/mining/rfm_kmeans_dw.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from common import load_env_file  # noqa: E402

K_RANGE = range(2, 11)
BUSINESS_K = 4
RANDOM_STATE = 42

SEGMENT_VIP = "VIP"
SEGMENT_LOYAL = "Trung thành"
SEGMENT_NEW = "Mới / ít tương tác"
SEGMENT_RISK = "Có nguy cơ rời bỏ"

RFM_SQL = """
    SELECT f.customer_key,
           c.customer_id,
           MAX(f.invoice_datetime)        AS last_purchase,
           COUNT(DISTINCT f.invoice_no)   AS frequency,
           SUM(f.revenue)                 AS monetary
    FROM fact_sales f
    JOIN dim_customer c ON c.customer_key = f.customer_key
    GROUP BY f.customer_key, c.customer_id
    ORDER BY f.customer_key
"""


def connect():
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("PG_DW_HOST", "localhost"),
        port=os.environ.get("PG_DW_PORT_INTERNAL", os.environ["PG_DW_PORT"]),
        dbname=os.environ["PG_DW_DB"],
        user=os.environ["PG_DW_USER"],
        password=os.environ["PG_DW_PASSWORD"],
    )


def fetch_customer_aggregates(conn) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(RFM_SQL)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df["last_purchase"] = pd.to_datetime(df["last_purchase"])
    df["frequency"] = df["frequency"].astype(int)
    df["monetary"] = df["monetary"].astype(float)
    return df


def compute_rfm(agg: pd.DataFrame):
    """Recency tinh tu ngay tham chieu = (lan mua cuoi cung cua toan bo khach) + 1 ngay."""
    reference = agg["last_purchase"].max() + pd.Timedelta(days=1)
    rfm = pd.DataFrame(
        {
            "customer_key": agg["customer_key"].astype(int),
            "customer_id": agg["customer_id"].astype(int),
            "recency": (reference - agg["last_purchase"]).dt.days.astype(int),
            "frequency": agg["frequency"],
            "monetary": agg["monetary"],
        }
    )
    return rfm, reference


def run_kmeans(rfm: pd.DataFrame):
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler

    features = np.column_stack(
        [rfm["recency"], np.log1p(rfm["frequency"]), np.log1p(rfm["monetary"])]
    )
    X = StandardScaler().fit_transform(features)

    scores = {}
    for k in K_RANGE:
        labels = KMeans(n_clusters=k, n_init=10, random_state=RANDOM_STATE).fit_predict(X)
        scores[k] = float(silhouette_score(X, labels))
        print(f"K={k}: silhouette={scores[k]:.4f}")

    best_k = max(scores, key=scores.get)
    out = rfm.copy()
    out["cluster_best"] = KMeans(n_clusters=best_k, n_init=10, random_state=RANDOM_STATE).fit_predict(X)
    out["cluster_k4"] = KMeans(n_clusters=BUSINESS_K, n_init=10, random_state=RANDOM_STATE).fit_predict(X)
    out["segment_name"] = name_segments(out)
    return out, scores, best_k


def name_segments(rfm: pd.DataFrame) -> pd.Series:
    """Dat ten cum K=4 theo dac diem trung binh (nhan do nhom dien giai, khong co san trong du lieu)."""
    prof = rfm.groupby("cluster_k4").agg(
        R=("recency", "mean"), F=("frequency", "mean"), M=("monetary", "mean")
    )
    names = {}
    vip = prof["M"].idxmax()
    names[vip] = SEGMENT_VIP
    rest = prof.drop(vip)
    risk = rest["R"].idxmax()
    names[risk] = SEGMENT_RISK
    rest = rest.drop(risk)
    loyal = rest["F"].idxmax()
    names[loyal] = SEGMENT_LOYAL
    names[rest.drop(loyal).index[0]] = SEGMENT_NEW
    print("\n=== Dac diem tung cum (K=4) ===")
    print(prof.round(1).assign(ten=pd.Series(names)))
    return rfm["cluster_k4"].map(names)


def save_results(conn, result: pd.DataFrame, scores: dict, best_k: int) -> None:
    """Ghi de toan bo ket qua cu (chay lai khong bi trung) trong 1 transaction."""
    from psycopg2.extras import execute_values

    seg_rows = [
        (int(r.customer_key), int(r.customer_id), int(r.recency), int(r.frequency),
         round(float(r.monetary), 2), int(r.cluster_best), int(r.cluster_k4), r.segment_name)
        for r in result.itertuples(index=False)
    ]
    score_rows = [(int(k), round(s, 4), k == best_k, k == BUSINESS_K) for k, s in scores.items()]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE customer_segments, silhouette_scores;")
        execute_values(
            cur,
            """INSERT INTO customer_segments
               (customer_key, customer_id, recency, frequency, monetary,
                cluster_best, cluster_k4, segment_name) VALUES %s""",
            seg_rows,
            page_size=2000,
        )
        execute_values(
            cur,
            "INSERT INTO silhouette_scores (k, score, is_best, is_business) VALUES %s",
            score_rows,
        )
    conn.commit()


def main() -> int:
    # Console Windows co the khong in duoc tieng Viet (ten cum co dau) neu khong ep UTF-8
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    load_env_file()
    conn = connect()
    try:
        agg = fetch_customer_aggregates(conn)
        if agg.empty:
            print("[LOI] fact_sales khong co giao dich nao co khach hang - chay DAG truoc.")
            return 1
        rfm, reference = compute_rfm(agg)
        print(f"Doc {len(rfm):,} khach hang co Customer ID tu warehouse. "
              f"Ngay tham chieu: {reference.date()}")
        print(rfm[["recency", "frequency", "monetary"]].describe().round(1))

        result, scores, best_k = run_kmeans(rfm)
        print(f"\nK tot nhat theo Silhouette: {best_k} (score={scores[best_k]:.4f}); "
              f"K nghiep vu = {BUSINESS_K} (score={scores[BUSINESS_K]:.4f})")

        save_results(conn, result, scores, best_k)
        print(f"\nDa ghi {len(result):,} dong vao customer_segments va {len(scores)} dong vao silhouette_scores.")
        print(result["segment_name"].value_counts().to_string())
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
