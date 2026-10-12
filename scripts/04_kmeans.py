"""
Phan nhom khach hang bang K-Means tren dac trung RFM.

- Chuan hoa Recency, Frequency, Monetary bang StandardScaler
  (Monetary va Frequency lech phai manh -> log-transform truoc khi chuan hoa).
- Thu K tu 2 den 10, danh gia bang Silhouette Score, chon K tot nhat.
- Gan nhan cum cho tung khach hang va luu ket qua.
- Ve bieu do Silhouette Score theo K va luu anh.

Dau ra:
  data/processed/customer_segments.csv
  docs/silhouette_scores.png

Chay: uv run scripts/04_kmeans.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"
INPUT_PATH = PROCESSED_DIR / "rfm.csv"
OUTPUT_PATH = PROCESSED_DIR / "customer_segments.csv"
PLOT_PATH = DOCS_DIR / "silhouette_scores.png"

K_RANGE = range(2, 11)
RANDOM_STATE = 42


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Chua co {INPUT_PATH}. Chay scripts/03_rfm.py truoc.")

    rfm = pd.read_csv(INPUT_PATH)
    print(f"Doc {len(rfm):,} khach hang tu {INPUT_PATH.name}")

    # Log-transform de giam lech phai (Frequency, Monetary co duoi dai)
    features = pd.DataFrame(
        {
            "Recency": rfm["Recency"],
            "Frequency": np.log1p(rfm["Frequency"]),
            "Monetary": np.log1p(rfm["Monetary"]),
        }
    )

    scaler = StandardScaler()
    X = scaler.fit_transform(features)

    print("\n=== Thu nhieu K, danh gia Silhouette Score ===")
    scores = {}
    for k in K_RANGE:
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)
        scores[k] = score
        print(f"K={k}: silhouette={score:.4f}")

    best_k = max(scores, key=scores.get)
    print(f"\nK tot nhat theo Silhouette Score: {best_k} (score={scores[best_k]:.4f})")

    # K=2 thuong cho silhouette cao nhat nhung chi tach "con hoat dong" vs
    # "khong hoat dong", it y nghia kinh doanh. K=4 pho bien hon cho RFM
    # segmentation (vd: VIP, Trung thanh, Can giu chan, Sap mat) nen duoc
    # giu lai de so sanh du silhouette thap hon.
    business_k = 4 if best_k != 4 else 5
    print(
        f"K={business_k} (K tham khao de phan nhom co y nghia kinh doanh hon): "
        f"silhouette={scores[business_k]:.4f}"
    )

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        DOCS_DIR.mkdir(parents=True, exist_ok=True)
        plt.figure(figsize=(7, 4))
        plt.plot(list(scores.keys()), list(scores.values()), marker="o")
        plt.axvline(best_k, color="red", linestyle="--", label=f"K tot nhat = {best_k}")
        plt.xlabel("So cum (K)")
        plt.ylabel("Silhouette Score")
        plt.title("Silhouette Score theo K")
        plt.legend()
        plt.tight_layout()
        plt.savefig(PLOT_PATH, dpi=120)
        print(f"Da luu bieu do: {PLOT_PATH}")
    except Exception as e:
        print(f"[canh bao] Khong ve duoc bieu do: {e}")

    def profile(labels: pd.Series, k: int) -> pd.DataFrame:
        tmp = rfm.copy()
        tmp["Cluster"] = labels
        print(f"\n=== Dac diem tung cum (K={k}) ===")
        p = (
            tmp.groupby("Cluster")
            .agg(
                SoKhachHang=("Customer ID", "count"),
                Recency_TB=("Recency", "mean"),
                Frequency_TB=("Frequency", "mean"),
                Monetary_TB=("Monetary", "mean"),
            )
            .round(1)
            .sort_values("Monetary_TB", ascending=False)
        )
        print(p)
        return p

    best_km = KMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=10)
    rfm["Cluster_best"] = best_km.fit_predict(X)
    profile(rfm["Cluster_best"], best_k)

    business_km = KMeans(n_clusters=business_k, random_state=RANDOM_STATE, n_init=10)
    rfm["Cluster_business"] = business_km.fit_predict(X)
    profile(rfm["Cluster_business"], business_k)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    rfm.to_csv(OUTPUT_PATH, index=False)
    print(f"\nDa luu ket qua phan cum vao: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
