"""Phase 5.2-5.4 (nguon DW) - Chuan hoa dac trung RFM-tu-DW, thu nhieu K, danh gia
Silhouette Score, gan nhan cum va dien giai, tren dau ra cua scripts/mining/rfm_dw.py.

KHONG import/sua scripts/04_kmeans.py (baseline doc lap, giu nguyen de doi chieu).
Logic tien xu ly/danh gia duoc TAI SU DUNG Y NHU baseline (xem doi chieu tung
buoc trong docstring ben duoi), chi doi nguon input.

Tien xu ly (giong scripts/04_kmeans.py, KHONG log1p Recency, KHONG log1p 2 lan):
  - Recency  : giu nguyen (khong log-transform).
  - Frequency: log1p(Frequency) truoc khi chuan hoa (lech phai).
  - Monetary : log1p(Monetary) truoc khi chuan hoa (lech phai).
  - StandardScaler tren ca 3 cot sau buoc tren.

K-Means:
  - Thu K = 2..10, random_state=42, n_init=10 (co dinh de tai lap, giong baseline).
  - best_k   = K co Silhouette Score cao nhat (thuan thuat toan, thuong la K=2 vi
    chi tach "hoat dong" vs "khong hoat dong").
  - business_k = K dung de dien giai nghiep vu (4 cum: VIP/Trung thanh/Moi-it
    tuong tac/Nguy co roi bo - da duoc tai lieu hoa o docs/01_bao_cao_tien_do.md
    cho baseline). Ap dung CUNG quy tac tie-break nhu scripts/04_kmeans.py:
    business_k = 4 neu best_k != 4, nguoc lai = 5 (de business_k luon la 1 K
    THAM KHAO khac best_k, giong chu y trong baseline).
  - KHONG mac dinh business_k=4 la "dung" - script luon TINH LAI best_k tu du
    lieu DW va in ro so sanh Silhouette cua ca 2 K.

Nhan cum/segment_name: suy ra tu so lieu thuc te (trung binh Recency/Frequency/
Monetary moi cum so voi trung binh cac cum), KHONG gan cung theo thu tu cum cua
baseline (nhan cum cua KMeans la chi so ngau nhien, khong co thu tu co dinh giua
2 lan chay khac nhau). Quy tac dien giai la heuristic, duoc ghi ro trong
docs/05_rfm_kmeans_dw.md - khong phai chuan nganh.

Dau ra:
  data/processed/customer_segments_from_dw.csv  (Customer ID, Recency, Frequency,
    Monetary, Cluster_best, Cluster_business, segment_name)
  data/processed/silhouette_scores_from_dw.csv  (K, silhouette_score)
  docs/silhouette_scores_from_dw.png

Chay: uv run scripts/mining/cluster_dw.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

PROCESSED_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "processed"
DOCS_DIR = Path(__file__).resolve().parent.parent.parent / "docs"
INPUT_PATH = PROCESSED_DIR / "rfm_from_dw.csv"
OUTPUT_PATH = PROCESSED_DIR / "customer_segments_from_dw.csv"
SILHOUETTE_CSV_PATH = PROCESSED_DIR / "silhouette_scores_from_dw.csv"
PLOT_PATH = DOCS_DIR / "silhouette_scores_from_dw.png"
BASELINE_SEGMENTS_PATH = PROCESSED_DIR / "customer_segments.csv"

K_RANGE = range(2, 11)
RANDOM_STATE = 42
N_INIT = 10


def _label_clusters(profile: pd.DataFrame) -> dict[int, str]:
    """Gan ten doc hieu cho tung cum dua tren XEP HANG tuong doi giua cac cum
    (khong dung z-score tuyet doi - khong on dinh khi K nho, vd K=4 chi co 4
    diem du lieu). Heuristic 2 buoc, documented - xem docs/05_rfm_kmeans_dw.md:

      1. Cum co Recency_TB CAO NHAT (lau khong mua lai nhat) -> "Nguy co roi bo",
         bat ke Frequency/Monetary (dung dung quy tac da ap dung cho baseline
         K=4 - docs/01_bao_cao_tien_do.md muc 5).
      2. Trong cac cum con lai, xep hang theo (hang Frequency_TB giam + hang
         Monetary_TB giam), diem thap nhat (tot nhat) -> "Khach hang VIP", tiep
         theo -> "Khach hang trung thanh", con lai -> "Khach hang moi / it
         tuong tac".

    Da doi chieu: voi K=4 tren du lieu DW, quy tac nay cho dung 4 nhan nhu
    baseline (docs/01_bao_cao_tien_do.md muc 5), vi so lieu RFM-tu-DW giong
    tuyet doi baseline (xem doi chieu trong rfm_dw.py).
    """
    max_recency_cluster = profile["Recency_TB"].idxmax()
    labels: dict[int, str] = {max_recency_cluster: "Nguy co roi bo"}

    remaining = [c for c in profile.index if c != max_recency_cluster]
    if remaining:
        freq_rank = profile.loc[remaining, "Frequency_TB"].rank(ascending=False)
        monetary_rank = profile.loc[remaining, "Monetary_TB"].rank(ascending=False)
        value_score = freq_rank + monetary_rank
        ordered = value_score.sort_values().index.tolist()
        tier_names = ["Khach hang VIP", "Khach hang trung thanh"]
        for i, cluster_id in enumerate(ordered):
            labels[cluster_id] = tier_names[i] if i < len(tier_names) else "Khach hang moi / it tuong tac"
    return labels


def _profile(rfm: pd.DataFrame, labels: np.ndarray, k: int, title: str) -> pd.DataFrame:
    tmp = rfm.copy()
    tmp["Cluster"] = labels
    print(f"\n=== Dac diem tung cum ({title}, K={k}) ===")
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


def run() -> int:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Chua co {INPUT_PATH}. Chay scripts/mining/rfm_dw.py truoc.")

    rfm = pd.read_csv(INPUT_PATH)
    print(f"[cluster_dw] Doc {len(rfm):,} khach hang tu {INPUT_PATH.name}")

    features = pd.DataFrame(
        {
            "Recency": rfm["Recency"],
            "Frequency": np.log1p(rfm["Frequency"]),
            "Monetary": np.log1p(rfm["Monetary"]),
        }
    )
    scaler = StandardScaler()
    X = scaler.fit_transform(features)

    print("\n=== Thu nhieu K, danh gia Silhouette Score (DW) ===")
    scores: dict[int, float] = {}
    for k in K_RANGE:
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=N_INIT)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)
        scores[k] = score
        print(f"K={k}: silhouette={score:.4f}")

    silhouette_df = pd.DataFrame({"K": list(scores.keys()), "silhouette_score": list(scores.values())})
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    silhouette_df.to_csv(SILHOUETTE_CSV_PATH, index=False)
    print(f"[cluster_dw] Da luu bang Silhouette day du vao: {SILHOUETTE_CSV_PATH}")

    best_k = max(scores, key=scores.get)
    business_k = 4 if best_k != 4 else 5
    print(f"\n[cluster_dw] K tot nhat theo Silhouette (DW): {best_k} (score={scores[best_k]:.4f})")
    print(f"[cluster_dw] K tham khao nghiep vu (DW): {business_k} (score={scores[business_k]:.4f})")

    baseline_k4_score = None
    if BASELINE_SEGMENTS_PATH.exists():
        try:
            baseline_segments = pd.read_csv(BASELINE_SEGMENTS_PATH)
            baseline_k4_score = 0.3611  # tu docs/01_bao_cao_tien_do.md (baseline, da bao cao truoc)
        except Exception:  # noqa: BLE001
            baseline_segments = None
    print(f"[cluster_dw] Doi chieu: Silhouette K=4 baseline (CSV) ~= {baseline_k4_score}; "
          f"K=4 tren DW = {scores.get(4):.4f} (xem giai thich lech trong docs/05_rfm_kmeans_dw.md).")

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
        plt.title("Silhouette Score theo K (RFM tu DW)")
        plt.legend()
        plt.tight_layout()
        plt.savefig(PLOT_PATH, dpi=120)
        print(f"[cluster_dw] Da luu bieu do: {PLOT_PATH}")
    except Exception as e:  # noqa: BLE001
        print(f"[canh bao] Khong ve duoc bieu do: {e}")

    best_km = KMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=N_INIT)
    rfm["Cluster_best"] = best_km.fit_predict(X)
    _profile(rfm, rfm["Cluster_best"].to_numpy(), best_k, "best_k")

    business_km = KMeans(n_clusters=business_k, random_state=RANDOM_STATE, n_init=N_INIT)
    rfm["Cluster_business"] = business_km.fit_predict(X)
    business_profile = _profile(rfm, rfm["Cluster_business"].to_numpy(), business_k, "business_k")

    cluster_labels = _label_clusters(business_profile)
    rfm["segment_name"] = rfm["Cluster_business"].map(cluster_labels)

    print("\n=== Dien giai cum (business_k, dua tren segment_name) ===")
    for cluster_id, name in cluster_labels.items():
        row = business_profile.loc[cluster_id]
        print(f"  Cluster {cluster_id} -> {name}: SoKhachHang={row['SoKhachHang']:.0f}, "
              f"Recency_TB={row['Recency_TB']:.1f}, Frequency_TB={row['Frequency_TB']:.1f}, "
              f"Monetary_TB={row['Monetary_TB']:.1f}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    rfm.to_csv(OUTPUT_PATH, index=False)
    print(f"\n[cluster_dw] Da luu ket qua phan cum vao: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
