"""
Tinh RFM (Recency, Frequency, Monetary) cho tung khach hang
tu du lieu da lam sach (data/processed/transactions_clean.csv).

- Recency  = so ngay tu lan mua gan nhat den ngay tham chieu
             (ngay tham chieu = ngay giao dich gan nhat trong du lieu + 1 ngay)
- Frequency = so hoa don (Invoice) duy nhat cua khach hang
- Monetary  = tong TotalPrice (Quantity * Price) cua khach hang

Dau ra: data/processed/rfm.csv

Chay: uv run scripts/03_rfm.py
"""

from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
INPUT_PATH = PROCESSED_DIR / "transactions_clean.csv"
OUTPUT_PATH = PROCESSED_DIR / "rfm.csv"


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Chua co {INPUT_PATH}. Chay scripts/02_clean_data.py truoc.")

    df = pd.read_csv(INPUT_PATH, parse_dates=["InvoiceDate"])
    print(f"Doc {len(df):,} dong giao dich sach, {df['Customer ID'].nunique():,} khach hang")

    reference_date = df["InvoiceDate"].max() + pd.Timedelta(days=1)
    print(f"Ngay tham chieu (reference date) cho Recency: {reference_date.date()}")

    rfm = (
        df.groupby("Customer ID")
        .agg(
            Recency=("InvoiceDate", lambda s: (reference_date - s.max()).days),
            Frequency=("Invoice", "nunique"),
            Monetary=("TotalPrice", "sum"),
        )
        .reset_index()
    )

    print("\n=== Thong ke RFM ===")
    print(rfm[["Recency", "Frequency", "Monetary"]].describe())

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    rfm.to_csv(OUTPUT_PATH, index=False)
    print(f"\nDa luu {len(rfm):,} dong RFM vao: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
