"""
Lam sach du lieu Online Retail II (buoc Transform cua ETL).

Cac buoc lam sach (co ghi ro ly do):
  1. Gop 2 sheet (2009-2010, 2010-2011) thanh 1 bang.
  2. Bo dong trung lap hoan toan.
  3. Bo dong thieu Customer ID (khong the gan giao dich cho khach hang -> khong dung cho RFM/segmentation).
  4. Bo don hang huy (Invoice bat dau bang 'C') - day la giao dich tra hang, khong phai mua hang.
  5. Bo dong Quantity <= 0 hoac Price <= 0 (giao dich loi / dieu chinh, khong phai doanh thu thuc).
  6. Bo cac StockCode khong phai san pham thuc (phi, dieu chinh so sach): POST, DOT, D, M,
     BANK CHARGES, ADJUST, S, B, AMAZONFEE, CRUK, GIFT, DCGSSGIRL, DCGSLBOY, DCGSSBOY, DCGSLGIRL.
  7. Tinh TotalPrice = Quantity * Price.

Dau ra: data/processed/transactions_clean.csv

Chay: uv run scripts/02_clean_data.py
"""

from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

NON_PRODUCT_CODES = {
    "POST", "DOT", "D", "M", "m", "BANK CHARGES", "ADJUST", "S", "B",
    "AMAZONFEE", "CRUK", "GIFT", "DCGSSGIRL", "DCGSLBOY", "DCGSSBOY", "DCGSLGIRL",
}


def find_raw_file() -> Path:
    candidates = [p for p in RAW_DIR.iterdir() if p.suffix.lower() in (".xlsx", ".csv") and p.name != ".gitkeep"]
    if not candidates:
        raise FileNotFoundError(f"Khong tim thay file du lieu trong {RAW_DIR}")
    return candidates[0]


def load_raw(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".xlsx":
        sheets = pd.read_excel(path, sheet_name=None)
        df = pd.concat(sheets.values(), ignore_index=True)
    else:
        df = pd.read_csv(path, encoding="ISO-8859-1")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    n0 = len(df)
    print(f"Du lieu tho: {n0:,} dong")

    df = df.drop_duplicates()
    n1 = len(df)
    print(f"Sau khi bo trung lap: {n1:,} dong (-{n0 - n1:,})")

    df = df.dropna(subset=["Customer ID"])
    n2 = len(df)
    print(f"Sau khi bo thieu Customer ID: {n2:,} dong (-{n1 - n2:,})")

    df = df[~df["Invoice"].astype(str).str.startswith("C")]
    n3 = len(df)
    print(f"Sau khi bo don huy: {n3:,} dong (-{n2 - n3:,})")

    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]
    n4 = len(df)
    print(f"Sau khi bo Quantity/Price <= 0: {n4:,} dong (-{n3 - n4:,})")

    df = df[~df["StockCode"].astype(str).isin(NON_PRODUCT_CODES)]
    n5 = len(df)
    print(f"Sau khi bo ma phi/dieu chinh: {n5:,} dong (-{n4 - n5:,})")

    df["Customer ID"] = df["Customer ID"].astype(int)
    df["TotalPrice"] = df["Quantity"] * df["Price"]

    print(f"\nDu lieu sach cuoi cung: {n5:,} dong ({n5 / n0 * 100:.1f}% so voi ban dau)")
    return df


def main() -> None:
    raw_path = find_raw_file()
    print(f"Doc du lieu tu: {raw_path.name}")
    df = load_raw(raw_path)
    clean_df = clean(df)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / "transactions_clean.csv"
    clean_df.to_csv(out_path, index=False)
    print(f"\nDa luu: {out_path}")


if __name__ == "__main__":
    main()
