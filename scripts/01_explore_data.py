"""
Khao sat du lieu Online Retail II.

Doc file thuc tu data/raw/ (.xlsx hoac .csv), kiem tra chat luong du lieu
(thieu, trung lap, don huy, gia tri bat thuong) va in bao cao tom tat.
Chay: uv run scripts/01_explore_data.py
"""

from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

EXPECTED_COLUMNS = {
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
}


def find_raw_file() -> Path:
    candidates = [
        p for p in RAW_DIR.iterdir() if p.suffix.lower() in (".xlsx", ".csv") and p.name != ".gitkeep"
    ]
    if not candidates:
        raise FileNotFoundError(
            f"Khong tim thay file du lieu trong {RAW_DIR}. "
            "Hay tai 'online_retail_II.xlsx' (hoac .csv) tu Kaggle va dat vao thu muc nay."
        )
    if len(candidates) > 1:
        print(f"[canh bao] Co nhieu file trong data/raw/: {[c.name for c in candidates]}. Dung file dau tien.")
    return candidates[0]


def load_raw(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".xlsx":
        sheets = pd.read_excel(path, sheet_name=None)
        print(f"File Excel co {len(sheets)} sheet: {list(sheets.keys())}")
        df = pd.concat(sheets.values(), ignore_index=True)
    else:
        df = pd.read_csv(path, encoding="ISO-8859-1")
    return df


def explore(df: pd.DataFrame) -> None:
    print("\n=== 1. KICH THUOC & COT ===")
    print(f"So dong: {len(df):,}")
    print(f"So cot: {df.shape[1]}")
    print(f"Ten cot: {list(df.columns)}")

    missing_cols = EXPECTED_COLUMNS - set(df.columns)
    if missing_cols:
        print(f"[canh bao] Thieu cot mong doi: {missing_cols}")

    print("\n=== 2. KIEU DU LIEU ===")
    print(df.dtypes)

    print("\n=== 3. GIA TRI THIEU (NULL) ===")
    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df) * 100).round(2)
    print(pd.DataFrame({"so_luong": null_counts, "ty_le_%": null_pct}))

    print("\n=== 4. DONG TRUNG LAP ===")
    dup_count = df.duplicated().sum()
    print(f"So dong trung lap hoan toan: {dup_count:,} ({dup_count / len(df) * 100:.2f}%)")

    if "Invoice" in df.columns:
        print("\n=== 5. DON HANG HUY (Invoice bat dau bang 'C') ===")
        invoice_str = df["Invoice"].astype(str)
        cancelled = invoice_str.str.startswith("C")
        print(f"So dong don huy: {cancelled.sum():,} ({cancelled.sum() / len(df) * 100:.2f}%)")

    if "Quantity" in df.columns:
        print("\n=== 6. QUANTITY BAT THUONG ===")
        print(df["Quantity"].describe())
        neg_qty = (df["Quantity"] < 0).sum()
        print(f"So dong Quantity <= 0: {neg_qty:,}")

    if "Price" in df.columns:
        print("\n=== 7. PRICE BAT THUONG ===")
        print(df["Price"].describe())
        neg_price = (df["Price"] <= 0).sum()
        print(f"So dong Price <= 0: {neg_price:,}")

    if "Customer ID" in df.columns:
        print("\n=== 8. KHACH HANG ===")
        print(f"So Customer ID duy nhat: {df['Customer ID'].nunique():,}")
        print(f"So dong thieu Customer ID: {df['Customer ID'].isnull().sum():,}")

    if "InvoiceDate" in df.columns:
        print("\n=== 9. KHOANG THOI GIAN ===")
        dates = pd.to_datetime(df["InvoiceDate"], errors="coerce")
        print(f"Tu {dates.min()} den {dates.max()}")

    if "Country" in df.columns:
        print("\n=== 10. QUOC GIA (top 10) ===")
        print(df["Country"].value_counts().head(10))


def main() -> None:
    raw_path = find_raw_file()
    print(f"Doc du lieu tu: {raw_path.name}")
    df = load_raw(raw_path)
    explore(df)


if __name__ == "__main__":
    main()
