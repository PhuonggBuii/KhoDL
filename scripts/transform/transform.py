"""Phase 2.3 - Transform: ap dung quy tac nghiep vu tren dataset Online Retail II,
xuat du lieu san sang Load vao Star Schema (dim_date/dim_customer/dim_product/
dim_country/fact_sales).

NGUON DU LIEU: doc truc tiep data/raw/online_retail_II.xlsx (toan bo 8 cot).
Ly do KHONG join lai 3 "nguon" mo phong trong Data Lake: nguon "File" (Phase 1.2)
chi mang 5 cot giao dich (Invoice, StockCode, Quantity, InvoiceDate, Price),
KHONG co Customer ID/Country; nguon "Database" (Phase 1.3) chi co Customer ID +
Country DA KHU TRUNG theo khach hang (khong con lien ket Invoice). Hai nguon nay
KHONG co khoa chung de join lai thanh 1 dong giao dich day du - day la han che
da biet cua kien truc 3-nguon-mo-phong (xem docs/03_star_schema.md muc 6,
docs/PROJECT_GAP_ANALYSIS.md). Ba "nguon" deu mo phong tu CHINH dataset nay
(PROJECT_CHECKLIST.md muc "Gia dinh quan trong"), nen Transform doc truc tiep tu
day, dong thoi XAC THUC Data Lake raw/* (Phase 1/2.2) ton tai va so dong khop
truoc khi tinh toan (kiem tra lineage).

Quy tac nghiep vu ap dung, theo thu tu (xem docs/03_star_schema.md muc 6):
  1. Bo dong trung chinh xac (drop_duplicates tren toan bo cot).
  2. Loai hoa don huy (Invoice bat dau bang "C") khoi du lieu ban hang/RFM.
  3. Loai dong Quantity <= 0 HOAC Price <= 0.
  4. Loai StockCode khong phai hang hoa thuc (NON_PRODUCT_CODES, giong
     scripts/02_clean_data.py, dong bo quy tac).
  5. GIU giao dich hop le thieu Customer ID (KHAC scripts/02_clean_data.py) - se
     Load voi customer_key = NULL, khong dua vao RFM theo khach hang.

KHONG sua du lieu raw (chi doc, khong ghi lai data/raw/ hay datalake/raw/).

Chay: uv run scripts/transform/transform.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from common import PROCESSED_DIR, RAW_DIR, ensure_dirs, log_ingest, today_str  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
RAW_SOURCE_PATH = ROOT_DIR / "data" / "raw" / "online_retail_II.xlsx"

NON_PRODUCT_CODES = {
    "POST", "DOT", "D", "M", "m", "BANK CHARGES", "ADJUST", "S", "B",
    "AMAZONFEE", "CRUK", "GIFT", "DCGSSGIRL", "DCGSLBOY", "DCGSSBOY", "DCGSLGIRL",
}

_WHITESPACE_RE = re.compile(r"\s+")


def _find_latest_dir(base: Path) -> Path | None:
    if not base.exists():
        return None
    dirs = sorted((d for d in base.iterdir() if d.is_dir()), reverse=True)
    return dirs[0] if dirs else None


def _verify_data_lake_lineage(n_raw_expected: int) -> dict:
    """Xac thuc Data Lake raw/* (Extract) ton tai va so dong khop dataset goc.

    Day la kiem tra lineage (khong phai nguon du lieu de tinh toan - xem docstring
    dau file). Khong raise loi fatal neu thieu, chi ghi canh bao ro rang.
    """
    report: dict = {}

    file_dir = _find_latest_dir(RAW_DIR / "file")
    if file_dir is not None:
        file_csv = file_dir / "transactions_file_source.csv"
        if file_csv.exists():
            n_file = sum(1 for _ in file_csv.open(encoding="utf-8")) - 1  # tru header
            report["raw_file_rows"] = n_file
            report["raw_file_matches_dataset"] = n_file == n_raw_expected
        else:
            report["raw_file_rows"] = None
    else:
        report["raw_file_rows"] = None

    db_dir = _find_latest_dir(RAW_DIR / "database")
    if db_dir is not None:
        db_csv = db_dir / "customers_from_db.csv"
        if db_csv.exists():
            n_db = sum(1 for _ in db_csv.open(encoding="utf-8")) - 1
            report["raw_database_customers"] = n_db
        else:
            report["raw_database_customers"] = None
    else:
        report["raw_database_customers"] = None

    api_dir = _find_latest_dir(RAW_DIR / "api")
    if api_dir is not None:
        api_json = api_dir / "products.json"
        report["raw_api_exists"] = api_json.exists()
    else:
        report["raw_api_exists"] = False

    return report


def _normalize_description(series: pd.Series) -> pd.Series:
    """Trim + thu gon khoang trang (quyet dinh #4, docs/03_star_schema.md muc 6).

    KHONG ep hoa/thuong (da khao sat: so it dong chu thuong la du lieu test, ep
    buoc co the lam mat thong tin goc khong can thiet).
    """
    cleaned = series.astype(str).str.strip()
    return cleaned.str.replace(_WHITESPACE_RE, " ", regex=True)


def _build_dim_date(dates: pd.Series) -> pd.DataFrame:
    dt = pd.to_datetime(dates.unique())
    iso = dt.isocalendar()
    return pd.DataFrame(
        {
            "full_date": dt.date,
            "year": dt.year,
            "quarter": dt.quarter,
            "month": dt.month,
            "month_name": dt.month_name(),
            "day": dt.day,
            "day_of_week": iso["day"].to_numpy(),
            "day_name": dt.day_name(),
            "week_of_year": iso["week"].to_numpy(),
        }
    ).sort_values("full_date").reset_index(drop=True)


def _build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    work = df[["StockCode", "Description"]].copy()
    work["Description"] = _normalize_description(work["Description"].fillna(""))
    work.loc[work["Description"] == "", "Description"] = pd.NA

    rows = []
    for stock_code, group in work.groupby("StockCode", sort=False):
        descriptions = group["Description"].dropna()
        description = descriptions.mode().iloc[0] if not descriptions.empty else None
        rows.append({"stock_code": stock_code, "description": description})
    return pd.DataFrame(rows)


def run() -> int:
    ensure_dirs()
    out_dir = PROCESSED_DIR / today_str()
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        if not RAW_SOURCE_PATH.exists():
            raise FileNotFoundError(f"Khong tim thay dataset tai {RAW_SOURCE_PATH}")

        sheets = pd.read_excel(RAW_SOURCE_PATH, sheet_name=None)
        df = pd.concat(sheets.values(), ignore_index=True)
        n_raw = len(df)
        print(f"[transform] Doc duoc {n_raw:,} dong tu dataset goc ({len(sheets)} sheet).")

        lineage = _verify_data_lake_lineage(n_raw)
        print(f"[transform] Kiem tra lineage Data Lake: {lineage}")
        if lineage.get("raw_file_rows") is None:
            print("[transform] [CANH BAO] Khong tim thay raw/file/*/transactions_file_source.csv "
                  "(Extract chua chay?) - van tiep tuc vi Transform doc truc tiep dataset goc.")
        elif not lineage.get("raw_file_matches_dataset"):
            print(f"[transform] [CANH BAO] So dong raw/file ({lineage['raw_file_rows']:,}) "
                  f"KHONG khop dataset goc ({n_raw:,}).")

        removed = {}

        df = df.drop_duplicates()
        removed["duplicate_rows"] = n_raw - len(df)
        print(f"[transform] Sau khi bo trung lap: {len(df):,} dong (-{removed['duplicate_rows']:,})")

        n_before = len(df)
        df = df[~df["Invoice"].astype(str).str.startswith("C")]
        removed["cancelled_invoices"] = n_before - len(df)
        print(f"[transform] Sau khi loai don huy: {len(df):,} dong (-{removed['cancelled_invoices']:,})")

        n_before = len(df)
        df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]
        removed["quantity_or_price_invalid"] = n_before - len(df)
        print(f"[transform] Sau khi loai Quantity/Price <= 0: {len(df):,} dong "
              f"(-{removed['quantity_or_price_invalid']:,})")

        n_before = len(df)
        df = df[~df["StockCode"].astype(str).isin(NON_PRODUCT_CODES)]
        removed["non_product_codes"] = n_before - len(df)
        print(f"[transform] Sau khi loai ma phi/dieu chinh: {len(df):,} dong "
              f"(-{removed['non_product_codes']:,})")

        n_output = len(df)
        n_missing_customer = int(df["Customer ID"].isna().sum())
        print(f"[transform] GIU {n_missing_customer:,} dong thieu Customer ID "
              f"(khong loai, se Load voi customer_key = NULL).")
        print(f"[transform] Du lieu sau Transform: {n_output:,} dong "
              f"({n_output / n_raw * 100:.1f}% dataset goc).")

        df["TotalPrice"] = df["Quantity"] * df["Price"]
        total_revenue = float(df["TotalPrice"].sum())
        print(f"[transform] Tong TotalPrice (QA, se doi chieu lai o Phase 3.3): {total_revenue:,.2f}")

        # ---- Chuan bi dimension (chi gom gia tri XUAT HIEN trong du lieu da loc) ----
        dim_date = _build_dim_date(pd.to_datetime(df["InvoiceDate"]).dt.floor("D"))
        dim_customer = (
            df["Customer ID"].dropna().astype(int).drop_duplicates()
            .sort_values().rename("customer_id").reset_index(drop=True).to_frame()
        )
        dim_product = _build_dim_product(df)
        dim_country = (
            df["Country"].astype(str).drop_duplicates()
            .sort_values().rename("country_name").reset_index(drop=True).to_frame()
        )

        fact = pd.DataFrame(
            {
                "invoice_no": df["Invoice"].astype(str),
                "invoice_datetime": pd.to_datetime(df["InvoiceDate"]),
                "invoice_date": pd.to_datetime(df["InvoiceDate"]).dt.floor("D").dt.date,
                "customer_id": df["Customer ID"].astype("Int64"),  # nullable int
                "stock_code": df["StockCode"].astype(str),
                "country_name": df["Country"].astype(str),
                "quantity": df["Quantity"].astype(int),
                "unit_price": df["Price"].astype(float),
            }
        )

        dim_date.to_csv(out_dir / "dim_date.csv", index=False)
        dim_customer.to_csv(out_dir / "dim_customer.csv", index=False)
        dim_product.to_csv(out_dir / "dim_product.csv", index=False)
        dim_country.to_csv(out_dir / "dim_country.csv", index=False)
        fact.to_csv(out_dir / "fact_sales.csv", index=False)

        print(f"[transform] Dimension: dim_date={len(dim_date):,}, "
              f"dim_customer={len(dim_customer):,}, dim_product={len(dim_product):,}, "
              f"dim_country={len(dim_country):,}")
        print(f"[transform] Da luu vao: {out_dir}")

        log_ingest("transform", "success", n_output, str(out_dir))
        return 0
    except Exception as exc:  # noqa: BLE001
        log_ingest("transform", "failed", None, str(out_dir), error_message=str(exc))
        print(f"[LOI] Transform thua bai: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
