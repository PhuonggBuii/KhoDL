"""Ingest nguon 'File' (Phase 1.2) — doc truc tiep Excel goc Kaggle (ca 2 sheet
Year 2009-2010 + Year 2010-2011), trich cac truong giao dich toi thieu, luu
snapshot vao Data Lake raw/file/ (Phase 1.4) va ghi ingest log (Phase 1.5).

KHONG ap dung bo quy tac lam sach de xuat o Phase 0.3 (bo don huy, bo
Quantity/Price <= 0, bo ma phi...) — nhom chua xac nhan chinh thuc quy tac
nay cho pipeline giao dich, nen day la snapshot RAW giu nguyen du lieu goc,
chi chon cot can thiet.

Chay: uv run scripts/ingest/ingest_file.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import RAW_DIR, ensure_dirs, log_ingest, today_str  # noqa: E402

RAW_SOURCE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "online_retail_II.xlsx"
FILE_COLUMNS = ["Invoice", "StockCode", "Quantity", "InvoiceDate", "Price"]


def run() -> int:
    ensure_dirs()
    out_dir = RAW_DIR / "file" / today_str()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "transactions_file_source.csv"

    try:
        if not RAW_SOURCE_PATH.exists():
            raise FileNotFoundError(f"Khong tim thay dataset tai {RAW_SOURCE_PATH}")

        sheets = pd.read_excel(RAW_SOURCE_PATH, sheet_name=None)
        df_raw = pd.concat(sheets.values(), ignore_index=True)
        df = df_raw[FILE_COLUMNS].copy()

        n_raw = len(df_raw)
        n_out = len(df)
        if n_raw != n_out:
            raise AssertionError(f"So dong khong khop: raw={n_raw}, output={n_out}")

        df.to_csv(out_path, index=False)

        print(f"Doc duoc {n_raw:,} dong tu dataset goc ({len(sheets)} sheet: {list(sheets.keys())}).")
        print(f"Trich {n_out:,} dong, cot: {list(df.columns)}")
        print(df.dtypes)
        print(f"Mau 3 dong:\n{df.head(3)}")
        print(f"Da luu: {out_path}")

        log_ingest("file", "success", n_out, str(out_path))
        return 0
    except Exception as exc:  # noqa: BLE001 - ghi log loi ro rang, khong nuot loi am tham
        log_ingest("file", "failed", None, str(out_path), error_message=str(exc))
        print(f"[LOI] Ingest nguon File thua bai: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
