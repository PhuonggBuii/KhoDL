"""Nap + cache danh muc san pham tu dataset Online Retail II cho FastAPI /products.

Phase 1.1 — chi lam sach toi thieu can cho muc dich "danh muc san pham":
bo StockCode rong/thieu, chon mo ta pho bien nhat, gia tham chieu = median cac
dong co Price > 0. KHONG ap dung bo quy tac lam sach day du o 0.3 (bo don huy,
bo ma phi...) vi do la quy tac cho pipeline giao dich, chua duoc nhom xac nhan
chinh thuc va khong phu hop pham vi "danh sach san pham".
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "online_retail_II.xlsx"


class Product(BaseModel):
    StockCode: str
    Description: str | None = None
    UnitPrice: float | None = None


_catalog_cache: dict[str, Product] | None = None


def _build_catalog() -> dict[str, Product]:
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Khong tim thay dataset tai {DATA_PATH}. Hay dat file 'online_retail_II.xlsx' "
            "vao data/raw/ (xem data/README.md) va mount volume ./data:/app/data trong "
            "docker-compose.yml."
        )

    sheets = pd.read_excel(DATA_PATH, sheet_name=None)
    df = pd.concat(sheets.values(), ignore_index=True)

    df = df.dropna(subset=["StockCode"])
    df["StockCode"] = df["StockCode"].astype(str).str.strip()
    df = df[df["StockCode"] != ""]

    catalog: dict[str, Product] = {}
    for stock_code, group in df.groupby("StockCode", sort=False):
        descriptions = group["Description"].dropna()
        description = str(descriptions.mode().iloc[0]) if not descriptions.empty else None

        positive_prices = group.loc[group["Price"] > 0, "Price"]
        unit_price = float(positive_prices.median()) if not positive_prices.empty else None

        catalog[stock_code] = Product(
            StockCode=stock_code,
            Description=description,
            UnitPrice=unit_price,
        )

    logger.info("Da nap %d san pham duy nhat tu dataset", len(catalog))
    return catalog


def get_catalog() -> dict[str, Product]:
    """Tra ve danh muc san pham, chi doc + build tu Excel o lan goi dau tien."""
    global _catalog_cache
    if _catalog_cache is None:
        _catalog_cache = _build_catalog()
    return _catalog_cache
