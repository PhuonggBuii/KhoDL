"""FastAPI service — mo phong nguon API cho Phase 1.1 (ingest 3 nguon).

/products va /products/{code}: danh muc san pham suy ra tu dataset Online
Retail II (xem app/data.py). Day la nguon "API" mo phong trong kien truc 3
nguon — khong phai API ben ngoai thuc te.
"""

from fastapi import FastAPI, HTTPException

from app.data import Product, get_catalog

app = FastAPI(title="KhoDL API mock", version="0.2.0")


@app.get("/")
def root() -> dict:
    return {"service": "khodl-api", "status": "ok"}


@app.get("/health")
def health() -> dict:
    return {"status": "healthy"}


@app.get("/products", response_model=list[Product])
def list_products() -> list[Product]:
    try:
        catalog = get_catalog()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return list(catalog.values())


@app.get("/products/{stock_code}", response_model=Product)
def get_product(stock_code: str) -> Product:
    try:
        catalog = get_catalog()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    product = catalog.get(stock_code)
    if product is None:
        raise HTTPException(status_code=404, detail=f"Khong tim thay StockCode '{stock_code}'")
    return product
