"""Ingest nguon 'API' — goi FastAPI /products thuc te (Phase 1.1), luu JSON
vao Data Lake raw/api/ (Phase 1.4) va ghi ingest log (Phase 1.5).

Yeu cau: service fastapi dang chay va healthy (docker compose up -d --build
fastapi). Host/port doc tu FASTAPI_HOST/FASTAPI_PORT (.env hoac moi truong),
mac dinh "localhost"/8000 de chay dung tren may host nhu truoc (Phase 1). Khi
chay trong container Airflow (Phase 2.2), docker-compose.yml dat
FASTAPI_HOST=fastapi (ten service Docker) vi "localhost" trong container
Airflow khong tro toi container fastapi.

Chay: uv run scripts/ingest/ingest_api.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import RAW_DIR, ensure_dirs, load_env_file, log_ingest, today_str  # noqa: E402


def run() -> int:
    load_env_file()
    ensure_dirs()
    host = os.environ.get("FASTAPI_HOST", "localhost")
    port = os.environ.get("FASTAPI_PORT", "8000")
    url = f"http://{host}:{port}/products"

    out_dir = RAW_DIR / "api" / today_str()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "products.json"

    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            if resp.status != 200:
                raise RuntimeError(f"API tra ve HTTP {resp.status}")
            data = json.loads(resp.read().decode("utf-8"))

        out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        n = len(data)
        print(f"Goi {url} thanh cong, nhan {n:,} san pham.")
        print(f"Da luu: {out_path}")

        log_ingest("api", "success", n, str(out_path))
        return 0
    except (urllib.error.URLError, RuntimeError, OSError) as exc:
        log_ingest("api", "failed", None, str(out_path), error_message=str(exc))
        print(f"[LOI] Ingest nguon API thua bai: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
