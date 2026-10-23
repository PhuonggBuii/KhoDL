"""Tien ich chung cho cac script ingest Phase 1 (Data Lake + ingest log)."""

from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATALAKE_DIR = ROOT_DIR / "datalake"
RAW_DIR = DATALAKE_DIR / "raw"
PROCESSED_DIR = DATALAKE_DIR / "processed"
METADATA_DIR = DATALAKE_DIR / "metadata"
INGEST_LOG_PATH = METADATA_DIR / "ingest_log.csv"

LOG_FIELDS = ["timestamp_utc", "source", "status", "record_count", "output_path", "error_message"]


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ensure_dirs() -> None:
    for d in (RAW_DIR / "api", RAW_DIR / "file", RAW_DIR / "database", PROCESSED_DIR, METADATA_DIR):
        d.mkdir(parents=True, exist_ok=True)


def load_env_file(path: Path | None = None) -> None:
    """Doc file .env o goc repo (KEY=VALUE) vao os.environ, khong de ghi de bien da co san.

    Thay cho them dependency python-dotenv — chi can parser don gian cho dinh
    dang .env hien co cua du an.
    """
    env_path = path or (ROOT_DIR / ".env")
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def log_ingest(
    source: str,
    status: str,
    record_count: int | None,
    output_path: str,
    error_message: str = "",
) -> None:
    """Ghi 1 dong vao datalake/metadata/ingest_log.csv (append, khong ghi de lich su cu)."""
    ensure_dirs()
    is_new = not INGEST_LOG_PATH.exists()
    with INGEST_LOG_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source": source,
                "status": status,
                "record_count": record_count if record_count is not None else "",
                "output_path": output_path,
                "error_message": error_message,
            }
        )
