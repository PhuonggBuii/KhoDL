"""Chay toan bo pipeline ingest Phase 1 (API -> File -> Database) tuan tu,
in tong ket va tra ve exit code != 0 neu co nguon loi.

Yeu cau: postgres-source va fastapi dang chay healthy (docker compose up -d
postgres-source fastapi).

Chay: uv run scripts/ingest/run_all.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ingest_api  # noqa: E402
import ingest_database  # noqa: E402
import ingest_file  # noqa: E402


def main() -> int:
    results = {}
    for name, module in (("api", ingest_api), ("file", ingest_file), ("database", ingest_database)):
        print(f"\n===== Ingest nguon: {name} =====")
        results[name] = module.run()

    print("\n=== Tong ket Phase 1 ingest ===")
    for source, code in results.items():
        print(f"  {source}: {'OK' if code == 0 else 'LOI'}")

    return 0 if all(code == 0 for code in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
