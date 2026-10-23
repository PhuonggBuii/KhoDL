"""Cau hinh toi gian cho Superset (Phase 0.2 — moi truong).

Metadata cua Superset dung SQLite mac dinh trong volume rieng (khong them
Postgres/Redis thu 4 de giam so cong nghe van hanh, dung tinh than muc 2
cua PROJECT_CHECKLIST.md). Ket noi Superset -> PostgreSQL DW (dashboard
thuc) se duoc khai bao qua UI o Phase 6, chua lam trong Phase 0.2 nay.
"""

import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "")
