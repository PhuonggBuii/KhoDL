"""Client toi gian cho Superset REST API (dung urllib, khong them dependency
moi - giong tinh than scripts/ingest/ingest_api.py).

Dung JWT Bearer (tu /api/v1/security/login) + CSRF token (tu
/api/v1/security/csrf_token/) + session cookie (bat buoc di kem CSRF token
cua Flask-WTF/Superset) cho moi request thay doi trang thai (POST/PUT/DELETE).
"""

from __future__ import annotations

import http.cookiejar
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from common import load_env_file  # noqa: E402


class SupersetClient:
    def __init__(self) -> None:
        load_env_file()
        self.base_url = f"http://localhost:{os.environ.get('SUPERSET_PORT', '8088')}"
        self.username = os.environ["SUPERSET_ADMIN_USER"]
        self.password = os.environ["SUPERSET_ADMIN_PASSWORD"]
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar))
        self.access_token: str | None = None
        self.csrf_token: str | None = None

    def _request(self, method: str, path: str, body: dict | None = None, auth: bool = True) -> dict:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if auth and self.access_token:
            req.add_header("Authorization", f"Bearer {self.access_token}")
        if auth and self.csrf_token and method in ("POST", "PUT", "DELETE"):
            req.add_header("X-CSRFToken", self.csrf_token)
            req.add_header("Referer", self.base_url)
        try:
            with self.opener.open(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {raw}") from None

    def login(self) -> None:
        resp = self._request(
            "POST",
            "/api/v1/security/login",
            {"username": self.username, "password": self.password, "provider": "db", "refresh": True},
            auth=False,
        )
        self.access_token = resp["access_token"]
        csrf_resp = self._request("GET", "/api/v1/security/csrf_token/")
        self.csrf_token = csrf_resp["result"]

    def get(self, path: str) -> dict:
        return self._request("GET", path)

    def post(self, path: str, body: dict) -> dict:
        return self._request("POST", path, body)

    def put(self, path: str, body: dict) -> dict:
        return self._request("PUT", path, body)

    def find_one(self, resource: str, name_column: str, name_value: str) -> dict | None:
        """Tim 1 object theo ten (idempotency: tranh tao trung khi chay lai script).

        Liet ke toan bo (page_size lon) va loc o client - tranh phai tu ma hoa
        Rison (dinh dang query param rieng cua Flask-AppBuilder) cho 1 script
        provisioning nho, chay 1 lan, danh sach object it.
        """
        page = 0
        while True:
            resp = self.get(f"/api/v1/{resource}/?q=(page:{page},page_size:100)")
            results = resp.get("result", [])
            for item in results:
                if item.get(name_column) == name_value:
                    return item
            if len(results) < 100:
                return None
            page += 1
