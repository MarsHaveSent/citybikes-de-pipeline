"""Первичная настройка Metabase через API: администратор и подключение к ClickHouse.

Повторный запуск безопасен: администратор создаётся один раз, подключение к ClickHouse
обновляется, например после смены пароля в .env. На сервере скрипт запускает deploy.sh,
локально: uv run python scripts/metabase_setup.py

Только стандартная библиотека: на сервере скрипт выполняется системным python3.
"""

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
METABASE_URL = os.environ.get("METABASE_URL", "http://127.0.0.1:3000")
DATABASE_NAME = "CityBikes ClickHouse"


def read_env(path: Path) -> dict[str, str]:
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and not line.startswith("#"):
            env[key.strip()] = value.strip()
    return env


class Metabase:
    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session: str | None = None

    def request(self, method: str, path: str, body: dict | None = None):
        request = urllib.request.Request(
            self.url + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={"Content-Type": "application/json"},
        )
        if self.session:
            request.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read()
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"{method} {path}: {e.code} {e.read().decode()[:500]}") from None
        return json.loads(raw) if raw else None

    def wait_healthy(self, timeout_s: int = 600) -> None:
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                if self.request("GET", "/api/health").get("status") == "ok":
                    return
            except (OSError, RuntimeError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"Metabase не ответил за {timeout_s} с")
            time.sleep(5)

    def login(self, email: str, password: str) -> None:
        properties = self.request("GET", "/api/session/properties")
        if properties.get("has-user-setup"):
            body = {"username": email, "password": password}
            self.session = self.request("POST", "/api/session", body)["id"]
            return
        # До первой настройки любой, кто откроет Metabase, может стать администратором,
        # поэтому скрипт запускается сразу после старта сервиса
        setup = {
            "token": properties["setup-token"],
            "user": {
                "email": email,
                "password": password,
                "first_name": "CityBikes",
                "last_name": "Admin",
                "site_name": "CityBikes",
            },
            "prefs": {"site_name": "CityBikes", "site_locale": "ru", "allow_tracking": False},
        }
        self.session = self.request("POST", "/api/setup", setup)["id"]
        print(f"Создан администратор {email}")

    def ensure_clickhouse(self, password: str) -> int:
        details = {
            "host": "clickhouse",
            "port": 8123,
            "user": "metabase",
            "password": password,
            "enable-multiple-db": False,
            "dbname": "citybikes",
            "ssl": False,
        }
        databases = self.request("GET", "/api/database")["data"]
        existing = next((db for db in databases if db["name"] == DATABASE_NAME), None)
        if existing:
            self.request("PUT", f"/api/database/{existing['id']}", {"details": details})
            return existing["id"]
        body = {"engine": "clickhouse", "name": DATABASE_NAME, "details": details}
        created = self.request("POST", "/api/database", body)
        print(f"Подключена база {DATABASE_NAME}")
        return created["id"]


def main() -> None:
    env = read_env(ROOT / ".env")
    metabase = Metabase(METABASE_URL)
    metabase.wait_healthy()
    metabase.login(env["METABASE_ADMIN_EMAIL"], env["METABASE_ADMIN_PASSWORD"])
    metabase.ensure_clickhouse(env["CLICKHOUSE_METABASE_PASSWORD"])
    print("Metabase настроен")


if __name__ == "__main__":
    main()
