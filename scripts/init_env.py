"""Создаёт .env из .env.example и заполняет пустые значения случайными секретами."""

import base64
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def generate_secret(key: str) -> str:
    if key == "AIRFLOW_FERNET_KEY":
        # Fernet-ключ - 32 случайных байта в urlsafe base64
        return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    # hex, чтобы пароль подставлялся в URI подключения без экранирования
    return secrets.token_hex(16)


def render_env(example: str) -> str:
    lines = []
    for line in example.splitlines():
        key, sep, value = line.partition("=")
        if sep and not line.startswith("#") and not value:
            line = f"{key}={generate_secret(key)}"
        lines.append(line)
    return "\n".join(lines) + "\n"


def main() -> None:
    target = ROOT / ".env"
    if target.exists():
        sys.exit(".env уже существует. Чтобы пересоздать, удалите его вручную.")
    target.write_text(render_env((ROOT / ".env.example").read_text(encoding="utf-8")), "utf-8")
    print(f"Создан {target}")


if __name__ == "__main__":
    main()
