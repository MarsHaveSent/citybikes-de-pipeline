"""Создаёт .env из .env.example и заполняет пустые значения случайными секретами.

Если .env уже есть, дописывает только ключи, которых в нём нет.
"""

import base64
import secrets
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


def missing_lines(example: str, current: str) -> list[str]:
    """Строки для ключей из example, которых нет в current."""
    present = {line.partition("=")[0] for line in current.splitlines() if "=" in line}
    rendered = render_env(example).splitlines()
    return [
        line
        for line in rendered
        if "=" in line and not line.startswith("#") and line.partition("=")[0] not in present
    ]


def main() -> None:
    target = ROOT / ".env"
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    if not target.exists():
        target.write_text(render_env(example), "utf-8")
        print(f"Создан {target}")
        return
    current = target.read_text(encoding="utf-8")
    added = missing_lines(example, current)
    if not added:
        print(".env актуален")
        return
    separator = "" if current.endswith("\n") else "\n"
    with target.open("a", encoding="utf-8") as f:
        f.write(separator + "\n".join(added) + "\n")
    print("Добавлены ключи:", ", ".join(line.partition("=")[0] for line in added))


if __name__ == "__main__":
    main()
