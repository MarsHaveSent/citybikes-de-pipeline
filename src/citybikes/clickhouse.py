"""Загрузка снимков витрины в ClickHouse."""

import logging
from datetime import date
from pathlib import Path
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)


def connect(conn: Any) -> Client:
    """Клиент к базе из подключения Airflow (host, port, login, password, schema).

    Образ ClickHouse создаёт базу из CLICKHOUSE_DB только при первом старте на пустом томе,
    поэтому база создаётся здесь.
    """
    params = {
        "host": conn.host,
        "port": conn.port,
        "username": conn.login,
        "password": conn.password,
    }
    admin = clickhouse_connect.get_client(**params)
    try:
        admin.command(f"CREATE DATABASE IF NOT EXISTS {conn.schema}")
    finally:
        admin.close()
    return clickhouse_connect.get_client(**params, database=conn.schema)


def run_script(client: Client, path: Path) -> None:
    """Выполняет SQL-файл по одной команде: clickhouse-connect не принимает несколько сразу."""
    for statement in path.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            client.command(statement)


def hdfs_parquet(url: str) -> str:
    return f"hdfs('{url}/*.parquet', 'Parquet')"


def _fill_stage(client: Client, table: str, source: str) -> str:
    """Копирует source в пустую staging-таблицу той же структуры, что table."""
    stage = f"{table}_stage"
    columns = ", ".join(
        client.query(
            "SELECT name FROM system.columns"
            " WHERE database = currentDatabase() AND table = {t:String} ORDER BY position",
            parameters={"t": table},
        ).result_columns[0]
    )
    client.command(f"DROP TABLE IF EXISTS {stage}")
    client.command(f"CREATE TABLE {stage} AS {table}")
    # Колонки сопоставляются по имени, а не по порядку в источнике
    client.command(f"INSERT INTO {stage} ({columns}) SELECT {columns} FROM {source}")
    return stage


def load_snapshot(client: Client, table: str, snapshot_date: str, source: str) -> int:
    """Подменяет снимок за snapshot_date строками из source.

    Строки грузятся в staging-таблицу и проверяются, затем день подменяется одной командой
    REPLACE PARTITION: читатели видят либо прежний снимок дня, либо новый.
    """
    day = date.fromisoformat(snapshot_date)
    try:
        stage = _fill_stage(client, table, source)
        rows, days = client.query(
            f"SELECT count(), groupUniqArray(snapshot_date) FROM {stage}"
        ).result_rows[0]
        if not rows:
            raise ValueError(f"{source}: нет строк")
        if days != [day]:
            raise ValueError(f"{source}: снимки {days}, ожидался {day}")
        client.command(f"ALTER TABLE {table} REPLACE PARTITION '{day}' FROM {stage}")
    finally:
        client.command(f"DROP TABLE IF EXISTS {table}_stage")
    log.info("%s: снимок %s, %d строк", table, day, rows)
    return rows


def load_days(client: Client, table: str, source: str) -> dict[date, int]:
    """Подменяет каждый день (партицию local_date), который есть в source.

    Каждая партиция подменяется атомарно; дни, которых нет в source, не трогаются.
    """
    try:
        stage = _fill_stage(client, table, source)
        counts = dict(
            client.query(
                f"SELECT local_date, count() FROM {stage} GROUP BY local_date ORDER BY local_date"
            ).result_rows
        )
        if not counts:
            raise ValueError(f"{source}: нет строк")
        for day in counts:
            client.command(f"ALTER TABLE {table} REPLACE PARTITION '{day}' FROM {stage}")
    finally:
        client.command(f"DROP TABLE IF EXISTS {table}_stage")
    log.info("%s: дни %s", table, {str(d): n for d, n in counts.items()})
    return counts
