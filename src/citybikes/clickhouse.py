"""Загрузка снимков витрины в ClickHouse."""

import logging
from datetime import date
from pathlib import Path

from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)


def run_script(client: Client, path: Path) -> None:
    """Выполняет SQL-файл по одной команде: clickhouse-connect не принимает несколько сразу."""
    for statement in path.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            client.command(statement)


def hdfs_parquet(url: str) -> str:
    return f"hdfs('{url}/*.parquet', 'Parquet')"


def load_snapshot(client: Client, table: str, snapshot_date: str, source: str) -> int:
    """Подменяет снимок за snapshot_date строками из source.

    Строки грузятся в staging-таблицу и проверяются, затем день подменяется одной командой
    REPLACE PARTITION: читатели видят либо прежний снимок дня, либо новый.
    """
    day = date.fromisoformat(snapshot_date)
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
    try:
        # Колонки сопоставляются по имени, а не по порядку в источнике
        client.command(f"INSERT INTO {stage} ({columns}) SELECT {columns} FROM {source}")
        rows, days = client.query(
            f"SELECT count(), groupUniqArray(snapshot_date) FROM {stage}"
        ).result_rows[0]
        if not rows:
            raise ValueError(f"{source}: нет строк")
        if days != [day]:
            raise ValueError(f"{source}: снимки {days}, ожидался {day}")
        client.command(f"ALTER TABLE {table} REPLACE PARTITION '{day}' FROM {stage}")
    finally:
        client.command(f"DROP TABLE IF EXISTS {stage}")
    log.info("%s: снимок %s, %d строк", table, day, rows)
    return rows
