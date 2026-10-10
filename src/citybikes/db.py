"""Запись в Postgres-источник: таблицы networks и stations."""

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

NETWORK_COLUMNS = (
    "id",
    "name",
    "href",
    "location",
    "company",
    "gbfs_href",
    "system",
    "source",
    "license",
    "ebikes",
    "scooters",
    "instances",
)
JSON_COLUMNS = {"location", "license", "instances"}

STATION_COLUMNS = (
    "network_id",
    "station_id",
    "name",
    "latitude",
    "longitude",
    "timestamp",
    "free_bikes",
    "empty_slots",
    "extra",
    "synced_at",
)


def _jsonb(value: Any) -> Jsonb | None:
    return Jsonb(value) if value is not None else None


def _upsert_sql(
    table: str, columns: Sequence[str], key: Sequence[str], keep: Sequence[str] = ()
) -> str:
    """INSERT ... ON CONFLICT DO UPDATE; колонки из keep при обновлении не перезаписываются."""
    names = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in key and c not in keep
    )
    return (
        f"INSERT INTO {table} ({names}) VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}"
    )


def network_row(network: dict[str, Any], seen_at: datetime) -> tuple:
    values = [
        _jsonb(network.get(col)) if col in JSON_COLUMNS else network.get(col)
        for col in NETWORK_COLUMNS
    ]
    return (*values, seen_at, seen_at)


def station_rows(
    network_id: str, stations: list[dict[str, Any]], synced_at: datetime
) -> list[tuple]:
    by_id = {s["id"]: s for s in stations}
    return [
        (
            network_id,
            s["id"],
            s.get("name"),
            s.get("latitude"),
            s.get("longitude"),
            s.get("timestamp"),
            s.get("free_bikes"),
            s.get("empty_slots"),
            _jsonb(s.get("extra")),
            synced_at,
        )
        for s in by_id.values()
    ]


def upsert_networks(conn: Connection, networks: list[dict[str, Any]], seen_at: datetime) -> int:
    sql = _upsert_sql(
        "networks",
        (*NETWORK_COLUMNS, "first_seen_at", "last_seen_at"),
        key=("id",),
        keep=("first_seen_at",),
    )
    rows = [network_row(n, seen_at) for n in networks]
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def select_networks_to_sync(conn: Connection, limit: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM networks
            ORDER BY stations_synced_at NULLS FIRST, id
            LIMIT %s
            """,
            (limit,),
        )
        return [row[0] for row in cur.fetchall()]


def replace_stations(
    conn: Connection, network_id: str, stations: list[dict[str, Any]], synced_at: datetime
) -> int:
    """Приводит станции сети к состоянию из ответа API и отмечает время синхронизации."""
    rows = station_rows(network_id, stations, synced_at)
    with conn.cursor() as cur:
        if rows:
            sql = _upsert_sql("stations", STATION_COLUMNS, key=("network_id", "station_id"))
            cur.executemany(sql, rows)
        # Таблица хранит текущее состояние: станции, пропавшие из ответа, удаляем
        cur.execute(
            """
            DELETE FROM stations
            WHERE network_id = %s AND NOT station_id = ANY(%s::text[])
            """,
            (network_id, [r[1] for r in rows]),
        )
        cur.execute(
            "UPDATE networks SET stations_synced_at = %s WHERE id = %s",
            (synced_at, network_id),
        )
    return len(rows)
