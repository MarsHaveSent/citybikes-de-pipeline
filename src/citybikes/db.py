"""Запись в Postgres-источник: сети, станции, лог изменений станций, реестр полей."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

log = logging.getLogger(__name__)

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
    register_fields(conn, "network", {k for n in networks for k in n}, seen_at)
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


def select_network_locations(
    conn: Connection, network_ids: Sequence[str]
) -> list[tuple[str, float, float]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, (location ->> 'latitude')::float, (location ->> 'longitude')::float
            FROM networks
            WHERE id = ANY(%s) AND location ? 'latitude' AND location ? 'longitude'
            ORDER BY id
            """,
            (list(network_ids),),
        )
        return cur.fetchall()


def upsert_weather(conn: Connection, rows: list[tuple]) -> int:
    sql = _upsert_sql(
        "weather_hourly",
        (
            "network_id",
            "hour",
            "temperature",
            "apparent_temperature",
            "precipitation",
            "wind_speed",
            "weather_code",
            "fetched_at",
        ),
        key=("network_id", "hour"),
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def upsert_network_timezones(conn: Connection, rows: list[tuple]) -> int:
    sql = _upsert_sql(
        "network_timezones", ("network_id", "timezone", "updated_at"), key=("network_id",)
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


@dataclass
class StationsSyncResult:
    stations: int
    changes: int


_STAGE_COLUMNS = ", ".join(f'"{c}"' for c in STATION_COLUMNS)

# Изменением считается новое значение хотя бы одного счётчика; новая станция тоже пишется в лог,
# это начальное состояние её истории
_LOG_CHANGES_SQL = """
    INSERT INTO station_status_log
        (network_id, station_id, observed_at, "timestamp", free_bikes, empty_slots, ebikes)
    SELECT st.network_id, st.station_id, st.synced_at, st."timestamp",
           st.free_bikes, st.empty_slots, ebikes_count(st.extra)
    FROM stations_stage st
    LEFT JOIN stations s USING (network_id, station_id)
    WHERE s.station_id IS NULL
       OR (st.free_bikes, st.empty_slots, ebikes_count(st.extra))
          IS DISTINCT FROM (s.free_bikes, s.empty_slots, ebikes_count(s.extra))
    ON CONFLICT DO NOTHING
"""

_UPSERT_FROM_STAGE_SQL = f"""
    INSERT INTO stations ({_STAGE_COLUMNS})
    SELECT {_STAGE_COLUMNS} FROM stations_stage
    ON CONFLICT (network_id, station_id) DO UPDATE SET
        {", ".join(f'"{c}" = EXCLUDED."{c}"' for c in STATION_COLUMNS[2:])}
"""


def sync_stations(
    conn: Connection, network_id: str, stations: list[dict[str, Any]], synced_at: datetime
) -> StationsSyncResult:
    """Приводит станции сети к ответу API и пишет изменения в station_status_log."""
    rows = station_rows(network_id, stations, synced_at)
    register_fields(conn, "station", {k for s in stations for k in s}, synced_at)
    register_fields(
        conn, "station_extra", {k for s in stations for k in s.get("extra") or {}}, synced_at
    )

    with conn.cursor() as cur:
        # Временная таблица живёт до конца соединения, поэтому её чистят перед каждой сетью
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS stations_stage (LIKE stations)")
        cur.execute("TRUNCATE stations_stage")
        with cur.copy(f"COPY stations_stage ({_STAGE_COLUMNS}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)

        # Сравнение с текущим состоянием должно идти до обновления
        cur.execute(_LOG_CHANGES_SQL)
        changes = cur.rowcount
        cur.execute(_UPSERT_FROM_STAGE_SQL)
        # Таблица хранит текущее состояние
        cur.execute(
            """
            DELETE FROM stations s
            WHERE s.network_id = %s
              AND NOT EXISTS (SELECT 1 FROM stations_stage st WHERE st.station_id = s.station_id)
            """,
            (network_id,),
        )
        cur.execute(
            "UPDATE networks SET stations_synced_at = %s WHERE id = %s",
            (synced_at, network_id),
        )
        cur.execute(
            "INSERT INTO network_syncs (network_id, synced_at, stations, changes)"
            " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (network_id, synced_at, len(rows), changes),
        )
    return StationsSyncResult(stations=len(rows), changes=changes)


def register_fields(
    conn: Connection, entity: str, fields: set[str], seen_at: datetime
) -> list[str]:
    """Отмечает поля, встреченные в ответе API, и возвращает те, которых раньше не было."""
    if not fields:
        return []
    with conn.cursor() as cur:
        cur.execute("SELECT field FROM schema_fields WHERE entity = %s", (entity,))
        known = {row[0] for row in cur.fetchall()}
        cur.executemany(
            """
            INSERT INTO schema_fields (entity, field, first_seen_at, last_seen_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (entity, field) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
            """,
            [(entity, f, seen_at, seen_at) for f in sorted(fields)],
        )
    new = sorted(fields - known)
    # При самой первой загрузке - не дрейф
    if new and known:
        log.warning("Новые поля в %s: %s", entity, ", ".join(new))
    return new
