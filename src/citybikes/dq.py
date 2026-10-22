"""Проверки качества данных: SQL-запрос возвращает одно число, оно сравнивается с порогами.

fail роняет задачу (и через колбэк уходит алерт), warn только записывается в ops_dq_results.
NULL означает, что проверку не с чем сравнить (например, нет вчерашнего снимка), и она проходит.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection

Query = Callable[[str, dict[str, Any]], Any]


@dataclass(frozen=True)
class Check:
    name: str
    layer: str
    sql: str
    warn_at: float | None = None
    fail_at: float | None = None
    # Для долей покрытия плохо, когда значение ниже порога
    lower_is_worse: bool = False


@dataclass(frozen=True)
class Result:
    check: Check
    value: float | None
    status: str


def evaluate(check: Check, value: float | None) -> str:
    if value is None:
        return "ok"

    def breached(threshold: float | None) -> bool:
        if threshold is None:
            return False
        return value < threshold if check.lower_is_worse else value > threshold

    if breached(check.fail_at):
        return "fail"
    if breached(check.warn_at):
        return "warn"
    return "ok"


def run_checks(query: Query, checks: Sequence[Check], params: dict[str, Any]) -> list[Result]:
    results = []
    for check in checks:
        raw = query(check.sql, params)
        value = float(raw) if raw is not None else None
        results.append(Result(check, value, evaluate(check, value)))
    return results


def save_results(
    conn: Connection, results: Sequence[Result], checked_at: datetime, run_id: str | None
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO ops_dq_results
                (checked_at, layer, check_name, value, warn_at, fail_at, status, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (check_name, checked_at) DO NOTHING
            """,
            [
                (
                    checked_at,
                    r.check.layer,
                    r.check.name,
                    r.value,
                    r.check.warn_at,
                    r.check.fail_at,
                    r.status,
                    run_id,
                )
                for r in results
            ],
        )


class DataQualityError(Exception):
    pass


def finish(results: Sequence[Result]) -> dict[str, int]:
    """Счётчики по статусам для журнала; при fail-проверках задача падает с их списком."""
    counts = {s: sum(r.status == s for r in results) for s in ("ok", "warn", "fail")}
    if counts["fail"]:
        raise DataQualityError("Не прошли проверки:\n" + summary(results, "fail"))
    return counts


def summary(results: Sequence[Result], status: str) -> str:
    lines = []
    for r in results:
        if r.status == status:
            threshold = r.check.fail_at if status == "fail" else r.check.warn_at
            lines.append(f"{r.check.name}: {r.value:g} (порог {threshold:g})")
    return "\n".join(lines)


# Bronze (Postgres). Параметр polled - сети частого опроса
BRONZE_CHECKS = [
    Check(
        "bronze_polled_lag_minutes",
        "bronze",
        """
        SELECT max(extract(epoch FROM now() - coalesce(stations_synced_at, '-infinity'))) / 60
        FROM networks WHERE id = ANY(%(polled)s)
        """,
        warn_at=30,
        fail_at=120,
    ),
    Check(
        "bronze_crawl_coverage_24h",
        "bronze",
        # coalesce: у ни разу не синхронизированной сети сравнение даёт NULL, а avg пропускает NULL
        """
        SELECT avg(coalesce(stations_synced_at > now() - interval '24 hours', false)::int)
        FROM networks
        """,
        warn_at=0.95,
        fail_at=0.8,
        lower_is_worse=True,
    ),
    Check(
        "bronze_catalog_lag_hours",
        "bronze",
        "SELECT extract(epoch FROM now() - max(last_seen_at)) / 3600 FROM networks",
        warn_at=26,
        fail_at=50,
    ),
    Check(
        "bronze_weather_lag_hours",
        "bronze",
        "SELECT extract(epoch FROM now() - max(fetched_at)) / 3600 FROM weather_hourly",
        warn_at=2,
        fail_at=6,
    ),
    # Поля самой первой загрузки - не дрейф
    Check(
        "bronze_new_api_fields_24h",
        "bronze",
        """
        SELECT count(*) FROM schema_fields
        WHERE first_seen_at > now() - interval '24 hours'
          AND first_seen_at > (SELECT min(first_seen_at) FROM schema_fields)
        """,
        warn_at=0,
    ),
]

# Gold (ClickHouse). Параметр day - дата снимка, сравнение с предыдущим снимком
_PREV_DAY = "(SELECT max(snapshot_date) FROM stations_obt WHERE snapshot_date < {day:Date})"
GOLD_CHECKS = [
    Check(
        "gold_snapshot_rows",
        "gold",
        "SELECT count() FROM stations_obt WHERE snapshot_date = {day:Date}",
        fail_at=1,
        lower_is_worse=True,
    ),
    Check(
        "gold_rows_change",
        "gold",
        f"""
        WITH (SELECT count() FROM stations_obt WHERE snapshot_date = {{day:Date}}) AS cur,
             (SELECT count() FROM stations_obt WHERE snapshot_date = {_PREV_DAY}) AS prev
        SELECT if(prev = 0, NULL, abs(cur - prev) / prev)
        """,
        warn_at=0.05,
        fail_at=0.2,
    ),
    Check(
        "gold_networks_change",
        "gold",
        f"""
        WITH (SELECT uniqExact(network_id) FROM stations_obt
              WHERE snapshot_date = {{day:Date}}) AS cur,
             (SELECT uniqExact(network_id) FROM stations_obt
              WHERE snapshot_date = {_PREV_DAY}) AS prev
        SELECT if(prev = 0, NULL, abs(cur - prev) / prev)
        """,
        warn_at=0.02,
        fail_at=0.1,
    ),
    Check(
        "gold_networks_without_stations_share",
        "gold",
        """
        SELECT countIf(station_id = '') / uniqExact(network_id)
        FROM stations_obt WHERE snapshot_date = {day:Date}
        """,
        warn_at=0.03,
        fail_at=0.1,
    ),
    Check(
        "gold_duplicate_stations",
        "gold",
        """
        SELECT count() - uniqExact(network_id, station_id)
        FROM stations_obt WHERE snapshot_date = {day:Date}
        """,
        fail_at=0,
    ),
    Check(
        "gold_null_timestamp_share",
        "gold",
        """
        SELECT countIf(station_timestamp IS NULL) / count()
        FROM stations_obt WHERE snapshot_date = {day:Date} AND station_id != ''
        """,
        warn_at=0.01,
        fail_at=0.05,
    ),
]
