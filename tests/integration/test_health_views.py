from datetime import UTC, datetime, timedelta

import pytest

from citybikes import db

T0 = datetime(2026, 10, 20, 8, tzinfo=UTC)


def fetch(conn, sql):
    return conn.execute(sql).fetchall()


def add_sync(conn, network_id, synced_at, feed_updated_at, stations=10, changes=1):
    conn.execute(
        "INSERT INTO network_syncs (network_id, synced_at, stations, changes, feed_updated_at)"
        " VALUES (%s, %s, %s, %s, %s)",
        (network_id, synced_at, stations, changes, feed_updated_at),
    )


def test_network_days_count_only_fresh_slots_of_polled_networks(conn):
    db.upsert_networks(conn, [{"id": "polled", "name": "P"}, {"id": "crawled", "name": "C"}], T0)
    db.upsert_network_timezones(
        conn, [("polled", "Europe/Berlin", T0), ("crawled", "Europe/Berlin", T0)]
    )
    # 30 опросов раз в 10 минут; после 11:10 данные замерли: опросы с 11:40 устаревшие
    frozen_at = T0 + timedelta(minutes=190)
    for i in range(30):
        synced_at = T0 + timedelta(minutes=10 * i)
        add_sync(conn, "polled", synced_at, min(synced_at, frozen_at))
    # Краулер заходит несколько раз в сутки, такая сеть в представление не попадает
    for i in range(5):
        add_sync(conn, "crawled", T0 + timedelta(hours=i), None)

    rows = fetch(
        conn,
        "SELECT network_id, local_date::text, syncs, stale_syncs, observed_share"
        " FROM health_network_days",
    )

    assert len(rows) == 1
    network_id, local_date, syncs, stale, observed = rows[0]
    assert (network_id, local_date, syncs, stale) == ("polled", "2026-10-20", 30, 7)
    assert float(observed) == pytest.approx(23 / 144)


def test_task_runs_expose_only_error_class(conn):
    conn.execute(
        "INSERT INTO ops_task_runs (dag_id, task_id, run_id, try_number, state, finished_at,"
        " error) VALUES ('d', 't', 'manual__1', 1, 'failed', now(),"
        " 'psycopg.errors.InsufficientPrivilege: must be owner of table x')"
    )

    assert fetch(conn, "SELECT is_manual, error_type FROM health_task_runs") == [
        (True, "psycopg.errors.InsufficientPrivilege")
    ]
