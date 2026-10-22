from datetime import UTC, datetime, timedelta
from pathlib import Path

from citybikes import monitoring

NOW = datetime.now(UTC)
CLEANUP = Path(__file__).parents[2] / "sql" / "postgres" / "cleanup_ops.sql"


def add_run(conn, run_id, state, finished_at, task_id="poll"):
    conn.execute(
        "INSERT INTO ops_task_runs (dag_id, task_id, run_id, try_number, state, finished_at)"
        " VALUES ('citybikes_stations_poll', %s, %s, 1, %s, %s)",
        (task_id, run_id, state, finished_at),
    )


def test_last_final_state_skips_retries_and_other_tasks(conn):
    add_run(conn, "r1", "success", NOW - timedelta(hours=3))
    add_run(conn, "r2", "failed", NOW - timedelta(hours=2))
    add_run(conn, "r3", "up_for_retry", NOW - timedelta(hours=1))
    add_run(conn, "r4", "success", NOW, task_id="create_tables")

    row = conn.execute(monitoring._LAST_FINAL_STATE, ("citybikes_stations_poll", "poll")).fetchone()

    assert row == ("failed", NOW - timedelta(hours=2))


def test_cleanup_removes_only_old_rows(conn):
    add_run(conn, "old", "success", NOW - timedelta(days=91))
    add_run(conn, "fresh", "success", NOW - timedelta(days=89))

    conn.execute(CLEANUP.read_text(encoding="utf-8"))

    assert conn.execute("SELECT run_id FROM ops_task_runs").fetchall() == [("fresh",)]
