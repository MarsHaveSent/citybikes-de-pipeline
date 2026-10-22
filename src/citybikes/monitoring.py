"""Колбэки задач Airflow: журнал ops_task_runs и алерты на почту.

Подключаются ко всем DAG'ам через default_args=CALLBACKS. Ошибка внутри колбэка только
пишется в лог: мониторинг не должен ронять задачу, которая уже отработала.

Письмо уходит, когда задача сломалась и когда снова заработала. Пока она продолжает падать,
повтор не чаще раза в REALERT_AFTER: опрос раз в 10 минут иначе слал бы письма всю ночь.
"""

import functools
import logging
import traceback
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg.types.json import Jsonb

from citybikes import alerts
from citybikes.settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

REALERT_AFTER = timedelta(hours=24)

_LAST_FINAL_STATE = """
    SELECT state, finished_at FROM ops_task_runs
    WHERE dag_id = %s AND task_id = %s AND state IN ('success', 'failed')
    ORDER BY finished_at DESC LIMIT 1
"""

_UPSERT_TASK_RUN = """
    INSERT INTO ops_task_runs
        (dag_id, task_id, run_id, try_number, state, started_at, finished_at, duration_s,
         metrics, error)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (dag_id, task_id, run_id, try_number) DO UPDATE SET
        state = EXCLUDED.state, finished_at = EXCLUDED.finished_at,
        duration_s = EXCLUDED.duration_s, metrics = EXCLUDED.metrics, error = EXCLUDED.error
"""


def task_run_row(
    ti: Any, state: str, result: Any, error: str | None, finished_at: datetime
) -> tuple:
    started_at = ti.start_date
    duration = (finished_at - started_at).total_seconds() if started_at else None
    # Метрики - словарь, который задача вернула; SQL-операторы возвращают строки запроса
    metrics = result if isinstance(result, dict) else None
    return (
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
        state,
        started_at,
        finished_at,
        duration,
        Jsonb(metrics) if metrics is not None else None,
        error,
    )


def format_error(exception: BaseException | None) -> str | None:
    if exception is None:
        return None
    return "".join(traceback.format_exception_only(exception)).strip()


def _record(row: tuple) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(_UPSERT_TASK_RUN, row)
        conn.commit()


def _callback(func):
    @functools.wraps(func)
    def wrapper(context: dict[str, Any]) -> None:
        # start/end - EmptyOperator - шум
        if type(context["ti"].task).__name__ == "EmptyOperator":
            return
        try:
            func(context)
        except Exception:
            log.exception("Колбэк %s не отработал", func.__name__)

    return wrapper


def should_alert(last: tuple[str, datetime] | None, now: datetime) -> bool:
    """Алерт о падении, если до этого задача не падала или о падении писали давно."""
    return not (last and last[0] == "failed" and now - last[1] < REALERT_AFTER)


def _last_final_state(dag_id: str, task_id: str) -> tuple[str, datetime] | None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        return conn.execute(_LAST_FINAL_STATE, (dag_id, task_id)).fetchone()


def _safely(func, *args):
    """Шаги колбэка независимы: недоступный Postgres - частая причина падений, и письмо
    о падении должно уйти, даже если журнал записать не удалось."""
    try:
        return func(*args)
    except Exception:
        log.exception("%s не отработал", func.__name__)
        return None


@_callback
def on_success(context: dict[str, Any]) -> None:
    ti = context["ti"]
    last = _safely(_last_final_state, ti.dag_id, ti.task_id)
    if last and last[0] == "failed":
        _safely(
            alerts.send_alert,
            f"{ti.dag_id}.{ti.task_id} снова работает",
            f"DAG: {ti.dag_id}\nЗадача: {ti.task_id}\nЗапуск: {ti.run_id}",
        )
    result = ti.xcom_pull(task_ids=ti.task_id)
    _safely(_record, task_run_row(ti, "success", result, None, datetime.now(UTC)))


@_callback
def on_retry(context: dict[str, Any]) -> None:
    error = format_error(context.get("exception"))
    _record(task_run_row(context["ti"], "up_for_retry", None, error, datetime.now(UTC)))


@_callback
def on_failure(context: dict[str, Any]) -> None:
    ti = context["ti"]
    now = datetime.now(UTC)
    error = format_error(context.get("exception"))
    # Без журнала (Postgres недоступен) повторность не проверить, и письмо уходит
    last = _safely(_last_final_state, ti.dag_id, ti.task_id)
    if should_alert(last, now):
        _safely(
            alerts.send_alert,
            f"{ti.dag_id}.{ti.task_id} упала",
            f"DAG: {ti.dag_id}\nЗадача: {ti.task_id}\nЗапуск: {ti.run_id}\n"
            f"Попытка: {ti.try_number}\n\n{error or 'без текста ошибки'}",
        )
    else:
        log.info("Задача падает повторно, алерт уже отправлен в %s", last[1])
    _safely(_record, task_run_row(ti, "failed", None, error, now))


CALLBACKS = {
    "on_success_callback": on_success,
    "on_retry_callback": on_retry,
    "on_failure_callback": on_failure,
}
