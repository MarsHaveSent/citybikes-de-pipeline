"""Почасовая погода Open-Meteo для опрашиваемых сетей -> weather_hourly."""

import logging
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from citybikes import db
from citybikes.monitoring import CALLBACKS
from citybikes.settings import POLLED_NETWORKS, POSTGRES_CONN_ID
from citybikes.weather import fetch_weather, weather_rows

log = logging.getLogger(__name__)


def load_weather() -> dict[str, int]:
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        locations = db.select_network_locations(conn, POLLED_NETWORKS)
        missing = set(POLLED_NETWORKS) - {network_id for network_id, _, _ in locations}
        if missing:
            log.warning("Нет координат для сетей: %s", ", ".join(sorted(missing)))
        rows = weather_rows(locations, fetch_weather(locations), datetime.now(UTC))
        count = db.upsert_weather(conn, rows)
        conn.commit()
    log.info("Записано часов погоды: %d по %d сетям", count, len(locations))
    return {"hours": count, "networks": len(locations)}


dag = DAG(
    dag_id="citybikes_weather",
    description="Почасовая погода Open-Meteo в городах опрашиваемых сетей",
    doc_md=__doc__,
    # 5 минут после начала часа: значения за прошедший час уже посчитаны
    schedule="5 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        **CALLBACKS,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath="/opt/airflow/sql",
    tags=["citybikes", "postgres", "weather"],
)

start = EmptyOperator(task_id="start", dag=dag)

create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id=POSTGRES_CONN_ID,
    sql="postgres/create_tables.sql",
    dag=dag,
)

load_weather_task = PythonOperator(
    task_id="load_weather",
    python_callable=load_weather,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> create_tables >> load_weather_task >> end
