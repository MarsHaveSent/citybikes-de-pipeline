"""Частый опрос выбранных сетей -> stations и station_status_log.

Список сетей - POLLED_NETWORKS в citybikes.settings.
"""

from contextlib import closing
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from citybikes.api import CityBikesClient
from citybikes.settings import POLLED_NETWORKS, POSTGRES_CONN_ID
from citybikes.sync import sync_networks


def poll() -> dict[str, Any]:
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        stats = sync_networks(CityBikesClient(), conn, POLLED_NETWORKS)
    return vars(stats)


dag = DAG(
    dag_id="citybikes_stations_poll",
    description="Опрос выбранных сетей каждые 10 минут для истории спроса",
    doc_md=__doc__,
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    template_searchpath="/opt/airflow/sql",
    tags=["citybikes", "postgres"],
)

start = EmptyOperator(task_id="start", dag=dag)

create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id=POSTGRES_CONN_ID,
    sql="postgres/create_tables.sql",
    dag=dag,
)

poll_task = PythonOperator(
    task_id="poll",
    python_callable=poll,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> create_tables >> poll_task >> end
