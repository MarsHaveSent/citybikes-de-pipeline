"""Каталог сетей CityBikes -> networks."""

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
from citybikes.api import CityBikesClient
from citybikes.settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)


def load_networks() -> int:
    networks = CityBikesClient().get_networks()
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        count = db.upsert_networks(conn, networks, datetime.now(UTC))
        conn.commit()
    log.info("Загружено сетей: %d", count)
    return count


dag = DAG(
    dag_id="citybikes_networks",
    description="Ежедневная загрузка каталога сетей из CityBikes API",
    doc_md=__doc__,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
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

load_networks_task = PythonOperator(
    task_id="load_networks",
    python_callable=load_networks,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> create_tables >> load_networks_task >> end
