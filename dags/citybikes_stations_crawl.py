"""Станции CityBikes -> stations.

Каждый прогон обновляет сети, которые синхронизировались давнее всего.
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
from citybikes.crawl import crawl_stations
from citybikes.settings import CRAWL_BATCH_SIZE, POSTGRES_CONN_ID, RATE_LIMIT_RESERVE


def crawl() -> dict[str, Any]:
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        stats = crawl_stations(CityBikesClient(), conn, CRAWL_BATCH_SIZE, RATE_LIMIT_RESERVE)
    return vars(stats)


dag = DAG(
    dag_id="citybikes_stations_crawl",
    description="Ежечасный инкрементальный обход станций CityBikes API",
    doc_md=__doc__,
    schedule="@hourly",
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

crawl_task = PythonOperator(
    task_id="crawl",
    python_callable=crawl,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> create_tables >> crawl_task >> end
