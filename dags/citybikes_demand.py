"""Спрос по станциям: история изменений в silver и витрины спроса.

- build_station_status: новые строки station_status_log -> /citybikes/silver/station_status
  (Delta, MERGE без дублей при перекрытии)
- prune_status_log: удаление из Postgres строк лога старше 30 дней, они уже в silver
"""

from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from citybikes.monitoring import CALLBACKS
from citybikes.settings import POSTGRES_CONN_ID
from citybikes.spark_env import POSTGRES_ENV

SPARK_JOBS = "/opt/airflow/spark_jobs"

dag = DAG(
    dag_id="citybikes_demand",
    description="История станций в silver и витрины спроса",
    doc_md=__doc__,
    # К 08:00 UTC закончились местные сутки во всех часовых поясах опрашиваемых сетей
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        **CALLBACKS,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath="/opt/airflow/sql",
    tags=["citybikes", "spark", "delta"],
)

start = EmptyOperator(task_id="start", dag=dag)

build_station_status = SparkSubmitOperator(
    task_id="build_station_status",
    application=f"{SPARK_JOBS}/station_status.py",
    name="citybikes_station_status",
    env_vars=POSTGRES_ENV,
    dag=dag,
)

prune_status_log = SQLExecuteQueryOperator(
    task_id="prune_status_log",
    conn_id=POSTGRES_CONN_ID,
    sql="postgres/prune_status_log.sql",
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> build_station_status >> prune_status_log >> end
