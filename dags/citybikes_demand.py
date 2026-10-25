"""Спрос по станциям: история изменений в silver и витрины спроса.

- build_station_status: новые строки station_status_log -> /citybikes/silver/station_status
  (Delta, MERGE без дублей при перекрытии)
- prune_status_log: удаление из Postgres строк лога старше 30 дней, они уже в silver
- build_demand: витрины station_daily и network_hourly за последние местные сутки
- load_demand: загрузка пересчитанных дней в ClickHouse
- dq_demand: проверки витрин, в том числе «застывшие» данные сетей
"""

from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Connection

from citybikes import clickhouse, dq
from citybikes.monitoring import CALLBACKS
from citybikes.settings import CLICKHOUSE_CONN_ID, EXPORT_URL, POSTGRES_CONN_ID
from citybikes.spark_env import POSTGRES_ENV

SPARK_JOBS = "/opt/airflow/spark_jobs"
CLICKHOUSE_DDL = Path("/opt/airflow/sql/clickhouse/create_tables.sql")
DEMAND_TABLES = ("station_daily", "network_hourly")


def load_demand() -> dict[str, int]:
    with closing(clickhouse.connect(Connection.get(CLICKHOUSE_CONN_ID))) as client:
        clickhouse.run_script(client, CLICKHOUSE_DDL)
        loaded = {
            table: clickhouse.load_days(
                client, table, clickhouse.hdfs_parquet(f"{EXPORT_URL}/{table}")
            )
            for table in DEMAND_TABLES
        }
    return {table: sum(days.values()) for table, days in loaded.items()}


def check_demand(day: str, run_id: str) -> dict[str, int]:
    with closing(clickhouse.connect(Connection.get(CLICKHOUSE_CONN_ID))) as client:
        results = dq.run_checks(
            lambda sql, params: client.query(sql, parameters=params).result_rows[0][0],
            dq.DEMAND_CHECKS,
            {"day": day},
        )
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        dq.save_results(conn, results, datetime.now(UTC), run_id)
        conn.commit()
    return dq.finish(results)


dag = DAG(
    dag_id="citybikes_demand",
    description="История станций в silver и витрины спроса",
    doc_md=__doc__,
    # К 08:00 UTC закончились вчерашние местные сутки во всех часовых поясах опрашиваемых сетей
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
    tags=["citybikes", "spark", "delta", "clickhouse"],
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

build_demand = SparkSubmitOperator(
    task_id="build_demand",
    application=f"{SPARK_JOBS}/demand.py",
    name="citybikes_demand",
    env_vars=POSTGRES_ENV,
    dag=dag,
)

load_demand_task = PythonOperator(
    task_id="load_demand",
    python_callable=load_demand,
    dag=dag,
)

dq_demand = PythonOperator(
    task_id="dq_demand",
    python_callable=check_demand,
    op_kwargs={
        "day": "{{ (dag_run.run_after - macros.timedelta(days=1)) | ds }}",
        "run_id": "{{ run_id }}",
    },
    # Повтор через 5 минут даст тот же результат и второй алерт
    retries=0,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> build_station_status >> [prune_status_log, build_demand]
build_demand >> load_demand_task >> dq_demand
[prune_status_log, dq_demand] >> end
