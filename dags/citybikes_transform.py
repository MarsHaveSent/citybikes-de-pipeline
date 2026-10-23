"""Spark: Postgres (bronze) -> silver -> gold в HDFS -> ClickHouse.

- silver: очищенные сети и станции, /citybikes/silver/{networks,stations}
- gold: One Big Table станций с атрибутами сетей, /citybikes/gold/stations_obt,
  ежедневный снимок в партиции snapshot_date
- ClickHouse: тот же снимок в citybikes.stations_obt, день подменяется атомарно
- dq_gold: проверки снимка в ClickHouse, результаты в ops_dq_results
"""

from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import clickhouse_connect
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Connection

from citybikes import clickhouse, dq
from citybikes.monitoring import CALLBACKS
from citybikes.settings import CLICKHOUSE_CONN_ID, OBT_EXPORT_URL, POSTGRES_CONN_ID
from citybikes.spark_env import POSTGRES_ENV

SPARK_JOBS = "/opt/airflow/spark_jobs"
CLICKHOUSE_DDL = Path("/opt/airflow/sql/clickhouse/create_tables.sql")
SNAPSHOT_DATE = "{{ dag_run.run_after | ds }}"


def clickhouse_client():
    conn = Connection.get(CLICKHOUSE_CONN_ID)
    params = {
        "host": conn.host,
        "port": conn.port,
        "username": conn.login,
        "password": conn.password,
    }
    # Образ ClickHouse создаёт базу из CLICKHOUSE_DB только при первом старте на пустом томе
    with closing(clickhouse_connect.get_client(**params)) as admin:
        admin.command(f"CREATE DATABASE IF NOT EXISTS {conn.schema}")
    return clickhouse_connect.get_client(**params, database=conn.schema)


def load_clickhouse(snapshot_date: str) -> dict[str, int]:
    with closing(clickhouse_client()) as client:
        clickhouse.run_script(client, CLICKHOUSE_DDL)
        rows = clickhouse.load_snapshot(
            client, "stations_obt", snapshot_date, clickhouse.hdfs_parquet(OBT_EXPORT_URL)
        )
    return {"rows": rows}


def check_gold(snapshot_date: str, run_id: str) -> dict[str, int]:
    with closing(clickhouse_client()) as client:
        results = dq.run_checks(
            lambda sql, params: client.query(sql, parameters=params).result_rows[0][0],
            dq.GOLD_CHECKS,
            {"day": snapshot_date},
        )
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        dq.save_results(conn, results, datetime.now(UTC), run_id)
        conn.commit()
    return dq.finish(results)


dag = DAG(
    dag_id="citybikes_transform",
    description="Silver и gold слои в HDFS на Spark, загрузка витрины в ClickHouse",
    doc_md=__doc__,
    # Витрину читают аналитики: один снимок в сутки, внутри дня данные не меняются
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        **CALLBACKS,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["citybikes", "spark", "hdfs", "clickhouse"],
)

start = EmptyOperator(task_id="start", dag=dag)

build_silver = SparkSubmitOperator(
    task_id="build_silver",
    application=f"{SPARK_JOBS}/silver.py",
    name="citybikes_silver",
    env_vars=POSTGRES_ENV,
    dag=dag,
)

build_obt = SparkSubmitOperator(
    task_id="build_obt",
    application=f"{SPARK_JOBS}/stations_obt.py",
    name="citybikes_stations_obt",
    # В Airflow 3 у ручного запуска нет logical_date, и ds не определён
    application_args=[SNAPSHOT_DATE],
    dag=dag,
)

load_obt_to_clickhouse = PythonOperator(
    task_id="load_clickhouse",
    python_callable=load_clickhouse,
    op_kwargs={"snapshot_date": SNAPSHOT_DATE},
    dag=dag,
)

dq_gold = PythonOperator(
    task_id="dq_gold",
    python_callable=check_gold,
    # Повтор через 5 минут даст тот же результат и второй алерт
    retries=0,
    op_kwargs={"snapshot_date": SNAPSHOT_DATE, "run_id": "{{ run_id }}"},
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> build_silver >> build_obt >> load_obt_to_clickhouse >> dq_gold >> end
