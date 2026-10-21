"""Spark: Postgres (bronze) -> silver -> gold в HDFS -> ClickHouse.

- silver: очищенные сети и станции, /citybikes/silver/{networks,stations}
- gold: One Big Table станций с атрибутами сетей, /citybikes/gold/stations_obt,
  ежедневный снимок в партиции snapshot_date
- ClickHouse: тот же снимок в citybikes.stations_obt, день подменяется атомарно
"""

from contextlib import closing
from datetime import timedelta
from pathlib import Path

import clickhouse_connect
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Connection

from citybikes import clickhouse
from citybikes.settings import CLICKHOUSE_CONN_ID, OBT_EXPORT_URL, POSTGRES_CONN_ID

SPARK_JOBS = "/opt/airflow/spark_jobs"
CLICKHOUSE_DDL = Path("/opt/airflow/sql/clickhouse/create_tables.sql")
SNAPSHOT_DATE = "{{ dag_run.run_after | ds }}"


def conn_field(field: str) -> str:
    return "{{ conn." + POSTGRES_CONN_ID + "." + field + " }}"


# Драйвер Spark работает в этом контейнере и сам передаёт параметры подключения executor'ам.
# Пароль из подключения Airflow маскирует в логах и интерфейсе
DB_ENV = {
    "CITYBIKES_DB_HOST": conn_field("host"),
    "CITYBIKES_DB_PORT": conn_field("port"),
    "CITYBIKES_DB_NAME": conn_field("schema"),
    "CITYBIKES_DB_USER": conn_field("login"),
    "CITYBIKES_DB_PASSWORD": conn_field("password"),
}


def load_clickhouse(snapshot_date: str) -> int:
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
    client = clickhouse_connect.get_client(**params, database=conn.schema)
    try:
        clickhouse.run_script(client, CLICKHOUSE_DDL)
        return clickhouse.load_snapshot(
            client, "stations_obt", snapshot_date, clickhouse.hdfs_parquet(OBT_EXPORT_URL)
        )
    finally:
        client.close()


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
    env_vars=DB_ENV,
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

end = EmptyOperator(task_id="end", dag=dag)

start >> build_silver >> build_obt >> load_obt_to_clickhouse >> end
