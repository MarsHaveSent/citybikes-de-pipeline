"""Spark: Postgres (bronze) -> silver -> gold в HDFS.

- silver: очищенные сети и станции, /citybikes/silver/{networks,stations}
- gold: One Big Table станций с атрибутами сетей, /citybikes/gold/stations_obt,
  ежедневный снимок в партиции snapshot_date
"""

from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from citybikes.settings import POSTGRES_CONN_ID

SPARK_JOBS = "/opt/airflow/spark_jobs"


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

dag = DAG(
    dag_id="citybikes_transform",
    description="Silver и gold слои в HDFS на Spark",
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
    tags=["citybikes", "spark", "hdfs"],
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
    # Дата запуска; перезапуск за ту же дату перезаписывает только свой снимок
    application_args=["{{ ds }}"],
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> build_silver >> build_obt >> end
