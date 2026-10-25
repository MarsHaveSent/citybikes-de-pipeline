"""Справочники GeoNames -> geo_countries, geo_cities.

Население стран и города от 15 тыс. жителей для витрин мирового рынка и точек роста.
Данные GeoNames, лицензия CC BY 4.0.
"""

import logging
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from citybikes import db, geonames
from citybikes.monitoring import CALLBACKS
from citybikes.settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)


def load_geonames() -> dict[str, int]:
    countries = geonames.parse_countries(geonames.download(geonames.COUNTRIES_URL).decode())
    cities = geonames.read_cities_zip(geonames.download(geonames.CITIES_URL))
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        db.replace_geonames(conn, countries, cities, datetime.now(UTC))
        conn.commit()
    log.info("Загружено стран: %d, городов: %d", len(countries), len(cities))
    return {"countries": len(countries), "cities": len(cities)}


dag = DAG(
    dag_id="citybikes_reference",
    description="Справочники GeoNames: население стран и крупные города",
    doc_md=__doc__,
    # Население в GeoNames обновляется редко
    schedule="0 3 1 * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        **CALLBACKS,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    template_searchpath="/opt/airflow/sql",
    tags=["citybikes", "postgres", "reference"],
)

start = EmptyOperator(task_id="start", dag=dag)

create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id=POSTGRES_CONN_ID,
    sql="postgres/create_tables.sql",
    dag=dag,
)

load_geonames_task = PythonOperator(
    task_id="load_geonames",
    python_callable=load_geonames,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> create_tables >> load_geonames_task >> end
