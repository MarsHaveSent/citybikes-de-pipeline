"""Проверки качества bronze-слоя в Postgres -> ops_dq_results.

Свежесть опрашиваемых сетей, покрытие краулера, каталог сетей, погода, новые поля в API.
fail роняет задачу, и колбэк отправляет алерт; warn только записывается.
Заодно удаляет записи журналов мониторинга старше 90 дней.
"""

from contextlib import closing
from datetime import UTC, datetime

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from citybikes import dq
from citybikes.monitoring import CALLBACKS
from citybikes.settings import POLLED_NETWORKS, POSTGRES_CONN_ID


def check_bronze(run_id: str) -> dict[str, int]:
    with closing(PostgresHook(POSTGRES_CONN_ID).get_conn()) as conn:
        results = dq.run_checks(
            lambda sql, params: conn.execute(sql, params).fetchone()[0],
            dq.BRONZE_CHECKS,
            {"polled": list(POLLED_NETWORKS)},
        )
        dq.save_results(conn, results, datetime.now(UTC), run_id)
        conn.commit()
    return dq.finish(results)


dag = DAG(
    dag_id="citybikes_dq",
    description="Проверки качества bronze-слоя",
    doc_md=__doc__,
    # После краулера (:00) и погоды (:05)
    schedule="45 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        **CALLBACKS,
        "retries": 0,
    },
    template_searchpath="/opt/airflow/sql",
    tags=["citybikes", "dq"],
)

start = EmptyOperator(task_id="start", dag=dag)

create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id=POSTGRES_CONN_ID,
    sql="postgres/create_tables.sql",
    dag=dag,
)

cleanup_ops = SQLExecuteQueryOperator(
    task_id="cleanup_ops",
    conn_id=POSTGRES_CONN_ID,
    sql="postgres/cleanup_ops.sql",
    dag=dag,
)

dq_bronze = PythonOperator(
    task_id="dq_bronze",
    python_callable=check_bronze,
    op_kwargs={"run_id": "{{ run_id }}"},
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> create_tables >> cleanup_ops >> dq_bronze >> end
