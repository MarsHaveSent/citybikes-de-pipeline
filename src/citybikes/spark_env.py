"""Переменные окружения для Spark-джоб, которые читают Postgres по JDBC.

Значения - шаблоны Airflow из подключения: драйвер Spark работает в контейнере Airflow
и сам передаёт параметры executor'ам. Пароль из подключения Airflow маскирует в логах.
"""

from citybikes.settings import POSTGRES_CONN_ID


def _field(field: str) -> str:
    return "{{ conn." + POSTGRES_CONN_ID + "." + field + " }}"


POSTGRES_ENV = {
    "CITYBIKES_DB_HOST": _field("host"),
    "CITYBIKES_DB_PORT": _field("port"),
    "CITYBIKES_DB_NAME": _field("schema"),
    "CITYBIKES_DB_USER": _field("login"),
    "CITYBIKES_DB_PASSWORD": _field("password"),
}
