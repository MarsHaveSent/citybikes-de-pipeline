"""Тесты на Postgres. Без TEST_DATABASE_URL пропускаются.

Каждый тест получает свою схему: таблицы создаются create_tables.sql,
после теста схема удаляется. Тесты можно запускать на любой базе не задевая данные.
"""

import os
import uuid
from pathlib import Path

import psycopg
import pytest

DSN = os.environ.get("TEST_DATABASE_URL")
DDL = Path(__file__).parents[2] / "sql" / "postgres" / "create_tables.sql"


@pytest.fixture
def schema():
    if not DSN:
        pytest.skip("TEST_DATABASE_URL не задан")
    name = f"test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {name}")
    try:
        yield name
    finally:
        with psycopg.connect(DSN, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {name} CASCADE")


@pytest.fixture
def run_ddl(schema):
    """Выполняет create_tables.sql отдельным соединением, как задача create_tables в DAG'е."""

    def run():
        with psycopg.connect(DSN, options=f"-c search_path={schema}") as connection:
            connection.execute(DDL.read_text(encoding="utf-8"))

    return run


@pytest.fixture
def conn(schema, run_ddl):
    run_ddl()
    with psycopg.connect(DSN, options=f"-c search_path={schema}") as connection:
        yield connection
