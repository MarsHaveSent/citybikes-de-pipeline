"""Локальный Spark для тестов трансформаций: без кластера, в одном процессе."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Python-воркеры Spark запускаются командой python из PATH; на Windows (для локальных тестов)
# это может быть другой интерпретатор и воркер не подключается к JVM
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

# Типы колонок такие же, какие Spark получает из Postgres по JDBC: jsonb приходит строкой
NETWORKS_SCHEMA = (
    "id string, name string, location string, company array<string>, system string, "
    "gbfs_href string, ebikes boolean, scooters boolean, first_seen_at timestamp, "
    "last_seen_at timestamp, stations_synced_at timestamp"
)
STATIONS_SCHEMA = (
    "network_id string, station_id string, name string, latitude double, longitude double, "
    "timestamp string, free_bikes int, empty_slots int, extra string, synced_at timestamp"
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("citybikes-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _factory(spark, schema):
    columns = [c.split()[0] for c in schema.split(", ")]

    def make(*rows):
        """Строки - словари с нужными полями, остальные колонки заполняются NULL."""
        return spark.createDataFrame([dict.fromkeys(columns) | row for row in rows], schema)

    return make


@pytest.fixture
def networks_df(spark):
    return _factory(spark, NETWORKS_SCHEMA)


@pytest.fixture
def stations_df(spark):
    return _factory(spark, STATIONS_SCHEMA)
