from datetime import UTC, date, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from spark_jobs.demand import (
    coverage,
    finished_days,
    intervals,
    localize,
    network_hourly,
    station_daily,
    station_hours,
)
from spark_jobs.station_status import LOG_SCHEMA

START = datetime(2026, 10, 20, 0, 0, tzinfo=UTC)
END = datetime(2026, 10, 21, 0, 0, tzinfo=UTC)
SYNCS_SCHEMA = "network_id string, synced_at timestamp, feed_updated_at timestamp"


def at(hour, minute=0, day=20):
    return datetime(2026, 10, day, hour, minute, tzinfo=UTC)


@pytest.fixture
def timezones(spark):
    return spark.createDataFrame([("net", "Europe/Berlin")], "network_id string, timezone string")


@pytest.fixture
def hourly(spark, timezones):
    status = spark.createDataFrame(
        [
            # Состояние до начала периода
            ("net", "1", at(23, day=19), 5, 5, 0),
            ("net", "1", at(10), 3, 7, 0),
            ("net", "1", at(10, 30), 0, 10, 0),
            ("net", "1", at(11, 15), 4, 6, 0),
        ],
        LOG_SCHEMA,
    )
    return localize(station_hours(intervals(status, START, END)), timezones)


def test_station_hours_split_intervals_and_count_changes(hourly):
    rows = {r.local_hour: r for r in hourly.collect()}

    # 10:00-11:00 UTC = 12:00-13:00 в Берлине (летнее время, UTC+2)
    noon, one_pm = rows[12], rows[13]
    assert (noon.pickups, noon.returns, noon.changes) == (5, 0, 2)
    assert noon.empty_seconds == 1800
    assert (one_pm.returns, one_pm.empty_seconds) == (4, 900)
    assert sum(r.seconds for r in rows.values()) == 24 * 3600
    assert rows[2].pickups is None


def test_large_jumps_are_operator_moves_not_trips(spark, timezones):
    status = spark.createDataFrame(
        [
            ("net", "1", at(23, day=19), 2, 8, 0),
            # Машина привезла 6 велосипедов, потом один взяли и 5 увезли
            ("net", "1", at(8), 8, 2, 0),
            ("net", "1", at(9), 7, 3, 0),
            ("net", "1", at(10), 2, 8, 0),
        ],
        LOG_SCHEMA,
    )
    hourly = localize(station_hours(intervals(status, START, END)), timezones)
    syncs = spark.createDataFrame([], SYNCS_SCHEMA)

    days = {
        str(r.local_date): r for r in station_daily(hourly, coverage(syncs, timezones)).collect()
    }
    day = days["2026-10-20"]

    assert (day.pickups, day.returns, day.net_flow) == (1, 0, -1)
    assert (day.operator_removed, day.operator_added) == (5, 6)


def test_network_wide_bursts_are_not_trips(spark, timezones):
    # 30 станций: в 9:00 у 25 из них пропал велосипед разом (сбой ленты), в 10:00 одну взяли
    rows = [("net", str(i), at(23, day=19), 1, 5, 0) for i in range(30)]
    rows += [("net", str(i), at(9), 0, 6, 0) for i in range(25)]
    rows += [("net", "29", at(10), 0, 6, 0)]
    status = spark.createDataFrame(rows, LOG_SCHEMA)

    spans = intervals(status, START, END)

    assert spans.agg(F.sum("pickups")).first()[0] == 1
    assert spans.filter(F.col("change").isNotNull()).count() == 1


def test_station_daily_totals_and_time_weighted_average(spark, hourly, timezones):
    syncs = spark.createDataFrame(
        [("net", START + timedelta(minutes=10 * i), None) for i in range(72)],
        SYNCS_SCHEMA,
    )

    rows = station_daily(hourly, coverage(syncs, timezones)).collect()
    days = {str(r.local_date): r for r in rows}

    day = days["2026-10-20"]
    assert (day.pickups, day.returns, day.net_flow) == (5, 4, -1)
    assert day.empty_hours == pytest.approx(0.75)
    assert (day.min_free_bikes, day.max_free_bikes) == (0, 5)
    # Местные сутки 20.10 начались в 22:00 UTC 19.10; синхронизации - первые 12 часов UTC 20.10
    assert day.observed_share == pytest.approx(12 / 24)


def test_coverage_of_day_with_clock_change_is_complete(spark, timezones):
    # Переход на зимнее время: 25.10 в Берлине длится 25 часов,
    # с 22:00 UTC 24.10 до 23:00 UTC 25.10
    first = datetime(2026, 10, 24, 22, 0, tzinfo=UTC)
    syncs = spark.createDataFrame(
        [("net", first + timedelta(minutes=10 * i), None) for i in range(25 * 6)],
        SYNCS_SCHEMA,
    )
    status = spark.createDataFrame([("net", "1", first, 1, 1, 0)], LOG_SCHEMA)
    hourly = localize(
        station_hours(intervals(status, first, first + timedelta(hours=25))), timezones
    )

    (day,) = station_daily(hourly, coverage(syncs, timezones)).collect()

    assert str(day.local_date) == "2026-10-25"
    assert day.observed_share == pytest.approx(1.0)


def test_syncs_with_stale_feed_are_not_observed(spark, timezones):
    # Данные обновились в 10:00 UTC и замерли: синхронизации после 10:30 не засчитываются
    feed = at(10)
    syncs = spark.createDataFrame([("net", at(10, 10 * i), feed) for i in range(6)], SYNCS_SCHEMA)

    (hour,) = coverage(syncs, timezones).collect()

    assert hour.slots == 4
    assert hour.observed_share == pytest.approx(4 / 6)


def test_unfinished_local_days_are_dropped(spark, hourly, timezones):
    syncs = spark.createDataFrame([], SYNCS_SCHEMA)
    daily = station_daily(hourly, coverage(syncs, timezones)).join(timezones, "network_id")
    # Сутки 20.10 в Берлине заканчиваются в 22:00 UTC
    now = datetime(2026, 10, 20, 21, 0)

    kept = finished_days(daily, date(2026, 10, 19), date(2026, 10, 20), now).collect()

    assert [str(r.local_date) for r in kept] == []


def test_weather_of_hour_end_is_joined_to_the_hour(spark, hourly, timezones):
    syncs = spark.createDataFrame([], SYNCS_SCHEMA)
    weather = spark.createDataFrame(
        [("net", at(11), 9.5, 8.0, 1.2, 4.0, 61, at(12))],
        "network_id string, hour timestamp, temperature float, apparent_temperature float, "
        "precipitation float, wind_speed float, weather_code smallint, fetched_at timestamp",
    )

    rows = {
        r.local_hour: r
        for r in network_hourly(hourly, coverage(syncs, timezones), weather).collect()
    }

    assert rows[12].precipitation == pytest.approx(1.2)
    assert rows[13].precipitation is None
    assert (rows[12].pickups, rows[12].stations) == (5, 1)


def test_recomputed_days_replace_previous_run(spark, tmp_path):
    from spark_jobs.demand import write_days

    def run(rows, first, last):
        df = spark.createDataFrame(rows, "local_date date, network_id string, pickups int")
        write_days(
            df, "station_daily", first, last, str(tmp_path / "gold"), str(tmp_path / "export")
        )

    d1, d2, d3 = date(2026, 10, 20), date(2026, 10, 21), date(2026, 10, 22)
    run([(d1, "net", 1), (d2, "net", 2)], d1, d2)
    # Следующий запуск пересчитывает окно со сдвигом на день: d2 заменяется, d1 остаётся
    run([(d2, "net", 20), (d3, "net", 3)], d2, d3)

    rows = spark.read.format("delta").load(str(tmp_path / "gold" / "station_daily")).collect()
    assert sorted((str(r.local_date), r.pickups) for r in rows) == [
        ("2026-10-20", 1),
        ("2026-10-21", 20),
        ("2026-10-22", 3),
    ]
