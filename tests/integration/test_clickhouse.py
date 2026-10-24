from datetime import date

import pytest

from citybikes.clickhouse import load_snapshot

DAY1, DAY2 = date(2026, 10, 19), date(2026, 10, 20)
# Источник с той же структурой
SOURCE = "src"
COLUMNS = ["snapshot_date", "network_id", "network_name", "station_id"]


def fill_source(ch, day, network_name, stations=("1", "2")):
    ch.command("DROP TABLE IF EXISTS src")
    ch.command("CREATE TABLE src AS stations_obt ENGINE = Memory")
    if stations:
        ch.insert("src", [(day, "bubi", network_name, s) for s in stations], column_names=COLUMNS)


def snapshots(ch):
    return ch.query(
        "SELECT toString(snapshot_date), any(network_name), count()"
        " FROM stations_obt GROUP BY snapshot_date ORDER BY snapshot_date"
    ).result_rows


def test_reload_replaces_only_its_day(ch):
    for day in (DAY1, DAY2):
        fill_source(ch, day, "Bubi")
        load_snapshot(ch, "stations_obt", day.isoformat(), SOURCE)

    fill_source(ch, DAY2, "Bubi renamed", stations=("1",))
    assert load_snapshot(ch, "stations_obt", DAY2.isoformat(), SOURCE) == 1

    assert snapshots(ch) == [("2026-10-19", "Bubi", 2), ("2026-10-20", "Bubi renamed", 1)]
    assert "stations_obt_stage" not in ch.query("SHOW TABLES").result_columns[0]


def test_rejects_rows_of_other_day(ch):
    fill_source(ch, DAY1, "Bubi")
    load_snapshot(ch, "stations_obt", DAY1.isoformat(), SOURCE)
    fill_source(ch, DAY2, "Stale")

    with pytest.raises(ValueError, match="ожидался 2026-10-19"):
        load_snapshot(ch, "stations_obt", DAY1.isoformat(), SOURCE)
    assert snapshots(ch) == [("2026-10-19", "Bubi", 2)]


def test_rejects_empty_source(ch):
    fill_source(ch, DAY1, "Bubi", stations=())

    with pytest.raises(ValueError, match="нет строк"):
        load_snapshot(ch, "stations_obt", DAY1.isoformat(), SOURCE)


def test_null_station_id_becomes_empty_string(ch):
    # В parquet у сетей без станций station_id = NULL, а колонка ключа сортировки не Nullable
    fill_source(ch, DAY1, "Empty", stations=("x",))
    source = "(SELECT * REPLACE (CAST(NULL, 'Nullable(String)') AS station_id) FROM src)"
    load_snapshot(ch, "stations_obt", DAY1.isoformat(), source)

    assert ch.query("SELECT station_id FROM stations_obt").result_rows == [("",)]


def test_load_days_replaces_only_days_present_in_source(ch):
    from citybikes.clickhouse import load_days

    columns = ["local_date", "network_id", "station_id", "pickups"]

    def fill(rows):
        ch.command("DROP TABLE IF EXISTS src_daily")
        ch.command("CREATE TABLE src_daily AS station_daily ENGINE = Memory")
        ch.insert("src_daily", rows, column_names=columns)

    fill([(DAY1, "bubi", "1", 10), (DAY2, "bubi", "1", 20)])
    load_days(ch, "station_daily", "src_daily")
    fill([(DAY2, "bubi", "1", 25)])

    assert load_days(ch, "station_daily", "src_daily") == {DAY2: 1}
    rows = ch.query("SELECT local_date, pickups FROM station_daily ORDER BY local_date").result_rows
    assert rows == [(DAY1, 10), (DAY2, 25)]
