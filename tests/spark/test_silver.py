import json

import pytest
from pyspark.sql import functions as F

from spark_jobs.silver import (
    clean_text,
    normalize_operator,
    parse_bool,
    parse_timestamp,
    transform_networks,
    transform_stations,
)


def evaluate(spark, func, values, type_="string"):
    df = spark.createDataFrame([(v,) for v in values], f"v {type_}")
    return [row[0] for row in df.select(func(F.col("v"))).collect()]


def as_text(ts):
    # collect() переводит timestamp в часовой пояс машины, строка форматируется в UTC сессии
    return F.date_format(ts, "yyyy-MM-dd HH:mm:ss")


# Все форматы из профиля данных
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2025-04-23T08:56:41.091621+00:00Z", "2025-04-23 08:56:41"),
        ("2024-09-04T07:49:04.764247Z", "2024-09-04 07:49:04"),
        ("2026-04-22T00:34:01", "2026-04-22 00:34:01"),
        ("2026-04-22 00:34:01", "2026-04-22 00:34:01"),
        ("2026-10-16T17:59:14+02:00", "2026-10-16 15:59:14"),
        ("2026-10-16T19:59:19-04:00", "2026-10-16 23:59:19"),
        ("1659630627", "2022-08-04 16:30:27"),
        ("1659630627000", "2022-08-04 16:30:27"),
        ("1970-01-01T00:00:00", None),
        ("not a date", None),
        (None, None),
    ],
)
def test_parse_timestamp(spark, raw, expected):
    assert evaluate(spark, lambda c: as_text(parse_timestamp(c)), [raw]) == [expected]


def test_parse_bool(spark):
    values = ["true", "1", "false", "0", " TRUE ", "yes", None]
    assert evaluate(spark, parse_bool, values) == [True, True, False, False, True, None, None]


def test_clean_text(spark):
    values = ["  Main st ", "", "   ", None]
    assert evaluate(spark, clean_text, values) == ["Main st", None, None, None]


def test_networks_location_and_companies(networks_df):
    raw = networks_df(
        {
            "id": "a",
            "name": " Bubi ",
            "location": json.dumps(
                {"city": "Budapest", "country": "HU", "latitude": 47.5, "longitude": 19.04}
            ),
            "company": [" BKK ", "", "Csepel"],
        },
        {"id": "b", "name": "Empty", "company": []},
        {"id": "c", "name": "Unknown"},
    )

    rows = {r.network_id: r for r in transform_networks(raw).collect()}

    a = rows["a"]
    assert (a.name, a.city, a.country, a.latitude, a.longitude) == (
        "Bubi",
        "Budapest",
        "HU",
        47.5,
        19.04,
    )
    assert (a.companies, a.companies_count) == (["BKK", "Csepel"], 2)
    assert rows["b"].companies_count == 0
    assert (rows["c"].city, rows["c"].companies_count) == (None, 0)


def test_stations_extra_with_mixed_types(stations_df):
    extra = {
        "renting": 1,
        "returning": False,
        "last_updated": 1659630627,
        "slots": 12,
        "ebikes": "n/a",
        "has_ebikes": True,
        "payment": ["key", "phone"],
        "payment-terminal": True,
        "address": " Andrássy út 1 ",
        "post_code": "",
    }
    raw = stations_df(
        {"network_id": "a", "station_id": "1", "name": "  Deák ", "extra": json.dumps(extra)},
        {"network_id": "a", "station_id": "2", "extra": None},
    )

    df = transform_stations(raw).withColumn("last_updated", as_text(F.col("last_updated")))
    rows = {r.station_id: r for r in df.collect()}

    full = rows["1"]
    assert full.name == "Deák"
    assert (full.renting, full.returning, full.has_ebikes, full.payment_terminal) == (
        True,
        False,
        True,
        True,
    )
    assert full.last_updated == "2022-08-04 16:30:27"
    assert (full.slots, full.ebikes) == (12, None)
    assert full.payment == ["key", "phone"]
    assert (full.address, full.post_code) == ("Andrássy út 1", None)

    empty = rows["2"]
    assert (empty.renting, empty.last_updated, empty.payment) == (None, None, None)


@pytest.mark.parametrize(
    ("company", "operator"),
    [
        ("Nextbike GmbH", "Nextbike"),
        ("nextbike GmbH", "Nextbike"),
        ("Motivate International, Inc.", "Motivate"),
        ("Motivate LLC", "Motivate"),
        ("YouBike Co., Ltd.", "YouBike"),
        ("Deutsche Bahn AG", "Deutsche Bahn"),
        ("JCDecaux", "JCDecaux"),
        ("", None),
        (None, None),
    ],
)
def test_normalize_operator(spark, company, operator):
    assert evaluate(spark, normalize_operator, [company]) == [operator]


def test_network_without_companies_has_no_operator(networks_df):
    rows = transform_networks(networks_df({"id": "a", "name": "A", "company": []})).collect()
    assert rows[0].operator is None
