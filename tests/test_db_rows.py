from datetime import UTC, datetime

from psycopg.types.json import Jsonb

from citybikes.db import (
    NETWORK_COLUMNS,
    feed_updated_at,
    network_row,
    parse_timestamp,
    station_rows,
)

NOW = datetime(2026, 10, 8, tzinfo=UTC)


def test_network_row_fills_missing_fields_with_none():
    row = dict(
        zip(
            (*NETWORK_COLUMNS, "first_seen_at", "last_seen_at"),
            network_row(
                {
                    "id": "velib",
                    "name": "Vélib'",
                    "location": {"city": "Paris"},
                    "company": ["Smovengo"],
                    "ebikes": True,
                },
                NOW,
            ),
            strict=True,
        )
    )
    assert row["company"] == ["Smovengo"]
    assert isinstance(row["location"], Jsonb)
    assert row["ebikes"] is True
    assert row["system"] is None
    assert row["license"] is None
    assert row["first_seen_at"] == row["last_seen_at"] == NOW


def test_station_rows_drop_duplicate_ids():
    stations = [
        {"id": "a", "name": "old", "free_bikes": 1, "extra": {"uid": 1}},
        {"id": "b", "name": "other"},
        {"id": "a", "name": "new", "free_bikes": 2},
    ]
    rows = {r[1]: r for r in station_rows("net", stations, NOW)}
    assert set(rows) == {"a", "b"}
    assert rows["a"][2] == "new"
    assert rows["a"][8] is None


def test_station_rows_keep_raw_timestamp():
    raw = "2026-10-08T09:28:46.069611+00:00Z"
    (row,) = station_rows("net", [{"id": "a", "timestamp": raw}], NOW)
    assert row[5] == raw


def test_parse_timestamp_accepts_offset_with_z_and_naive_utc():
    expected = datetime(2026, 10, 31, 7, 7, 1, 106739, tzinfo=UTC)
    assert parse_timestamp("2026-10-31T07:07:01.106739+00:00Z") == expected
    assert parse_timestamp("2026-10-31T07:07:01.106739Z") == expected
    assert parse_timestamp("2026-10-31T07:07:01.106739") == expected
    assert parse_timestamp("not a date") is None
    assert parse_timestamp(None) is None


def test_feed_updated_at_is_latest_station_timestamp():
    stations = [
        {"id": "a", "timestamp": "2026-10-31T07:07:00+00:00Z"},
        {"id": "b", "timestamp": "2026-10-31T07:07:02+00:00Z"},
        {"id": "c", "timestamp": None},
    ]
    assert feed_updated_at(stations) == datetime(2026, 10, 31, 7, 7, 2, tzinfo=UTC)
    assert feed_updated_at([{"id": "a"}]) is None
