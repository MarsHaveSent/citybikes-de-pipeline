from datetime import UTC, datetime

from psycopg.types.json import Jsonb

from citybikes.db import NETWORK_COLUMNS, network_row, station_rows

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
