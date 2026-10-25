from datetime import UTC, datetime, timedelta

from citybikes import db

T0 = datetime(2026, 10, 15, 12, tzinfo=UTC)
T1 = T0 + timedelta(minutes=10)


def station(station_id, free_bikes, empty_slots=5, **extra):
    return {
        "id": station_id,
        "name": f"station {station_id}",
        "free_bikes": free_bikes,
        "empty_slots": empty_slots,
        "extra": extra,
    }


def add_network(conn, network_id="net", **fields):
    db.upsert_networks(conn, [{"id": network_id, "name": network_id, **fields}], T0)


def fetch(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


def test_first_sync_logs_initial_state(conn):
    add_network(conn)

    result = db.sync_stations(conn, "net", [station("a", 1), station("b", 2)], T0)

    assert (result.stations, result.changes) == (2, 2)
    assert fetch(conn, "SELECT station_id, free_bikes FROM station_status_log ORDER BY 1") == [
        ("a", 1),
        ("b", 2),
    ]
    assert fetch(conn, "SELECT stations_synced_at FROM networks") == [(T0,)]


def test_only_counter_changes_are_logged(conn):
    add_network(conn)
    db.sync_stations(conn, "net", [station("a", 1), station("b", 2), station("c", 3, ebikes=1)], T0)

    result = db.sync_stations(
        conn,
        "net",
        [
            {**station("a", 1), "name": "renamed"},
            station("b", 0),
            station("c", 3, ebikes=2),
        ],
        T1,
    )

    assert result.changes == 2
    logged = "SELECT station_id, ebikes FROM station_status_log WHERE observed_at = %s ORDER BY 1"
    assert fetch(conn, logged, (T1,)) == [("b", None), ("c", 2)]
    assert fetch(conn, "SELECT name FROM stations WHERE station_id = 'a'") == [("renamed",)]


def test_removed_station_is_deleted_but_history_kept(conn):
    add_network(conn)
    db.sync_stations(conn, "net", [station("a", 1), station("b", 2)], T0)

    db.sync_stations(conn, "net", [station("a", 1)], T1)

    assert fetch(conn, "SELECT station_id FROM stations") == [("a",)]
    assert fetch(conn, "SELECT count(*) FROM station_status_log WHERE station_id = 'b'") == [(1,)]


def test_sync_does_not_touch_other_networks(conn):
    add_network(conn, "net")
    add_network(conn, "other")
    db.sync_stations(conn, "other", [station("a", 1)], T0)

    db.sync_stations(conn, "net", [station("a", 5)], T1)

    assert fetch(conn, "SELECT network_id, free_bikes FROM stations ORDER BY 1") == [
        ("net", 5),
        ("other", 1),
    ]


def test_non_numeric_ebikes_are_stored_as_null(conn):
    add_network(conn)

    db.sync_stations(conn, "net", [station("a", 1, ebikes="n/a")], T0)

    assert fetch(conn, "SELECT ebikes FROM station_status_log") == [(None,)]


def test_networks_upsert_keeps_first_seen_at(conn):
    db.upsert_networks(conn, [{"id": "net", "name": "old"}], T0)

    db.upsert_networks(conn, [{"id": "net", "name": "new", "company": ["Acme"]}], T1)

    assert fetch(conn, "SELECT name, company, first_seen_at, last_seen_at FROM networks") == [
        ("new", ["Acme"], T0, T1)
    ]


def test_never_synced_networks_come_first(conn):
    for network_id in ("a", "b", "c"):
        add_network(conn, network_id)
    db.sync_stations(conn, "a", [], T1)
    db.sync_stations(conn, "c", [], T0)

    assert db.select_networks_to_sync(conn, limit=2) == ["b", "c"]


def test_network_locations_skip_networks_without_coordinates(conn):
    add_network(conn, "with", location={"latitude": 47.5, "longitude": 19.04})
    add_network(conn, "without", location={"city": "Nowhere"})

    assert db.select_network_locations(conn, ["with", "without", "unknown"]) == [
        ("with", 47.5, 19.04)
    ]


def test_register_fields_returns_only_new(conn):
    assert db.register_fields(conn, "station", {"id", "name"}, T0) == ["id", "name"]

    assert db.register_fields(conn, "station", {"id", "name", "rental_uris"}, T1) == ["rental_uris"]
    registry = "SELECT field, first_seen_at, last_seen_at FROM schema_fields ORDER BY 1"
    assert fetch(conn, registry) == [
        ("id", T0, T1),
        ("name", T0, T1),
        ("rental_uris", T1, T1),
    ]


def test_weather_upsert_is_idempotent(conn):
    row = ("net", T0, 12.5, 11.0, 0.0, 3.2, 1, T0)

    db.upsert_weather(conn, [row])
    db.upsert_weather(conn, [(*row[:2], 13.0, *row[3:7], T1)])

    assert fetch(conn, "SELECT temperature, fetched_at FROM weather_hourly") == [(13.0, T1)]


def test_network_timezones_upsert(conn):
    db.upsert_network_timezones(conn, [("net", "Europe/Berlin", T0)])
    db.upsert_network_timezones(conn, [("net", "Europe/Paris", T1)])

    assert fetch(conn, "SELECT timezone, updated_at FROM network_timezones") == [
        ("Europe/Paris", T1)
    ]


def test_every_sync_is_journaled_even_without_changes(conn):
    add_network(conn)
    db.sync_stations(conn, "net", [station("a", 1), station("b", 2)], T0)
    db.sync_stations(conn, "net", [station("a", 1), station("b", 2)], T1)

    assert fetch(conn, "SELECT synced_at, stations, changes FROM network_syncs ORDER BY 1") == [
        (T0, 2, 2),
        (T1, 2, 0),
    ]


def test_geonames_are_replaced_entirely(conn):
    db.replace_geonames(conn, [("DE", "Germany", "EU", 83_000_000)], [], T0)
    db.replace_geonames(
        conn,
        [("FR", "France", "EU", 68_000_000)],
        [(2988507, "Paris", "FR", 48.85, 2.35, 2_138_551, "Europe/Paris")],
        T1,
    )

    assert fetch(conn, "SELECT iso2, loaded_at FROM geo_countries") == [("FR", T1)]
    assert fetch(conn, "SELECT name, population FROM geo_cities") == [("Paris", 2_138_551)]
