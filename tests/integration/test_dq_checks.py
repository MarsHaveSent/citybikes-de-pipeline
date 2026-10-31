from datetime import UTC, date, datetime, timedelta

from citybikes import db, dq

NOW = datetime.now(UTC)


def values(results):
    return {r.check.name: r.value for r in results}


def test_bronze_checks(conn):
    db.upsert_networks(conn, [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}], NOW)
    db.sync_stations(conn, "a", [], NOW - timedelta(minutes=10))
    db.upsert_weather(conn, [("a", NOW, 10, 9, 0, 3, 1, NOW - timedelta(hours=3))])

    results = dq.run_checks(
        lambda sql, params: conn.execute(sql, params).fetchone()[0],
        dq.BRONZE_CHECKS,
        {"polled": ["a"]},
    )
    found = values(results)

    assert 9.9 < found["bronze_polled_lag_minutes"] < 11
    assert found["bronze_stale_feed_networks"] == 0
    assert found["bronze_crawl_coverage_24h"] == 0.5
    assert found["bronze_catalog_lag_hours"] < 0.1
    assert 2.9 < found["bronze_weather_lag_hours"] < 3.1
    # Поля первой загрузки дрейфом не считаются
    assert found["bronze_new_api_fields_24h"] == 0
    assert {r.check.name: r.status for r in results}["bronze_weather_lag_hours"] == "warn"

    dq.save_results(conn, results, NOW, "manual__1")
    assert conn.execute("SELECT count(*) FROM ops_dq_results").fetchone()[0] == len(results)


def test_never_synced_polled_network_fails_freshness(conn):
    db.upsert_networks(conn, [{"id": "a", "name": "A"}], NOW)

    (result,) = dq.run_checks(
        lambda sql, params: conn.execute(sql, params).fetchone()[0],
        [dq.BRONZE_CHECKS[0]],
        {"polled": ["a"]},
    )

    assert result.status == "fail"


def test_stale_feed_is_counted_by_latest_sync(conn):
    db.upsert_networks(conn, [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}], NOW)
    feed = (NOW - timedelta(hours=2)).isoformat()
    # Сеть a замерла, у сети b была заморозка, но последняя синхронизация свежая
    db.sync_stations(conn, "a", [{"id": "1", "timestamp": feed}], NOW)
    db.sync_stations(conn, "b", [{"id": "1", "timestamp": feed}], NOW - timedelta(minutes=10))
    db.sync_stations(conn, "b", [{"id": "1", "timestamp": NOW.isoformat()}], NOW)

    results = dq.run_checks(
        lambda sql, params: conn.execute(sql, params).fetchone()[0],
        dq.BRONZE_CHECKS,
        {"polled": ["a", "b"]},
    )

    assert values(results)["bronze_stale_feed_networks"] == 1


def snapshot(ch, day, stations, extra_rows=()):
    rows = [(day, "bubi", s, datetime(2026, 10, 1, tzinfo=UTC)) for s in stations]
    rows += list(extra_rows)
    ch.insert(
        "stations_obt",
        rows,
        column_names=["snapshot_date", "network_id", "station_id", "station_timestamp"],
    )


def test_gold_checks(ch):
    snapshot(ch, date(2026, 10, 19), [str(i) for i in range(100)])
    # На следующий день станций на 10% меньше и появилась сеть без станций
    snapshot(
        ch,
        date(2026, 10, 20),
        [str(i) for i in range(90)],
        extra_rows=[(date(2026, 10, 20), "empty", "", None)],
    )

    results = dq.run_checks(
        lambda sql, params: ch.query(sql, parameters=params).result_rows[0][0],
        dq.GOLD_CHECKS,
        {"day": "2026-10-20"},
    )
    found = values(results)

    assert found["gold_snapshot_rows"] == 91
    assert found["gold_rows_change"] == 0.09
    assert found["gold_networks_change"] == 1.0
    assert found["gold_networks_without_stations_share"] == 0.5
    assert found["gold_duplicate_stations"] == 0
    assert found["gold_null_timestamp_share"] == 0
    statuses = {r.check.name: r.status for r in results}
    assert statuses["gold_rows_change"] == "warn"
    assert statuses["gold_networks_change"] == "fail"


def test_gold_first_snapshot_has_nothing_to_compare(ch):
    snapshot(ch, date(2026, 10, 19), ["1"])

    results = dq.run_checks(
        lambda sql, params: ch.query(sql, parameters=params).result_rows[0][0],
        dq.GOLD_CHECKS,
        {"day": "2026-10-19"},
    )

    assert {r.check.name: r.value for r in results}["gold_rows_change"] is None
    assert dq.finish(results)["fail"] == 0


def test_demand_checks(ch):
    day = date(2026, 10, 20)
    rows = [
        # Живая сеть: станции менялись
        (day, "live", "1", 5, 1.0),
        (day, "live", "2", 3, 1.0),
        # Застывшая: опрашивалась весь день, изменений нет
        (day, "frozen", "1", 0, 1.0),
        (day, "frozen", "2", 0, 1.0),
        # Опрос шёл только часть суток
        (day, "partial", "1", 4, 0.5),
    ]
    ch.insert(
        "station_daily",
        rows,
        column_names=["local_date", "network_id", "station_id", "changes", "observed_share"],
    )

    results = dq.run_checks(
        lambda sql, params: ch.query(sql, parameters=params).result_rows[0][0],
        dq.DEMAND_CHECKS,
        {"day": day.isoformat()},
    )

    assert values(results) == {
        "demand_observed_networks": 2,
        "demand_frozen_networks": 1,
        "demand_partially_observed_networks": 1,
    }


def test_market_checks(ch):
    day = date(2026, 10, 25)
    snapshot(ch, day, ["1", "2", "3"])
    ch.insert(
        "market_by_country",
        [(day, "DE", 83_000_000, 2), (day, "XK", None, 1)],
        column_names=["snapshot_date", "country", "population", "stations"],
    )

    results = dq.run_checks(
        lambda sql, params: ch.query(sql, parameters=params).result_rows[0][0],
        dq.MARKET_CHECKS,
        {"day": day.isoformat()},
    )

    found = values(results)
    assert found["market_stations_match_obt"] == 0
    assert found["market_countries_without_population"] == 1
    assert {r.check.name: r.status for r in results}["market_growth_point_cities"] == "fail"
