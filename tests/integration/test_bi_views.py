from datetime import date

SUNDAY, MONDAY = date(2026, 10, 25), date(2026, 10, 26)


def test_snapshot_views_show_latest_day(ch):
    ch.insert(
        "market_by_country",
        [(SUNDAY, "DE", 1), (MONDAY, "DE", 2)],
        column_names=["snapshot_date", "country", "stations"],
    )

    rows = ch.query("SELECT snapshot_date, stations FROM bi_market_by_country").result_rows
    assert rows == [(MONDAY, 2)]


def test_demand_views_keep_only_well_observed_days(ch):
    ch.insert(
        "station_daily",
        [(SUNDAY, "full", "1", 0.95), (MONDAY, "full", "1", 1.0), (MONDAY, "gap", "1", 0.5)],
        column_names=["local_date", "network_id", "station_id", "observed_share"],
    )
    ch.insert(
        "network_hourly",
        [(SUNDAY, 8, "full", 0.0), (MONDAY, 8, "full", 1.2), (MONDAY, 8, "gap", 0.0)],
        column_names=["local_date", "local_hour", "network_id", "precipitation"],
    )

    daily = ch.query(
        "SELECT network_id, local_date, weekday, weekday_name, day_type"
        " FROM bi_station_daily ORDER BY local_date"
    ).result_rows
    assert daily == [
        ("full", SUNDAY, 7, "7 Вс", "выходные"),
        ("full", MONDAY, 1, "1 Пн", "будни"),
    ]
    hourly = ch.query(
        "SELECT network_id, local_date, weekday, day_type, is_rainy"
        " FROM bi_network_hourly ORDER BY local_date"
    ).result_rows
    assert hourly == [
        ("full", SUNDAY, 7, "выходные", False),
        ("full", MONDAY, 1, "будни", True),
    ]
