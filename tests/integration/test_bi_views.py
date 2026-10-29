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


def test_world_views_get_country_names_from_market(ch):
    ch.insert(
        "market_by_country",
        [(MONDAY, "DE", "Germany", "EU", 1)],
        column_names=["snapshot_date", "country", "country_name", "continent", "stations"],
    )
    ch.insert(
        "operator_presence",
        [(MONDAY, "Nextbike", "DE", 1, 1)],
        column_names=["snapshot_date", "operator", "country", "networks", "stations"],
    )
    ch.insert(
        "stations_obt",
        [
            (SUNDAY, "n1", "Net", "DE", "1"),
            (SUNDAY, "n1", "Net", "DE", "2"),
            (MONDAY, "n1", "Net", "DE", "1"),
        ],
        column_names=[
            "snapshot_date",
            "network_id",
            "network_name",
            "network_country",
            "station_id",
        ],
    )

    presence = ch.query("SELECT operator, country_name, continent FROM bi_operator_presence")
    assert presence.result_rows == [("Nextbike", "Germany", "Европа")]
    stations = ch.query("SELECT station_id, country_name, continent FROM bi_stations")
    assert stations.result_rows == [("1", "Germany", "Европа")]
    history = ch.query(
        "SELECT snapshot_date, stations, country_name FROM bi_network_snapshots"
        " ORDER BY snapshot_date"
    )
    assert history.result_rows == [(SUNDAY, 2, "Germany"), (MONDAY, 1, "Germany")]
