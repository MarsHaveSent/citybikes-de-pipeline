import pytest

from spark_jobs.market import (
    growth_points,
    market_by_country,
    networks_of,
    operator_presence,
    with_country_names,
)

OBT_SCHEMA = (
    "network_id string, network_name string, network_country string, network_operator string, "
    "network_latitude double, network_longitude double, station_id string, "
    "station_latitude double, station_longitude double, station_has_ebikes boolean, "
    "station_ebikes int"
)
BERLIN = (52.52, 13.40)


@pytest.fixture
def obt(spark):
    rows = [
        # Оператор A: 3 станции, электровелосипеды стоят на двух. Третья их поддерживает
        # (has_ebikes), но сейчас их там нет
        ("a1", "A Berlin", "DE", "A", *BERLIN, "1", 52.53, 13.41, True, 2),
        ("a1", "A Berlin", "DE", "A", *BERLIN, "2", 52.51, 13.39, None, 1),
        ("a1", "A Berlin", "DE", "A", *BERLIN, "3", 52.52, 13.40, True, 0),
        # Оператор B: 1 станция, про электро не сообщает
        ("b1", "B Berlin", "DE", "B", *BERLIN, "1", 52.50, 13.42, None, None),
        # Сеть без оператора и без станций
        ("x1", "Solo", "FR", None, 48.85, 2.35, None, None, None, None, None),
    ]
    return spark.createDataFrame(rows, OBT_SCHEMA)


def test_market_concentration_and_density(spark, obt):
    networks = networks_of(obt)
    countries = spark.createDataFrame(
        [("DE", "Germany", "EU", 1_000_000), ("FR", "France", "EU", 0)],
        "iso2 string, name string, continent string, population long",
    )

    rows = {
        r.country: r
        for r in market_by_country(operator_presence(networks), networks, countries).collect()
    }

    de = rows["DE"]
    # Доли 75% и 25%: 75² + 25² = 6250
    assert (de.stations, de.networks, de.operators, de.hhi) == (4, 2, 2, 6250)
    assert (de.top_operator, de.top_operator_share) == ("A", 0.75)
    assert de.stations_per_million == 4.0
    # Число электровелосипедов сообщают 3 станции, они есть на двух
    assert de.ebike_station_share == pytest.approx(2 / 3)
    assert rows["FR"].stations_per_million is None


def test_network_without_operator_counts_as_its_own_operator(obt):
    presence = {(r.operator, r.country) for r in operator_presence(networks_of(obt)).collect()}
    assert ("Solo", "FR") in presence


def test_growth_points(spark, obt):
    cities = spark.createDataFrame(
        [
            (1, "Berlin", "DE", *BERLIN, 3_400_000),
            # ~100 км от ближайшей сети
            (2, "Far City", "DE", 53.42, 13.40, 400_000),
            # Меньше порога крупного города
            (3, "Town", "DE", 52.52, 13.40, 50_000),
        ],
        "geoname_id long, name string, country string, latitude double, longitude double, "
        "population long",
    )
    stations = obt.filter("station_id IS NOT NULL")

    rows = {r.city: r for r in growth_points(cities, networks_of(obt), stations).collect()}

    assert set(rows) == {"Berlin", "Far City"}
    berlin, far = rows["Berlin"], rows["Far City"]
    assert (berlin.without_network, berlin.stations_in_radius) == (False, 4)
    assert berlin.stations_per_100k == pytest.approx(4 / 3_400_000 * 1e5)
    assert far.without_network
    assert far.nearest_network_km == pytest.approx(100, abs=1)
    assert far.stations_in_radius == 0


def test_country_names_attached_by_iso_code(spark):
    df = spark.createDataFrame([("DE",), ("XX",)], "country string")
    countries = spark.createDataFrame(
        [("DE", "Germany", "EU", 83_000_000)],
        "iso2 string, name string, continent string, population long",
    )

    rows = {r.country: r for r in with_country_names(df, countries).collect()}

    assert (rows["DE"].country_name, rows["DE"].continent) == ("Germany", "EU")
    assert (rows["XX"].country_name, rows["XX"].continent) == (None, None)
