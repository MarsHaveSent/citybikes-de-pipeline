import io
import zipfile

from citybikes.geonames import parse_cities, parse_countries, read_cities_zip

COUNTRIES = (
    "# комментарий\n"
    "#ISO\tISO3\tISO-Numeric\tfips\tCountry\tCapital\tArea\tPopulation\tContinent\n"
    "DE\tDEU\t276\tGM\tGermany\tBerlin\t357021\t83491249\tEU\t.de\n"
    "AQ\tATA\t010\tAY\tAntarctica\t\t14000000\t\tAN\t.aq\n"
)


def city_line(geoname_id, name, feature_code, country, population):
    fields = [""] * 19
    fields[0], fields[1], fields[4], fields[5] = str(geoname_id), name, "52.52", "13.41"
    fields[7], fields[8], fields[14], fields[17] = (
        feature_code,
        country,
        str(population),
        "Europe/Berlin",
    )
    return "\t".join(fields) + "\n"


def test_countries_skip_comments_and_default_missing_population():
    assert parse_countries(COUNTRIES) == [
        ("DE", "Germany", "EU", 83491249),
        ("AQ", "Antarctica", "AN", 0),
    ]


def test_city_districts_are_dropped():
    lines = [
        city_line(1, "Berlin", "PPLC", "DE", 3426354),
        city_line(2, "Mitte", "PPLX", "DE", 98000),
    ]
    assert parse_cities(lines) == [(1, "Berlin", "DE", 52.52, 13.41, 3426354, "Europe/Berlin")]


def test_cities_are_read_from_zip():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("cities15000.txt", city_line(1, "Berlin", "PPLC", "DE", 3426354))
    assert [row[1] for row in read_cities_zip(buffer.getvalue())] == ["Berlin"]
