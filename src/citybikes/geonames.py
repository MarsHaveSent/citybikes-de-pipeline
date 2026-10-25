"""Справочники GeoNames (CC BY 4.0): население стран и города от 15 тыс. жителей."""

import io
import zipfile
from collections.abc import Iterable

import requests

from citybikes.api import USER_AGENT

COUNTRIES_URL = "https://download.geonames.org/export/dump/countryInfo.txt"
CITIES_URL = "https://download.geonames.org/export/dump/cities15000.zip"

# Районы внутри города (PPLX) записаны отдельными «городами», крупные города от них дублировались бы
CITY_DISTRICT = "PPLX"


def download(url: str, session: requests.Session | None = None) -> bytes:
    session = session or requests.Session()
    response = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=120)
    response.raise_for_status()
    return response.content


def parse_countries(text: str) -> list[tuple]:
    """Строки countryInfo.txt -> (iso2, название, континент, население)."""
    rows = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        rows.append((fields[0], fields[4], fields[8], int(fields[7] or 0)))
    return rows


def parse_cities(lines: Iterable[str]) -> list[tuple]:
    """Строки cities15000.txt -> (geoname_id, название, страна, широта, долгота, население,
    часовой пояс); районы внутри городов отбрасываются."""
    rows = []
    for line in lines:
        fields = line.rstrip("\n").split("\t")
        if fields[7] == CITY_DISTRICT:
            continue
        rows.append(
            (
                int(fields[0]),
                fields[1],
                fields[8],
                float(fields[4]),
                float(fields[5]),
                int(fields[14] or 0),
                fields[17],
            )
        )
    return rows


def read_cities_zip(content: bytes) -> list[tuple]:
    with (
        zipfile.ZipFile(io.BytesIO(content)) as archive,
        archive.open("cities15000.txt") as raw,
    ):
        return parse_cities(io.TextIOWrapper(raw, encoding="utf-8"))
