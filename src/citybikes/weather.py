"""Почасовая погода из Open-Meteo для городов опрашиваемых сетей."""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import requests

from citybikes.api import USER_AGENT

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = (
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
)


def fetch_weather(
    locations: Sequence[tuple[str, float, float]], session: requests.Session | None = None
) -> list[dict[str, Any]]:
    """Один запрос на все координаты отдает список в порядке переданных точек"""
    session = session or requests.Session()
    response = session.get(
        FORECAST_URL,
        params={
            "latitude": ",".join(str(lat) for _, lat, _ in locations),
            "longitude": ",".join(str(lon) for _, _, lon in locations),
            "hourly": ",".join(HOURLY_VARIABLES),
            # Прошедшие сутки перезаписываются каждый час: Open-Meteo уточняет недавние значения
            "past_days": 1,
            "forecast_days": 1,
            # auto - пояс по координатам, он нужен витринам спроса для местных суток;
            # время приходит в в UTC
            "timezone": "auto",
            "timeformat": "unixtime",
        },
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    # Для одной точки API возвращает объект, а не список
    return payload if isinstance(payload, list) else [payload]


def weather_rows(
    locations: Sequence[tuple[str, float, float]],
    payload: list[dict[str, Any]],
    now: datetime,
) -> list[tuple]:
    """Строки для weather_hourly; часы из будущего (прогноз) отбрасываются."""
    rows = []
    for (network_id, _, _), point in zip(locations, payload, strict=True):
        hourly = point["hourly"]
        for i, time in enumerate(hourly["time"]):
            hour = datetime.fromtimestamp(time, UTC)
            if hour > now:
                continue
            rows.append(
                (
                    network_id,
                    hour,
                    *(hourly[var][i] for var in HOURLY_VARIABLES),
                    now,
                )
            )
    return rows


def timezone_rows(
    locations: Sequence[tuple[str, float, float]],
    payload: list[dict[str, Any]],
    now: datetime,
) -> list[tuple]:
    """Строки для network_timezones: пояс, который Open-Meteo определил по координатам."""
    return [
        (network_id, point["timezone"], now)
        for (network_id, _, _), point in zip(locations, payload, strict=True)
    ]
