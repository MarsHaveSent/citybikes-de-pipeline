from datetime import UTC, datetime

import pytest
import requests

from citybikes.weather import FORECAST_URL, fetch_weather, timezone_rows, weather_rows

LOCATIONS = [("budapest", 47.5, 19.04), ("barcelona", 41.39, 2.16)]
NOW = datetime(2026, 10, 12, 10, 30, tzinfo=UTC)


def point(temperatures, timezone="Europe/Budapest"):
    # timeformat=unixtime: время в секундах от эпохи, то есть в UTC при любом timezone
    times = [int(datetime(2026, 10, 12, h, tzinfo=UTC).timestamp()) for h in (9, 10, 11)]
    return {
        "timezone": timezone,
        "hourly": {
            "time": times,
            "temperature_2m": temperatures,
            "apparent_temperature": temperatures,
            "precipitation": [0.0, 0.4, 1.2],
            "wind_speed_10m": [5.0, 6.0, 7.0],
            "weather_code": [1, 61, 63],
        },
    }


def test_rows_keep_location_order_and_drop_future_hours():
    rows = weather_rows(LOCATIONS, [point([10, 11, 12]), point([20, 21, 22])], NOW)

    assert [(r[0], r[1].hour, r[2]) for r in rows] == [
        ("budapest", 9, 10),
        ("budapest", 10, 11),
        ("barcelona", 9, 20),
        ("barcelona", 10, 21),
    ]
    assert rows[1][4] == 0.4
    assert all(r[-1] == NOW for r in rows)


def test_rows_require_point_per_location():
    with pytest.raises(ValueError):
        weather_rows(LOCATIONS, [point([10, 11, 12])], NOW)


def test_single_location_response_is_wrapped(requests_mock):
    requests_mock.get(FORECAST_URL, json=point([1, 2, 3]))
    assert len(fetch_weather(LOCATIONS[:1])) == 1


def test_http_error_is_raised(requests_mock):
    requests_mock.get(FORECAST_URL, status_code=500)
    with pytest.raises(requests.HTTPError):
        fetch_weather(LOCATIONS)


def test_timezone_rows_follow_location_order():
    payload = [point([1, 2, 3]), point([1, 2, 3], timezone="Europe/Madrid")]

    assert timezone_rows(LOCATIONS, payload, NOW) == [
        ("budapest", "Europe/Budapest", NOW),
        ("barcelona", "Europe/Madrid", NOW),
    ]
