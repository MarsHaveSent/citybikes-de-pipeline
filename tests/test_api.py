import pytest
import requests

from citybikes.api import (
    BASE_URL,
    CityBikesClient,
    NetworkNotFoundError,
    RateLimitExceededError,
)

NETWORK_URL = f"{BASE_URL}/networks/velib"


@pytest.fixture
def sleeps():
    return []


@pytest.fixture
def client(sleeps):
    return CityBikesClient(sleep=sleeps.append)


def test_tracks_remaining_requests(client, requests_mock):
    requests_mock.get(
        NETWORK_URL, json={"network": {"id": "velib"}}, headers={"ratelimit-remaining": "42"}
    )
    assert client.get_network("velib") == {"id": "velib"}
    assert client.remaining == 42


def test_waits_on_short_rate_limit_and_retries(client, sleeps, requests_mock):
    requests_mock.get(
        NETWORK_URL,
        [
            {"status_code": 429, "headers": {"ratelimit-reset": "30"}},
            {"json": {"network": {"id": "velib"}}},
        ],
    )
    assert client.get_network("velib") == {"id": "velib"}
    assert sleeps == [30]


def test_gives_up_on_long_rate_limit(client, sleeps, requests_mock):
    requests_mock.get(NETWORK_URL, status_code=429, headers={"ratelimit-reset": "3000"})
    with pytest.raises(RateLimitExceededError):
        client.get_network("velib")
    assert sleeps == []


def test_retries_server_errors(client, sleeps, requests_mock):
    requests_mock.get(NETWORK_URL, [{"status_code": 502}, {"json": {"network": {"id": "velib"}}}])
    assert client.get_network("velib") == {"id": "velib"}
    assert len(sleeps) == 1


def test_raises_after_last_server_error(client, requests_mock):
    requests_mock.get(NETWORK_URL, status_code=503)
    with pytest.raises(requests.HTTPError):
        client.get_network("velib")


def test_missing_network(client, requests_mock):
    requests_mock.get(NETWORK_URL, status_code=404)
    with pytest.raises(NetworkNotFoundError):
        client.get_network("velib")
