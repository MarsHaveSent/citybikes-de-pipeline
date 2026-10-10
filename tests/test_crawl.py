import pytest

from citybikes import crawl
from citybikes.api import NetworkNotFoundError, RateLimitExceededError


class FakeConn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


class FakeClient:
    def __init__(self, responses, remaining=None):
        self.responses = responses
        self.remaining = remaining

    def get_network(self, network_id):
        result = self.responses[network_id]
        if isinstance(result, Exception):
            raise result
        if self.remaining is not None:
            self.remaining -= 1
        return result


@pytest.fixture
def synced(monkeypatch):
    calls = {}

    def replace_stations(conn, network_id, stations, synced_at):
        calls[network_id] = len(stations)
        return len(stations)

    monkeypatch.setattr(crawl.db, "replace_stations", replace_stations)
    return calls


def queue(monkeypatch, ids):
    monkeypatch.setattr(crawl.db, "select_networks_to_sync", lambda conn, limit: ids[:limit])


def test_missing_network_is_synced_as_empty(monkeypatch, synced):
    queue(monkeypatch, ["a", "gone"])
    client = FakeClient({"a": {"stations": [{"id": "1"}]}, "gone": NetworkNotFoundError("gone")})
    conn = FakeConn()

    stats = crawl.crawl_stations(client, conn, batch_size=10, rate_limit_reserve=0)

    assert synced == {"a": 1, "gone": 0}
    assert (stats.synced, stats.empty, stats.not_found) == (2, 1, 1)
    assert conn.commits == 2


def test_stops_on_rate_limit_error(monkeypatch, synced):
    queue(monkeypatch, ["a", "b", "c"])
    client = FakeClient({"a": {"stations": []}, "b": RateLimitExceededError(), "c": {}})

    stats = crawl.crawl_stations(client, FakeConn(), batch_size=10, rate_limit_reserve=0)

    assert synced == {"a": 0}
    assert stats.stopped_by_limit


def test_keeps_reserve_for_polling(monkeypatch, synced):
    queue(monkeypatch, ["a", "b", "c"])
    client = FakeClient({n: {"stations": []} for n in "abc"}, remaining=12)

    stats = crawl.crawl_stations(client, FakeConn(), batch_size=10, rate_limit_reserve=10)

    assert list(synced) == ["a", "b"]
    assert stats.stopped_by_limit
