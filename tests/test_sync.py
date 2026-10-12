import pytest

from citybikes import sync
from citybikes.api import NetworkNotFoundError, RateLimitExceededError
from citybikes.db import StationsSyncResult


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

    def sync_stations(conn, network_id, stations, synced_at):
        calls[network_id] = len(stations)
        return StationsSyncResult(stations=len(stations), changes=len(stations) // 2)

    monkeypatch.setattr(sync.db, "sync_stations", sync_stations)
    return calls


def queue(monkeypatch, ids):
    monkeypatch.setattr(sync.db, "select_networks_to_sync", lambda conn, limit: ids[:limit])


def test_missing_network_is_synced_as_empty(monkeypatch, synced):
    queue(monkeypatch, ["a", "gone"])
    client = FakeClient({"a": {"stations": [{"id": "1"}]}, "gone": NetworkNotFoundError("gone")})
    conn = FakeConn()

    stats = sync.crawl_stations(client, conn, batch_size=10, rate_limit_reserve=0)

    assert synced == {"a": 1, "gone": 0}
    assert (stats.synced, stats.empty, stats.not_found) == (2, 1, 1)
    assert conn.commits == 2


def test_stops_on_rate_limit_error(monkeypatch, synced):
    queue(monkeypatch, ["a", "b", "c"])
    client = FakeClient({"a": {"stations": []}, "b": RateLimitExceededError(), "c": {}})

    stats = sync.crawl_stations(client, FakeConn(), batch_size=10, rate_limit_reserve=0)

    assert synced == {"a": 0}
    assert stats.stopped_by_limit


def test_keeps_reserve_for_polling(monkeypatch, synced):
    queue(monkeypatch, ["a", "b", "c"])
    client = FakeClient({n: {"stations": []} for n in "abc"}, remaining=12)

    stats = sync.crawl_stations(client, FakeConn(), batch_size=10, rate_limit_reserve=10)

    assert list(synced) == ["a", "b"]
    assert stats.stopped_by_limit


def test_poll_syncs_given_networks_and_counts_changes(synced):
    client = FakeClient({"a": {"stations": [{"id": "1"}, {"id": "2"}]}, "b": {"stations": []}})

    stats = sync.sync_networks(client, FakeConn(), ["a", "b"])

    assert synced == {"a": 2, "b": 0}
    assert (stats.stations, stats.changes, stats.empty) == (2, 1, 1)
