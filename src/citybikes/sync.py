"""Синхронизация станций с учётом лимита API: инкрементальный обход и частый опрос"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from psycopg import Connection

from citybikes import db
from citybikes.api import CityBikesClient, NetworkNotFoundError, RateLimitExceededError

log = logging.getLogger(__name__)


@dataclass
class SyncStats:
    synced: int = 0
    stations: int = 0
    changes: int = 0
    empty: int = 0
    not_found: int = 0
    stopped_by_limit: bool = False


def sync_networks(
    client: CityBikesClient,
    conn: Connection,
    network_ids: Iterable[str],
    rate_limit_reserve: int = 0,
) -> SyncStats:
    stats = SyncStats()
    for network_id in network_ids:
        if client.remaining is not None and client.remaining <= rate_limit_reserve:
            log.info("В окне осталось %d запросов, остальное в следующий прогон", client.remaining)
            stats.stopped_by_limit = True
            break
        try:
            stations = client.get_network(network_id).get("stations") or []
        except RateLimitExceededError as e:
            log.warning("Остановка по лимиту: %s", e)
            stats.stopped_by_limit = True
            break
        except NetworkNotFoundError:
            # Сеть пропала из API после загрузки каталога - как пустая,
            # иначе останется первой в очереди
            log.warning("Сеть %s не найдена", network_id)
            stats.not_found += 1
            stations = []

        result = db.sync_stations(conn, network_id, stations, datetime.now(UTC))
        # Коммит после каждой сети
        conn.commit()
        stats.synced += 1
        stats.stations += result.stations
        stats.changes += result.changes
        stats.empty += result.stations == 0

    log.info("Итог синхронизации: %s", stats)
    return stats


def crawl_stations(
    client: CityBikesClient, conn: Connection, batch_size: int, rate_limit_reserve: int
) -> SyncStats:
    """Обновляет сети, которые синхронизировались давнее всего."""
    network_ids = db.select_networks_to_sync(conn, batch_size)
    return sync_networks(client, conn, network_ids, rate_limit_reserve)
