"""Клиент CityBikes API v2."""

import logging
import time
from collections.abc import Callable
from typing import Any

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.citybik.es/v2"
USER_AGENT = "citybikes-de-pipeline (+https://github.com/MarsHaveSent/citybikes-de-pipeline)"

# Дольше не ждёт: окно лимита часовое, остаток отдается следующему прогону
MAX_RATE_LIMIT_WAIT = 120


class RateLimitExceededError(Exception):
    pass


class NetworkNotFoundError(Exception):
    pass


class CityBikesClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout: float = 30,
        max_retries: int = 3,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self._sleep = sleep
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        # Остаток запросов в текущем окне по заголовку последнего ответа
        self.remaining: int | None = None

    def get_networks(self) -> list[dict[str, Any]]:
        return self._get("/networks")["networks"]

    def get_network(self, network_id: str) -> dict[str, Any]:
        try:
            return self._get(f"/networks/{network_id}")["network"]
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                raise NetworkNotFoundError(network_id) from e
            raise

    def _get(self, path: str) -> dict[str, Any]:
        url = self.base_url + path
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt == self.max_retries:
                    raise
                log.warning("%s: %s, попытка %d", url, type(e).__name__, attempt)
                self._sleep(5 * 2**attempt)
                continue

            if "ratelimit-remaining" in response.headers:
                self.remaining = int(response.headers["ratelimit-remaining"])

            if response.status_code == 429:
                wait = int(response.headers.get("ratelimit-reset", MAX_RATE_LIMIT_WAIT + 1))
                if wait > MAX_RATE_LIMIT_WAIT or attempt == self.max_retries:
                    raise RateLimitExceededError(f"лимит исчерпан, сброс через {wait} с")
                log.warning("429 на %s, ждём %d с", url, wait)
                self._sleep(wait)
                continue

            if response.status_code >= 500 and attempt < self.max_retries:
                log.warning("%s: HTTP %d, попытка %d", url, response.status_code, attempt)
                self._sleep(5 * 2**attempt)
                continue

            response.raise_for_status()
            return response.json()

        raise RuntimeError("unreachable")
