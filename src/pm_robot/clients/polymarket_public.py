"""Read-only public Polymarket API client.

This is intentionally small and dependency-free. It provides the read-only
client boundary for wallet discovery and bounded research evidence collection.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from pm_robot.clients.http import RateLimitedHttpClient


DATA_BASE = "https://data-api.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
LB_BASE = "https://lb-api.polymarket.com"
MAX_CURRENT_POSITIONS_LIMIT = 500
MAX_CLOSED_POSITIONS_LIMIT = 50
MAX_ACTIVITY_LIMIT = 500

@dataclass(frozen=True)
class PublicPolymarketClient:
    timeout: int = 20
    conn: sqlite3.Connection | None = None
    http: RateLimitedHttpClient | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if self.http is None:
            object.__setattr__(
                self,
                "http",
                RateLimitedHttpClient(timeout=self.timeout, conn=self.conn),
            )

    def get_json(self, base: str, path: str, params: dict[str, Any] | None = None) -> Any:
        assert self.http is not None
        return self.http.get_json(base, path, params)

    def positions(
        self,
        wallet: str,
        *,
        size_threshold: float = 0.0,
        limit: int = MAX_CURRENT_POSITIONS_LIMIT,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        data = self.get_json(
            DATA_BASE,
            "/positions",
            {
                "user": wallet,
                "sizeThreshold": str(size_threshold),
                "limit": str(_bounded_limit(limit, MAX_CURRENT_POSITIONS_LIMIT)),
                "offset": str(max(0, int(offset))),
            },
        )
        return data if isinstance(data, list) else []

    def closed_positions(
        self,
        wallet: str,
        *,
        limit: int = MAX_CLOSED_POSITIONS_LIMIT,
        offset: int = 0,
        size_threshold: float = 0.0,
    ) -> list[dict[str, Any]]:
        data = self.get_json(
            DATA_BASE,
            "/closed-positions",
            {
                "user": wallet,
                "limit": str(_bounded_limit(limit, MAX_CLOSED_POSITIONS_LIMIT)),
                "offset": str(max(0, int(offset))),
                "sizeThreshold": str(size_threshold),
            },
        )
        return data if isinstance(data, list) else []

    def position_values(self, wallet: str) -> list[dict[str, Any]]:
        """Return current position-value rows; this is not account equity."""

        data = self.get_json(DATA_BASE, "/value", {"user": wallet})
        return data if isinstance(data, list) else []

    def activity(
        self,
        wallet: str,
        *,
        limit: int = 100,
        offset: int = 0,
        start: int | None = None,
        end: int | None = None,
    ) -> list[dict[str, Any]]:
        params = {
            "user": wallet,
            "limit": str(_bounded_limit(limit, MAX_ACTIVITY_LIMIT)),
            "offset": str(max(0, int(offset))),
        }
        if start is not None:
            params["start"] = str(max(0, int(start)))
        if end is not None:
            params["end"] = str(max(0, int(end)))
        data = self.get_json(
            DATA_BASE,
            "/activity",
            params,
        )
        return data if isinstance(data, list) else []

    def global_activity(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        data = self.get_json(
            DATA_BASE,
            "/activity",
            {"limit": str(limit), "offset": str(offset)},
        )
        return data if isinstance(data, list) else []

    def recent_trades(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        taker_only: bool = True,
        min_cash_usdc: float = 0.0,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": str(limit),
            "offset": str(offset),
            "takerOnly": "true" if taker_only else "false",
        }
        if min_cash_usdc > 0:
            params["filterType"] = "CASH"
            params["filterAmount"] = str(min_cash_usdc)
        data = self.get_json(
            DATA_BASE,
            "/trades",
            params,
        )
        return data if isinstance(data, list) else []

    def wallet_trades(
        self,
        wallet: str,
        *,
        limit: int = 10_000,
        offset: int = 0,
        taker_only: bool = False,
    ) -> list[dict[str, Any]]:
        data = self.get_json(
            DATA_BASE,
            "/trades",
            {
                "user": wallet,
                "limit": str(limit),
                "offset": str(offset),
                "takerOnly": "true" if taker_only else "false",
            },
        )
        return data if isinstance(data, list) else []

    def leaderboard(self, metric: str = "profit", *, window: str = "30d") -> list[dict[str, Any]]:
        params = {"window": window} if window else {}
        data = self.get_json(LB_BASE, f"/{metric}", params)
        return data if isinstance(data, list) else []

    def trader_leaderboard(
        self,
        *,
        category: str = "OVERALL",
        time_period: str = "MONTH",
        order_by: str = "PNL",
        limit: int = 50,
        offset: int = 0,
        user: str = "",
    ) -> list[dict[str, Any]]:
        params = {
            "category": category,
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": str(limit),
            "offset": str(offset),
        }
        if user.strip():
            params["user"] = user.strip().lower()
        data = self.get_json(
            DATA_BASE,
            "/v1/leaderboard",
            params,
        )
        return data if isinstance(data, list) else []

    def market_by_slug(self, slug: str) -> dict[str, Any]:
        data = self.get_json(GAMMA_BASE, f"/markets/slug/{quote(slug, safe='')}")
        return data if isinstance(data, dict) else {}

    def book(self, token_id: str) -> dict[str, Any]:
        data = self.get_json(CLOB_BASE, "/book", {"token_id": token_id})
        return data if isinstance(data, dict) else {}


def _bounded_limit(limit: int, maximum: int) -> int:
    return min(maximum, max(1, int(limit)))
