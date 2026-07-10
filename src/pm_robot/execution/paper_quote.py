"""Conservative CLOB quote simulation for paper BUY orders."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any


PAPER_FEE_BPS = 100.0


@dataclass(frozen=True)
class PaperQuote:
    best_bid: float | None
    best_ask: float | None
    executable_price: float | None
    fillable_stake_usd: float
    fee_usd: float
    snapshot_at: int
    source: str
    raw_json: str


def simulate_buy_quote(book: dict[str, Any], requested_stake_usd: float) -> PaperQuote:
    snapshot_at = int(time.time())
    bids = _levels(book.get("bids"))
    asks = sorted(_levels(book.get("asks")), key=lambda item: item[0])
    best_bid = max((price for price, _ in bids), default=None)
    best_ask = asks[0][0] if asks else None
    remaining = max(float(requested_stake_usd), 0.0)
    filled_usd = 0.0
    shares = 0.0
    for price, size in asks:
        if price <= 0 or size <= 0 or remaining <= 0:
            continue
        level_usd = price * size
        take_usd = min(remaining, level_usd)
        filled_usd += take_usd
        shares += take_usd / price
        remaining -= take_usd
    executable = filled_usd / shares if shares > 0 else None
    return PaperQuote(
        best_bid=best_bid,
        best_ask=best_ask,
        executable_price=executable,
        fillable_stake_usd=filled_usd,
        fee_usd=filled_usd * PAPER_FEE_BPS / 10_000.0,
        snapshot_at=snapshot_at,
        source="polymarket_clob_book",
        raw_json=json.dumps(book, ensure_ascii=False, sort_keys=True)[:100_000],
    )


def _levels(value: Any) -> list[tuple[float, float]]:
    if not isinstance(value, list):
        return []
    out: list[tuple[float, float]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        try:
            price = float(item.get("price"))
            size = float(item.get("size"))
        except (TypeError, ValueError):
            continue
        if 0 < price <= 1 and size > 0:
            out.append((price, size))
    return out
