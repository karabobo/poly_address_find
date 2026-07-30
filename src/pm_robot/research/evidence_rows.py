"""Stable row identities for public Polymarket evidence."""

from __future__ import annotations

from collections.abc import Callable, Iterable
import json
import math
from typing import Any


def dedupe_activity_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse the same trade/event when only non-semantic metadata differs."""

    return _dedupe(rows, key_fn=_activity_key)


def dedupe_closed_position_rows(
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse repeated closed-position evidence using settlement facts."""

    return _dedupe(rows, key_fn=_closed_position_key)


def _dedupe(
    rows: Iterable[dict[str, Any]],
    *,
    key_fn: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for source_row in rows:
        if not isinstance(source_row, dict):
            continue
        row = dict(source_row)
        key = key_fn(row)
        existing = unique.get(key)
        if existing is None or _richness(row) > _richness(existing):
            unique[key] = row
    return list(unique.values())


def _activity_key(row: dict[str, Any]) -> str:
    trade_id = _text(row, "tradeId", "trade_id")
    tx_hash = _text(row, "transactionHash", "transaction_hash", "txHash", "tx_hash")
    log_index = _text(row, "logIndex", "log_index")
    facts = (
        _text(row, "type") or "TRADE",
        _text(row, "asset", "assetId", "asset_id", "tokenId", "token_id"),
        _text(row, "conditionId", "condition_id"),
        _text(row, "slug", "marketSlug", "market_slug"),
        _text(row, "outcome"),
        _text(row, "side"),
        _time(row, "timestamp"),
        _number(row, "size"),
        _number(row, "price"),
        _trade_usdc(row),
    )
    if trade_id:
        return _key("trade_id", trade_id)
    if tx_hash:
        return _key("transaction", tx_hash, log_index, *facts)
    return _exact_key(row)


def _closed_position_key(row: dict[str, Any]) -> str:
    asset = _text(row, "asset", "assetId", "asset_id", "tokenId", "token_id")
    condition = _text(row, "conditionId", "condition_id")
    timestamp = _time(
        row,
        "timestamp",
        "closedAt",
        "closed_at",
        "endDate",
        "end_date",
    )
    if not (timestamp and (asset or condition)):
        return _exact_key(row)
    return _key(
        "closed_position",
        asset,
        condition,
        _text(row, "outcome"),
        timestamp,
        _number(row, "realizedPnl", "realized_pnl", "pnl"),
        _number(row, "totalBought", "total_bought", "size"),
    )


def _trade_usdc(row: dict[str, Any]) -> str:
    explicit = _finite_number(row.get("usdcSize"))
    if explicit is None:
        explicit = _finite_number(row.get("usdc_size"))
    if explicit is not None:
        return _format_number(explicit)
    size = _finite_number(row.get("size"))
    price = _finite_number(row.get("price"))
    if size is None or price is None:
        return ""
    return _format_number(size * price)


def _number(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        parsed = _finite_number(row.get(key))
        if parsed is not None:
            return _format_number(parsed)
    return ""


def _time(row: dict[str, Any], *keys: str) -> str:
    numeric = _number(row, *keys)
    return numeric or _text(row, *keys)


def _finite_number(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value).strip().replace(",", "").removeprefix("$"))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _format_number(value: float) -> str:
    return "0" if value == 0 else format(value, ".12g")


def _text(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key) or "").strip().lower()
        if value:
            return value
    return ""


def _key(kind: str, *facts: str) -> str:
    return json.dumps((kind, *facts), ensure_ascii=False, separators=(",", ":"))


def _exact_key(row: dict[str, Any]) -> str:
    return "exact:" + json.dumps(
        row,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )


def _richness(row: dict[str, Any]) -> tuple[int, int]:
    populated = sum(value not in (None, "", [], {}) for value in row.values())
    return populated, len(row)
