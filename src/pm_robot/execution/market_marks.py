"""Shared Gamma market mark parsing for research and paper accounting."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GammaMarketMark:
    price: float
    source: str
    is_settlement: bool


def gamma_market_mark(row: Any | None, asset_id: str) -> GammaMarketMark | None:
    """Return the cached Gamma mark for one token without fetching or writing data."""

    if row is None or not asset_id:
        return None
    price = _gamma_asset_price(row, asset_id)
    if price is None:
        return None
    is_settlement = int(row["closed"] or 0) == 1 and (price <= 0.001 or price >= 0.999)
    return GammaMarketMark(
        price=price,
        source="gamma_settlement" if is_settlement else "gamma_outcome_price",
        is_settlement=is_settlement,
    )


def _gamma_asset_price(row: Any, asset_id: str) -> float | None:
    token_ids = [str(item) for item in _json_list(row["clob_token_ids_json"])]
    prices = [_to_probability(item) for item in _json_list(row["outcome_prices_json"])]
    if asset_id in token_ids:
        index = token_ids.index(asset_id)
        if index < len(prices) and prices[index] is not None:
            return prices[index]

    raw = _json_object(row["raw_json"])
    tokens = raw.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = str(token.get("token_id") or token.get("tokenId") or token.get("id") or "")
            if token_id != asset_id:
                continue
            for key in ("price", "last_price", "lastPrice"):
                price = _to_probability(token.get(key))
                if price is not None:
                    return price
    return None


def _json_list(value: Any) -> list[Any]:
    parsed = _json_value(value)
    return parsed if isinstance(parsed, list) else []


def _json_object(value: Any) -> dict[str, Any]:
    parsed = _json_value(value)
    return parsed if isinstance(parsed, dict) else {}


def _json_value(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, str):
        try:
            return json.loads(parsed)
        except json.JSONDecodeError:
            return parsed
    return parsed


def _to_probability(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if 0 <= number <= 1 else None
