"""Pure PnL estimates from Polymarket position API payloads."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class PnlEstimate:
    open_estimated_pnl_usdc: float
    closed_realized_pnl_usdc: float
    total_estimated_pnl_usdc: float
    capital_basis_usdc: float | None
    cost_roi_estimate: float | None
    open_positions_count: int
    closed_positions_count: int
    open_pnl_count: int
    closed_pnl_count: int
    open_basis_count: int
    closed_basis_count: int
    malformed_rows_count: int


def estimate_wallet_pnl(
    current_positions: Iterable[dict[str, Any]],
    closed_positions: Iterable[dict[str, Any]],
) -> PnlEstimate:
    """Estimate wallet PnL from current and closed position rows.

    This is a cost-basis estimate, not account ROI. Open rows contribute only
    unrealized/current PnL; closed rows contribute realized PnL. The function
    intentionally does not add ``realizedPnl`` from open/current rows, because
    that can overlap with the closed-positions endpoint depending on API shape.
    """

    open_pnl = 0.0
    closed_pnl = 0.0
    basis = 0.0
    open_count = 0
    closed_count = 0
    open_pnl_count = 0
    closed_pnl_count = 0
    open_basis_count = 0
    closed_basis_count = 0
    malformed_count = 0

    for row in current_positions:
        if not isinstance(row, dict):
            malformed_count += 1
            continue
        open_count += 1
        pnl_value = _open_pnl(row)
        if pnl_value is not None:
            open_pnl += pnl_value
            open_pnl_count += 1
        basis_value = _basis(row)
        if basis_value is not None:
            basis += basis_value
            open_basis_count += 1

    for row in closed_positions:
        if not isinstance(row, dict):
            malformed_count += 1
            continue
        closed_count += 1
        pnl_value = _closed_pnl(row)
        if pnl_value is not None:
            closed_pnl += pnl_value
            closed_pnl_count += 1
        basis_value = _basis(row)
        if basis_value is not None:
            basis += basis_value
            closed_basis_count += 1

    total = open_pnl + closed_pnl
    capital_basis = basis if basis > 0 else None
    roi = total / capital_basis if capital_basis is not None else None
    return PnlEstimate(
        open_estimated_pnl_usdc=open_pnl,
        closed_realized_pnl_usdc=closed_pnl,
        total_estimated_pnl_usdc=total,
        capital_basis_usdc=capital_basis,
        cost_roi_estimate=roi,
        open_positions_count=open_count,
        closed_positions_count=closed_count,
        open_pnl_count=open_pnl_count,
        closed_pnl_count=closed_pnl_count,
        open_basis_count=open_basis_count,
        closed_basis_count=closed_basis_count,
        malformed_rows_count=malformed_count,
    )


def _open_pnl(row: dict[str, Any]) -> float | None:
    direct = _first_float(
        row,
        "cashPnl",
        "cash_pnl",
        "unrealizedPnl",
        "unrealized_pnl",
        "unrealized",
    )
    if direct is not None:
        return direct
    current_value = _first_float(row, "currentValue", "current_value", "value")
    initial_value = _first_float(row, "initialValue", "initial_value", "costBasis", "cost_basis")
    if current_value is not None and initial_value is not None:
        return current_value - initial_value
    return None


def _closed_pnl(row: dict[str, Any]) -> float | None:
    direct = _first_float(
        row,
        "realizedPnl",
        "realized_pnl",
        "realizedPnlUsdc",
        "realized_pnl_usdc",
        "closedPnl",
        "closed_pnl",
        "pnl",
        "profit",
        "cashPnl",
        "cash_pnl",
    )
    if direct is not None:
        return direct
    proceeds = _first_float(
        row,
        "proceeds",
        "redeemed",
        "redeemable",
        "sold",
        "sellAmount",
        "sell_amount",
    )
    basis = _basis(row)
    if proceeds is not None and basis is not None:
        return proceeds - basis
    return None


def _basis(row: dict[str, Any]) -> float | None:
    direct = _first_float(
        row,
        "initialValue",
        "initial_value",
        "costBasis",
        "cost_basis",
        "capitalBasis",
        "capital_basis",
        "totalBought",
        "total_bought",
        "bought",
        "buyAmount",
        "buy_amount",
        "invested",
        "investment",
    )
    if direct is not None and direct > 0:
        return direct
    size = _first_float(row, "size", "shares", "quantity", "qty")
    price = _first_float(row, "avgPrice", "avg_price", "averagePrice", "average_price")
    if size is None or price is None:
        return None
    derived = abs(size) * abs(price)
    return derived if derived > 0 else None


def _first_float(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _as_float(row.get(key))
        if value is not None:
            return value
    return None


def _as_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace(",", "").removeprefix("$")
            if cleaned.endswith("%"):
                cleaned = cleaned[:-1]
            parsed = float(cleaned)
        else:
            parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None
