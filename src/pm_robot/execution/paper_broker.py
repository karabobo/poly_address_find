"""JSONL paper broker for shadow/paper execution."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from pm_robot.execution.broker import Broker
from pm_robot.execution.paper_quote import PAPER_FEE_BPS
from pm_robot.models import ExecutionDecision, ExecutionMode, TradeSignal
from pm_robot.storage.repository import persist_paper_order

PAPER_MIN_PRICE = 0.02
PAPER_MAX_PRICE = 0.98


class PaperBroker(Broker):
    def __init__(
        self,
        ledger_path: Path | None = None,
        *,
        conn: sqlite3.Connection | None = None,
        max_stake_usd: float = 40.0,
    ) -> None:
        self.ledger_path = ledger_path
        self.conn = conn
        self.max_stake_usd = max_stake_usd

    def evaluate(self, signal: TradeSignal) -> ExecutionDecision:
        if signal.price < PAPER_MIN_PRICE or signal.price > PAPER_MAX_PRICE:
            return ExecutionDecision(ExecutionMode.PAPER, False, "invalid_price")
        stake = self.requested_stake(signal)
        if signal.executable_price is None or signal.best_ask is None:
            return ExecutionDecision(ExecutionMode.PAPER, False, "missing_order_book_quote")
        if signal.fillable_stake_usd + 1e-9 < stake:
            return ExecutionDecision(
                ExecutionMode.PAPER,
                False,
                "insufficient_order_book_depth",
                executable_price=signal.executable_price,
            )
        if signal.executable_price < PAPER_MIN_PRICE or signal.executable_price > PAPER_MAX_PRICE:
            return ExecutionDecision(ExecutionMode.PAPER, False, "executable_price_out_of_range")
        slippage_bps = ((signal.executable_price / signal.price) - 1.0) * 10_000.0
        return ExecutionDecision(
            mode=ExecutionMode.PAPER,
            accepted=True,
            reason="paper_clob_vwap",
            stake_usd=round(stake, 2),
            route="paper_clob_vwap",
            executable_price=signal.executable_price,
            fee_usd=round(stake * PAPER_FEE_BPS / 10_000.0, 6),
            slippage_bps=round(slippage_bps, 4),
        )

    def requested_stake(self, signal: TradeSignal) -> float:
        return min(self.max_stake_usd, max(1.0, self.max_stake_usd * signal.confidence))

    def submit(self, signal: TradeSignal, decision: ExecutionDecision) -> str:
        order_id = f"paper-{int(time.time())}-{signal.signal_id}"
        if self.conn is not None:
            persist_paper_order(self.conn, order_id, signal, decision)
        if self.ledger_path is None:
            return order_id
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "order_id": order_id,
            "recorded_at": int(time.time()),
            "signal": signal.__dict__,
            "decision": decision.__dict__,
        }
        with self.ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return order_id
