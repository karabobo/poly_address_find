"""Execution broker interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pm_robot.models import ExecutionDecision, TradeSignal


class Broker(ABC):
    @abstractmethod
    def evaluate(self, signal: TradeSignal) -> ExecutionDecision:
        """Return an execution decision without side effects."""

    @abstractmethod
    def submit(self, signal: TradeSignal, decision: ExecutionDecision) -> str:
        """Submit or record an order. Returns an order id."""
