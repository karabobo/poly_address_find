from pm_robot.execution.paper_broker import PaperBroker
from pm_robot.models import ExecutionMode, TradeSignal
from pm_robot.storage.db import connect, run_migrations


def _signal() -> TradeSignal:
    return TradeSignal(
        signal_id="s1",
        wallet="0x" + "4" * 40,
        market_slug="will-test-pass",
        asset_id="asset",
        outcome="YES",
        side="BUY",
        price=0.52,
        detected_at=1,
        best_bid=0.51,
        best_ask=0.53,
        executable_price=0.53,
        fillable_stake_usd=25,
        quote_snapshot_at=1,
        quote_source="test_book",
    )


def test_paper_broker_records_order(tmp_path):
    broker = PaperBroker(tmp_path / "ledger.jsonl", max_stake_usd=25)
    decision = broker.evaluate(_signal())
    assert decision.accepted
    assert decision.mode == ExecutionMode.PAPER
    order_id = broker.submit(_signal(), decision)
    assert order_id.startswith("paper-")
    assert (tmp_path / "ledger.jsonl").exists()


def test_paper_broker_can_persist_to_sqlite(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'paper_orders'"
            ).fetchall()
        }
        broker = PaperBroker(conn=conn, max_stake_usd=25)
        decision = broker.evaluate(_signal())
        order_id = broker.submit(_signal(), decision)
        row = conn.execute("SELECT * FROM paper_orders WHERE order_id = ?", (order_id,)).fetchone()
        assert row is not None
        assert row["accepted"] == 1
        assert "idx_paper_orders_signal_id" in indexes
    finally:
        conn.close()
