from dataclasses import replace

from pm_robot.clients.http import HttpClientError
from pm_robot.execution.paper_broker import PaperBroker
from pm_robot.execution.paper_portfolio import settle_paper_portfolio
from pm_robot.models import CandidateAddress
from pm_robot.models import TradeSignal
from pm_robot.orchestration.gamma_ingestor import ingest_gamma_markets
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    gamma_market_cache_summary,
    persist_wallet_activity,
    upsert_candidate,
)


class FakeGammaClient:
    def market_by_slug(self, slug):
        return {
            "slug": slug,
            "conditionId": "condition-1",
            "question": "Will this test pass?",
            "category": "test",
            "endDate": "2026-12-31T00:00:00Z",
            "closed": False,
            "active": True,
            "clobTokenIds": '["token-yes","token-no"]',
            "outcomes": '["Yes","No"]',
            "outcomePrices": '["0.5","0.5"]',
        }


class EmptyGammaClient:
    def market_by_slug(self, slug):
        return {}


class RecordingGammaClient(FakeGammaClient):
    def __init__(self):
        self.slugs = []

    def market_by_slug(self, slug):
        self.slugs.append(slug)
        return super().market_by_slug(slug)


class ClosedGammaClient(FakeGammaClient):
    def market_by_slug(self, slug):
        market = super().market_by_slug(slug)
        market["closed"] = True
        market["active"] = False
        market["outcomePrices"] = '["1","0"]'
        return market


class RateLimitedGammaClient:
    def __init__(self):
        self.calls = 0

    def market_by_slug(self, slug):
        self.calls += 1
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )


def test_gamma_market_ingestor_caches_referenced_slug(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "market-1",
                    "asset": "asset-1",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": "0xhash",
                }
            ],
            ingested_at=2_000,
        )

        summary = ingest_gamma_markets(conn, limit=10, client=FakeGammaClient())
        cache = gamma_market_cache_summary(conn)

        assert summary.markets_attempted == 1
        assert summary.rows_written == 1
        assert cache["referenced_market_slugs"] == 1
        assert cache["cached_markets"] == 1
    finally:
        conn.close()


def test_gamma_ingestor_stops_market_batch_on_shared_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    client = RateLimitedGammaClient()
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            wallet,
            [
                {
                    "timestamp": 1_000 + idx,
                    "conditionId": f"condition-{idx}",
                    "eventSlug": f"event-{idx}",
                    "slug": f"market-{idx}",
                    "asset": f"asset-{idx}",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": f"0xhash-{idx}",
                }
                for idx in range(2)
            ],
            ingested_at=2_000,
        )

        summary = ingest_gamma_markets(
            conn,
            limit=2,
            sleep_seconds=0,
            client=client,
        )

        assert summary.status == "partial"
        assert summary.markets_attempted == 1
        assert summary.markets_succeeded == 0
        assert client.calls == 1
        run = conn.execute(
            "SELECT wallets_attempted FROM ingest_runs WHERE run_id = ?",
            (summary.run_id,),
        ).fetchone()
        assert run["wallets_attempted"] == 1
    finally:
        conn.close()


def test_gamma_market_ingestor_rebuilds_closed_market_episodes(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "market-1",
                    "asset": "token-yes",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": "0xhash",
                }
            ],
            ingested_at=2_000,
        )

        summary = ingest_gamma_markets(conn, limit=10, client=ClosedGammaClient())
        episode = conn.execute("SELECT * FROM wallet_episodes WHERE address = ?", (wallet,)).fetchone()

        assert summary.markets_attempted == 1
        assert summary.rows_written == 1
        assert summary.episodes_rebuilt == 1
        assert episode["status"] == "closed"
        assert episode["realized_pnl_est"] == 5
    finally:
        conn.close()


def test_gamma_market_ingestor_rebuilds_only_open_closed_market_episodes(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    open_wallet = "0x" + "a" * 40
    already_closed_wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        for wallet in (open_wallet, already_closed_wallet):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            open_wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "market-1",
                    "asset": "token-yes",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": "0xopen",
                }
            ],
            ingested_at=2_000,
        )
        persist_wallet_activity(
            conn,
            already_closed_wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "market-1",
                    "asset": "token-yes",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": "0xclosed-buy",
                },
                {
                    "timestamp": 1_100,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "market-1",
                    "asset": "token-yes",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "SELL",
                    "price": 0.6,
                    "size": 10,
                    "usdcSize": 6,
                    "transactionHash": "0xclosed-sell",
                },
            ],
            ingested_at=2_000,
        )

        summary = ingest_gamma_markets(conn, limit=10, client=ClosedGammaClient())
        open_episode = conn.execute("SELECT * FROM wallet_episodes WHERE address = ?", (open_wallet,)).fetchone()

        assert summary.episodes_rebuilt == 1
        assert open_episode["status"] == "closed"
        assert open_episode["realized_pnl_est"] == 5
    finally:
        conn.close()


def test_gamma_market_ingestor_prioritizes_paper_fill_slugs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "activity-market",
                    "asset": "asset-1",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": "0xhash",
                }
            ],
            ingested_at=2_000,
        )
        signal = TradeSignal(
            signal_id="signal-1",
            wallet=wallet,
            market_slug="paper-market",
            asset_id="token-yes",
            outcome="YES",
            side="BUY",
            price=0.5,
            detected_at=1,
        )
        signal = replace(
            signal,
            best_bid=0.49,
            best_ask=0.51,
            executable_price=0.51,
            fillable_stake_usd=10,
            quote_snapshot_at=1,
            quote_source="test_book",
        )
        broker = PaperBroker(conn=conn, max_stake_usd=10)
        broker.submit(signal, broker.evaluate(signal))
        settle_paper_portfolio(conn, now=3_000)
        client = RecordingGammaClient()

        summary = ingest_gamma_markets(conn, limit=1, client=client)

        assert summary.markets_attempted == 1
        assert client.slugs == ["paper-market"]
    finally:
        conn.close()


def test_gamma_market_ingestor_caches_empty_response_failure(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "missing-market",
                    "asset": "asset-1",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": "0xhash",
                }
            ],
            ingested_at=2_000,
        )

        summary = ingest_gamma_markets(conn, limit=10, client=EmptyGammaClient())
        cache = gamma_market_cache_summary(conn)

        assert summary.markets_attempted == 1
        assert summary.markets_succeeded == 0
        assert summary.failures_cached == 1
        assert cache["cached_markets"] == 1
        assert cache["cached_error_markets"] == 1
    finally:
        conn.close()
