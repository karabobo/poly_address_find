from pm_robot.models import CandidateAddress
from pm_robot.orchestration.wallet_history_pipeline import (
    plan_wallet_history_jobs,
    run_wallet_history_worker,
)
from pm_robot.orchestration.wallet_level_selection import reconcile_wallet_level_selections
from pm_robot.orchestration.wallet_screening import (
    plan_wallet_screen_jobs,
    run_wallet_screen_worker,
)
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import get_wallet_level
from pm_robot.wallet_levels import WalletLevel


class FakePublicClient:
    def __init__(self, wallet: str):
        self.wallet = wallet
        self.history = [
            {
                "timestamp": 1_000 + index * 300,
                "slug": f"market-{index % 6}",
                "conditionId": f"condition-{index % 6}",
                "asset": f"asset-{index % 6}",
                "type": "TRADE",
                "side": "BUY" if index % 2 == 0 else "SELL",
                "price": 0.5,
                "size": 40,
                "usdcSize": 20,
                "transactionHash": f"0x{index:064x}",
            }
            for index in range(120)
        ]

    def wallet_trades(self, wallet, *, limit, offset, taker_only):
        assert wallet == self.wallet
        return self.history[offset : offset + limit]

    def positions(self, wallet, *, size_threshold):
        return [{"cashPnl": 20, "initialValue": 200}]

    def closed_positions(self, wallet, *, limit, offset, size_threshold):
        return [{"realizedPnl": 30, "totalBought": 300}]

    def position_values(self, wallet):
        return [{"user": wallet, "value": 220}]

    def activity(self, wallet, *, limit, offset):
        return self.history[offset : offset + limit]


def test_discovery_only_funnel_reaches_l5_without_legacy_gates_or_sqlite_raw_history(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "d" * 40
    candidate = CandidateAddress(address=wallet, sources="polymarket_trades_global")
    client = FakePublicClient(wallet)
    try:
        run_migrations(conn)
        record_wallet_sighting(
            conn,
            candidate,
            recent_trades=[
                {
                    "transaction_hash": "0xl0-first",
                    "timestamp": 1_000,
                    "usdc_size": 40,
                }
            ],
            verified_trade=True,
            allow_l1=False,
            now=1_000,
        )
        assert get_wallet_level(conn, wallet).level is WalletLevel.L0
        record_wallet_sighting(
            conn,
            candidate,
            recent_trades=[
                {
                    "transaction_hash": "0xl0-second",
                    "timestamp": 1_100,
                    "usdc_size": 60,
                }
            ],
            verified_trade=True,
            now=1_100,
        )
        conn.commit()
        assert get_wallet_level(conn, wallet).level is WalletLevel.L1

        plan_wallet_screen_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        run_wallet_screen_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="screen-e2e",
            client=client,
        )
        assert get_wallet_level(conn, wallet).level is WalletLevel.L2

        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=3_000)
        conn.commit()
        run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="light-e2e",
            client=client,
        )
        reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            l3_fraction=1.0,
            now=4_000,
        )
        conn.commit()
        assert get_wallet_level(conn, wallet).level is WalletLevel.L3

        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=5_000)
        conn.commit()
        run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="deep-e2e",
            client=client,
        )
        reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            l4_fraction=1.0,
            now=6_000,
        )
        conn.commit()
        assert get_wallet_level(conn, wallet).level is WalletLevel.L4

        reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            l5_fraction=1.0,
            now=7_000,
        )
        conn.commit()
        assert get_wallet_level(conn, wallet).level is WalletLevel.L5

        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'wallet_activity'"
        ).fetchone() is None
        active = conn.execute(
            "SELECT history_depth, relative_path FROM wallet_history_artifacts "
            "WHERE wallet = ? AND status = 'active'",
            (wallet,),
        ).fetchone()
        assert active["history_depth"] == "deep"
        assert (archive_dir / active["relative_path"]).is_file()
        assert {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT job_type FROM pipeline_jobs WHERE wallet = ?", (wallet,)
            ).fetchall()
        } == {"wallet_recent_screen", "wallet_history_collect"}
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_level_events WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == 5
    finally:
        conn.close()
