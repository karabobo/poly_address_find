from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress
from pm_robot.orchestration.positions_ingestor import ingest_positions
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import upsert_candidate


class RateLimitedPositionClient:
    def __init__(self):
        self.calls = 0

    def positions(self, wallet, *, size_threshold=0.0):
        self.calls += 1
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )


def test_positions_ingestor_stops_wallet_batch_on_shared_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    client = RateLimitedPositionClient()
    try:
        run_migrations(conn)
        for suffix in ("1", "2"):
            upsert_candidate(
                conn,
                CandidateAddress(address="0x" + suffix * 40, sources="test"),
            )
        conn.commit()

        summary = ingest_positions(
            conn,
            limit=2,
            sleep_seconds=0,
            client=client,
        )

        assert summary.status == "partial"
        assert summary.wallets_attempted == 1
        assert summary.wallets_succeeded == 0
        assert client.calls == 1
        run = conn.execute(
            "SELECT wallets_attempted FROM ingest_runs WHERE run_id = ?",
            (summary.run_id,),
        ).fetchone()
        assert run["wallets_attempted"] == 1
    finally:
        conn.close()
