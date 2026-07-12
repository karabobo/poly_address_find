import json

from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress, CandidateStage
from pm_robot.orchestration.activity_ingestor import _fetch_wallet_activity, ingest_activity
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import activity_event_key, upsert_candidate


class FakeActivityClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def activity(self, wallet, *, limit, offset):
        self.calls.append((wallet, limit, offset))
        return self.pages.get(offset, [])


class RateLimitedActivityClient:
    def __init__(self):
        self.calls = 0

    def activity(self, wallet, *, limit, offset):
        self.calls += 1
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )


def _event(ts: int, tx: str) -> dict:
    return {
        "timestamp": ts,
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
        "transactionHash": tx,
    }


def test_fetch_wallet_activity_stops_at_watermark():
    known = _event(1_000, "0xknown")
    newest = _event(1_100, "0xnew")
    older = _event(900, "0xold")
    client = FakeActivityClient({0: [newest, known], 2: [older]})

    events = _fetch_wallet_activity(
        client,
        "0x" + "1" * 40,
        page_limit=2,
        max_events=10,
        sleep_seconds=0,
        stop_at_timestamp=1_000,
        stop_at_key=activity_event_key(known),
    )

    assert events == [newest]
    assert client.calls == [("0x" + "1" * 40, 2, 0)]


def test_ingest_activity_can_refresh_only_paper_stage_wallets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        paper_wallet = "0x" + "a" * 40
        review_wallet = "0x" + "b" * 40
        upsert_candidate(conn, CandidateAddress(address=paper_wallet, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=review_wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_APPROVED.value, paper_wallet),
        )
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.NEEDS_REVIEW.value, review_wallet),
        )
        conn.commit()
        client = FakeActivityClient({0: [_event(1_200, "0xpaper")]})

        summary = ingest_activity(
            conn,
            wallet_limit=10,
            page_limit=10,
            max_events_per_wallet=10,
            paper_stage_only=True,
            sleep_seconds=0,
            client=client,
        )

        assert summary.status == "ok"
        assert summary.wallets_attempted == 1
        assert summary.events_written == 1
        assert summary.episodes_rebuilt == 1
        assert client.calls == [(paper_wallet, 10, 0)]
        raw = conn.execute(
            "SELECT raw_json FROM wallet_activity WHERE address = ?",
            (paper_wallet,),
        ).fetchone()["raw_json"]
        assert json.loads(raw)["source"] == "paper_wallet_activity"
        run = conn.execute(
            "SELECT ingest_type, status FROM ingest_runs WHERE run_id = ?",
            (summary.run_id,),
        ).fetchone()
        assert dict(run) == {"ingest_type": "paper_activity", "status": "ok"}
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
                (review_wallet,),
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()


def test_ingest_activity_skips_episode_rebuild_when_poll_has_no_new_events(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "f" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_APPROVED.value, wallet),
        )
        conn.commit()
        event = _event(1_400, "0xfresh")

        first = ingest_activity(
            conn,
            wallet_limit=1,
            page_limit=10,
            max_events_per_wallet=10,
            paper_stage_only=True,
            sleep_seconds=0,
            client=FakeActivityClient({0: [event]}),
        )
        rebuilt_at = conn.execute(
            "SELECT rebuilt_at FROM wallet_episodes WHERE address = ?",
            (wallet,),
        ).fetchone()["rebuilt_at"]

        second = ingest_activity(
            conn,
            wallet_limit=1,
            page_limit=10,
            max_events_per_wallet=10,
            paper_stage_only=True,
            sleep_seconds=0,
            client=FakeActivityClient({0: [event]}),
        )

        assert first.events_written == 1
        assert first.episodes_rebuilt == 1
        assert second.events_written == 0
        assert second.episodes_rebuilt == 0
        assert (
            conn.execute(
                "SELECT rebuilt_at FROM wallet_episodes WHERE address = ?",
                (wallet,),
            ).fetchone()["rebuilt_at"]
            == rebuilt_at
        )
    finally:
        conn.close()


def test_ingest_activity_recovers_missing_episode_snapshot_without_new_events(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "9" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.commit()
        event = _event(1_500, "0xrecover")
        first = ingest_activity(
            conn,
            wallet_limit=1,
            page_limit=10,
            max_events_per_wallet=10,
            sleep_seconds=0,
            client=FakeActivityClient({0: [event]}),
        )
        assert first.episodes_rebuilt == 1
        conn.execute("DELETE FROM wallet_episodes WHERE address = ?", (wallet,))
        conn.commit()

        recovered = ingest_activity(
            conn,
            wallet_limit=1,
            page_limit=10,
            max_events_per_wallet=10,
            sleep_seconds=0,
            client=FakeActivityClient({0: [event]}),
        )

        assert recovered.events_written == 0
        assert recovered.episodes_rebuilt == 1
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM wallet_episodes WHERE address = ?",
                (wallet,),
            ).fetchone()[0]
            == 1
        )
    finally:
        conn.close()


def test_ingest_activity_marks_general_poll_source(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "c" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.commit()
        client = FakeActivityClient({0: [_event(1_300, "0xpoll")]})

        summary = ingest_activity(
            conn,
            wallet_limit=10,
            page_limit=10,
            max_events_per_wallet=10,
            sleep_seconds=0,
            client=client,
        )

        raw = conn.execute(
            "SELECT raw_json FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()["raw_json"]

        assert summary.status == "ok"
        assert summary.events_written == 1
        assert json.loads(raw)["source"] == "wallet_activity_poll"
    finally:
        conn.close()


def test_activity_ingestor_stops_wallet_batch_on_shared_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    client = RateLimitedActivityClient()
    try:
        run_migrations(conn)
        for suffix in ("d", "e"):
            upsert_candidate(
                conn,
                CandidateAddress(address="0x" + suffix * 40, sources="test"),
            )
        conn.commit()

        summary = ingest_activity(
            conn,
            wallet_limit=2,
            page_limit=10,
            max_events_per_wallet=10,
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
