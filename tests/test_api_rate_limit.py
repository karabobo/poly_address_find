import sqlite3
from concurrent.futures import ThreadPoolExecutor
from email.utils import formatdate
from threading import Barrier
from urllib.error import HTTPError

import pytest

from pm_robot.clients.http import (
    HttpClientError,
    RateLimitedHttpClient,
    _retry_after_seconds,
)
from pm_robot.orchestration.retry_policy import upstream_aware_retry_at
from pm_robot.storage.api_rate_limit import (
    RateLimitScope,
    SharedApiRateLimiter,
    SharedRateLimitDeferred,
    api_rate_limit_summary,
    sqlite_main_database_path,
    writable_sqlite_main_database_path,
)
from pm_robot.storage.db import connect, connect_readonly, run_migrations


def _prepare_database(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        applied = run_migrations(conn)
        assert 38 in applied
    finally:
        conn.close()
    return db_path


def test_shared_rate_limiter_staggers_separate_process_reservations(tmp_path):
    db_path = _prepare_database(tmp_path)
    scope = RateLimitScope("data:/activity", capacity=2, window_seconds=1.0)

    first = SharedApiRateLimiter(db_path).reserve([scope], now=100.0)
    second = SharedApiRateLimiter(db_path).reserve([scope], now=100.0)
    third = SharedApiRateLimiter(db_path).reserve([scope], now=100.0)

    assert first.coordinated is True
    assert second.coordinated is True
    assert third.coordinated is True
    assert [first.scheduled_at, second.scheduled_at, third.scheduled_at] == [
        100.0,
        100.5,
        101.0,
    ]


def test_shared_rate_limiter_serializes_concurrent_reservations(tmp_path):
    db_path = _prepare_database(tmp_path)
    scopes = [
        RateLimitScope("gamma:*", capacity=10, window_seconds=1.0),
        RateLimitScope("gamma:/markets", capacity=2, window_seconds=1.0),
    ]
    barrier = Barrier(2)

    def reserve_slot():
        limiter = SharedApiRateLimiter(db_path)
        barrier.wait()
        return limiter.reserve(scopes, now=200.0)

    with ThreadPoolExecutor(max_workers=2) as executor:
        reservations = list(executor.map(lambda _: reserve_slot(), range(2)))

    assert all(item.coordinated for item in reservations)
    assert sorted(item.scheduled_at for item in reservations) == [200.0, 200.5]
    conn = connect(db_path)
    try:
        permit_counts = {
            row["scope"]: row["total_permits"]
            for row in conn.execute(
                "SELECT scope, total_permits FROM api_rate_limit_state ORDER BY scope"
            ).fetchall()
        }
    finally:
        conn.close()
    assert permit_counts == {"gamma:*": 2, "gamma:/markets": 2}


def test_retry_after_cooldown_is_visible_to_other_connections(tmp_path):
    db_path = _prepare_database(tmp_path)
    scopes = [
        RateLimitScope("data:*", capacity=50, window_seconds=10.0),
        RateLimitScope("data:/activity", capacity=30, window_seconds=10.0),
    ]
    writer = SharedApiRateLimiter(db_path)
    reader = SharedApiRateLimiter(db_path)

    assert writer.record_cooldown(
        scopes,
        retry_after_seconds=7.5,
        status_code=429,
        now=300.0,
    )
    assert reader.current_cooldown_wait(scopes, now=302.0) == pytest.approx(5.5)

    conn = connect(db_path)
    try:
        summary = api_rate_limit_summary(conn, now=302.0)
    finally:
        conn.close()
    assert summary["scope_count"] == 2
    assert summary["active_cooldowns"] == 2
    assert summary["total_cooldowns"] == 2


def test_long_shared_cooldown_defers_to_queue_without_consuming_permit(tmp_path):
    db_path = _prepare_database(tmp_path)
    scope = RateLimitScope("data:/positions", capacity=20, window_seconds=10.0)
    writer = SharedApiRateLimiter(db_path)
    reader = SharedApiRateLimiter(
        db_path,
        max_block_seconds=30.0,
        clock=lambda: 500.0,
    )

    assert writer.record_cooldown(
        [scope],
        retry_after_seconds=90.0,
        now=500.0,
    )
    with pytest.raises(SharedRateLimitDeferred) as captured:
        reader.wait([scope])

    assert captured.value.retry_after_seconds == 90.0
    conn = connect(db_path)
    try:
        row = conn.execute(
            "SELECT total_permits FROM api_rate_limit_state WHERE scope = ?",
            (scope.name,),
        ).fetchone()
    finally:
        conn.close()
    assert row["total_permits"] == 0


def test_shared_rate_limiter_blocks_network_when_sqlite_is_locked(tmp_path, monkeypatch):
    db_path = _prepare_database(tmp_path)
    locker = sqlite3.connect(db_path, timeout=0)
    network_calls = []

    def unexpected_request(request, timeout):
        network_calls.append(request.full_url)
        raise AssertionError("uncoordinated network request must not run")

    monkeypatch.setattr("pm_robot.clients.http.urlopen", unexpected_request)
    try:
        locker.execute("BEGIN IMMEDIATE")
        client = RateLimitedHttpClient(
            base_kind={"https://example.test": "clob"},
            shared_limiter=SharedApiRateLimiter(
                db_path,
                lock_timeout_seconds=0.01,
            ),
        )
        with pytest.raises(HttpClientError) as captured:
            client.get_json("https://example.test", "/book")
    finally:
        locker.rollback()
        locker.close()

    assert captured.value.status_code == 429
    assert captured.value.error_type == "rate_limit_coordination_unavailable"
    assert network_calls == []


def test_shared_rate_limiter_blocks_network_when_migration_is_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "unmigrated.sqlite"
    sqlite3.connect(db_path).close()
    network_calls = []

    def unexpected_request(request, timeout):
        network_calls.append(request.full_url)
        raise AssertionError("uncoordinated network request must not run")

    monkeypatch.setattr("pm_robot.clients.http.urlopen", unexpected_request)
    client = RateLimitedHttpClient(
        base_kind={"https://example.test": "data"},
        shared_limiter=SharedApiRateLimiter(db_path, lock_timeout_seconds=0.01),
    )

    with pytest.raises(HttpClientError) as captured:
        client.get_json("https://example.test", "/activity")

    assert captured.value.error_type == "rate_limit_coordination_unavailable"
    assert network_calls == []


def test_sqlite_database_path_only_enables_file_backed_coordination(tmp_path):
    memory_conn = sqlite3.connect(":memory:")
    file_conn = connect(tmp_path / "robot.sqlite")
    readonly_conn = connect_readonly(tmp_path / "robot.sqlite")
    try:
        assert sqlite_main_database_path(memory_conn) is None
        assert sqlite_main_database_path(file_conn) == tmp_path / "robot.sqlite"
        assert writable_sqlite_main_database_path(readonly_conn) is None
        assert RateLimitedHttpClient(conn=memory_conn).shared_limiter is None
        assert RateLimitedHttpClient(conn=file_conn).shared_limiter is not None
        assert RateLimitedHttpClient(conn=readonly_conn).shared_limiter is None
    finally:
        memory_conn.close()
        file_conn.close()
        readonly_conn.close()


class _SharedLimiterSpy:
    def __init__(self):
        self.waited = []
        self.cooldowns = []

    def wait(self, scopes):
        self.waited.append(list(scopes))

    def record_cooldown(self, scopes, **kwargs):
        self.cooldowns.append((list(scopes), kwargs))
        return True


class _DeferredLimiterSpy:
    def wait(self, scopes):
        raise SharedRateLimitDeferred(45.0)


def test_http_429_propagates_retry_after_to_shared_limiter(monkeypatch):
    def reject_request(request, timeout):
        raise HTTPError(
            request.full_url,
            429,
            "Too Many Requests",
            {"Retry-After": "7"},
            None,
        )

    monkeypatch.setattr("pm_robot.clients.http.urlopen", reject_request)
    shared = _SharedLimiterSpy()
    client = RateLimitedHttpClient(
        max_retries=0,
        base_kind={"https://example.test": "data"},
        shared_limiter=shared,
    )

    with pytest.raises(HttpClientError) as captured:
        client.get_json("https://example.test", "/activity")

    assert captured.value.status_code == 429
    assert captured.value.retry_after_seconds == 7.0
    assert [[scope.name for scope in call] for call in shared.waited] == [
        ["data:*", "data:/activity"]
    ]
    cooldown_scopes, cooldown_data = shared.cooldowns[0]
    assert [scope.name for scope in cooldown_scopes] == ["data:*", "data:/activity"]
    assert cooldown_data["retry_after_seconds"] == 7.0
    assert cooldown_data["status_code"] == 429


def test_http_client_converts_long_shared_wait_to_scheduler_error(monkeypatch):
    def unexpected_request(request, timeout):
        raise AssertionError("network request should be deferred")

    monkeypatch.setattr("pm_robot.clients.http.urlopen", unexpected_request)
    client = RateLimitedHttpClient(
        base_kind={"https://example.test": "data"},
        shared_limiter=_DeferredLimiterSpy(),
    )

    with pytest.raises(HttpClientError) as captured:
        client.get_json("https://example.test", "/positions")

    assert captured.value.status_code == 429
    assert captured.value.error_type == "upstream_cooldown"
    assert captured.value.retry_after_seconds == 45.0


def test_http_client_does_not_hold_worker_during_long_retry_after(monkeypatch):
    calls = []

    def reject_request(request, timeout):
        calls.append(request.full_url)
        raise HTTPError(
            request.full_url,
            429,
            "Too Many Requests",
            {"Retry-After": "60"},
            None,
        )

    monkeypatch.setattr("pm_robot.clients.http.urlopen", reject_request)
    monkeypatch.setattr(
        "pm_robot.clients.http.time.sleep",
        lambda seconds: pytest.fail(f"unexpected in-call sleep: {seconds}"),
    )
    client = RateLimitedHttpClient(
        max_retries=3,
        base_kind={"https://example.test": "data"},
        shared_limiter=_SharedLimiterSpy(),
    )

    with pytest.raises(HttpClientError) as captured:
        client.get_json("https://example.test", "/activity")

    assert calls == ["https://example.test/activity"]
    assert captured.value.retry_after_seconds == 60.0


def test_retry_after_parser_supports_http_dates():
    header_time = formatdate(1_010, usegmt=True)
    assert _retry_after_seconds({"Retry-After": header_time}, now=1_000) == 10.0
    assert _retry_after_seconds({"Retry-After": "999999"}, now=1_000) == 3_600.0
    assert _retry_after_seconds({"Retry-After": "invalid"}, now=1_000) is None


def test_queue_retry_policy_uses_upstream_cooldown_only_for_429():
    rate_limited = HttpClientError(
        "limited",
        status_code=429,
        error_type="rate_limited",
        retry_after_seconds=12.2,
    )
    server_error = HttpClientError(
        "server error",
        status_code=503,
        error_type="server_error",
        retry_after_seconds=2.0,
    )

    assert upstream_aware_retry_at(rate_limited, now=1_000, attempts=3) == 1_013
    assert upstream_aware_retry_at(server_error, now=1_000, attempts=3) == 3_700
    assert upstream_aware_retry_at(RuntimeError("local"), now=1_000, attempts=99) == 22_600
