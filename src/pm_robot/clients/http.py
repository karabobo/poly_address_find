"""Rate-limited HTTP client with retry and optional SQLite request logging."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pm_robot.storage.api_rate_limit import (
    RateLimitScope,
    SharedApiRateLimiter,
    SharedRateLimitDeferred,
    SharedRateLimitUnavailable,
    writable_sqlite_main_database_path,
)
from pm_robot.storage.repository import log_api_request


USER_AGENT = "pm-robot/0.1"
MAX_RETRY_AFTER_SECONDS = 3_600.0


class HttpClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_type: str = "http_error",
        retry_after_seconds: float | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type
        self.retry_after_seconds = retry_after_seconds


@dataclass
class TokenBucket:
    capacity: int
    window_seconds: float
    _events: list[float] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock)

    def wait(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - self.window_seconds
                self._events = [t for t in self._events if t > cutoff]
                if len(self._events) < self.capacity:
                    self._events.append(now)
                    return
                sleep_for = max(0.01, self.window_seconds - (now - self._events[0]))
            time.sleep(sleep_for)


DEFAULT_LIMITS: dict[tuple[str, str], tuple[int, float]] = {
    ("data", "*"): (50, 10.0),
    ("data", "/positions"): (20, 10.0),
    ("data", "/activity"): (30, 10.0),
    ("gamma", "*"): (100, 10.0),
    ("gamma", "/markets"): (30, 10.0),
    ("gamma", "/events"): (50, 10.0),
    ("clob", "*"): (100, 10.0),
    ("lb", "*"): (50, 10.0),
}


@dataclass
class RateLimitedHttpClient:
    timeout: int = 20
    max_retries: int = 3
    conn: sqlite3.Connection | None = None
    base_kind: dict[str, str] = field(default_factory=dict)
    limits: dict[tuple[str, str], tuple[int, float]] = field(default_factory=lambda: DEFAULT_LIMITS.copy())
    shared_limiter: SharedApiRateLimiter | None = None
    _buckets: dict[tuple[str, str], TokenBucket] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.shared_limiter is not None or self.conn is None:
            return
        db_path = writable_sqlite_main_database_path(self.conn)
        if db_path is not None:
            self.shared_limiter = SharedApiRateLimiter(db_path)

    def get_json(self, base_url: str, path: str, params: dict[str, Any] | None = None) -> Any:
        self._wait_for_slot(base_url, path)
        query = f"?{urlencode(params)}" if params else ""
        url = f"{base_url}{path}{query}"
        attempt = 0
        last_error: HttpClientError | None = None
        while attempt <= self.max_retries:
            started = time.monotonic()
            status_code: int | None = None
            error_type = ""
            try:
                req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
                with urlopen(req, timeout=self.timeout) as response:
                    status_code = int(response.status)
                    raw = response.read()
                    text = raw.decode("utf-8", errors="replace")
                if _looks_like_html(text):
                    raise HttpClientError("non-json/html response", status_code=status_code, error_type="cloudflare_or_html")
                data = json.loads(text)
                self._log(base_url, path, status_code, started, attempt, "", True)
                return data
            except HTTPError as exc:
                status_code = int(exc.code)
                error_type = _http_error_type(status_code)
                retry_after_seconds = None
                if status_code == 429:
                    retry_after_seconds = _retry_after_seconds(exc.headers)
                    if retry_after_seconds is None:
                        retry_after_seconds = min(60.0, max(2.0, 2.0 ** (attempt + 1)))
                    self._record_shared_cooldown(
                        base_url,
                        path,
                        retry_after_seconds=retry_after_seconds,
                        status_code=status_code,
                    )
                last_error = HttpClientError(
                    str(exc),
                    status_code=status_code,
                    error_type=error_type,
                    retry_after_seconds=retry_after_seconds,
                )
            except URLError as exc:
                error_type = "url_error"
                last_error = HttpClientError(str(exc), error_type=error_type)
            except TimeoutError as exc:
                error_type = "timeout"
                last_error = HttpClientError(str(exc), error_type=error_type)
            except json.JSONDecodeError as exc:
                error_type = "json_decode"
                last_error = HttpClientError(str(exc), status_code=status_code, error_type=error_type)
            except HttpClientError as exc:
                status_code = exc.status_code
                error_type = exc.error_type
                last_error = exc

            self._log(base_url, path, status_code, started, attempt, error_type, False)
            if attempt >= self.max_retries or not _retryable(error_type, status_code):
                break
            if status_code == 429 and last_error is not None:
                retry_after = float(last_error.retry_after_seconds or 0)
                if retry_after > 30.0:
                    break
                time.sleep(max(0.0, retry_after))
            else:
                time.sleep(min(8.0, 2.0**attempt))
            self._wait_for_slot(base_url, path)
            attempt += 1
        raise last_error or HttpClientError("request failed")

    def _wait_for_slot(self, base_url: str, path: str) -> None:
        scopes = self._rate_limit_scopes(base_url, path)
        for scope in scopes:
            limit = (scope.capacity, scope.window_seconds)
            kind, endpoint = scope.name.split(":", 1)
            key = (kind, endpoint)
            bucket = self._buckets.setdefault(key, TokenBucket(limit[0], limit[1]))
            bucket.wait()
        if self.shared_limiter is not None:
            try:
                self.shared_limiter.wait(scopes)
            except SharedRateLimitDeferred as exc:
                raise HttpClientError(
                    "shared upstream request budget is cooling down",
                    status_code=429,
                    error_type="upstream_cooldown",
                    retry_after_seconds=exc.retry_after_seconds,
                ) from exc
            except SharedRateLimitUnavailable as exc:
                raise HttpClientError(
                    "shared upstream request budget is unavailable",
                    status_code=429,
                    error_type="rate_limit_coordination_unavailable",
                    retry_after_seconds=exc.retry_after_seconds,
                ) from exc

    def _rate_limit_scopes(self, base_url: str, path: str) -> list[RateLimitScope]:
        kind = self.base_kind.get(base_url, _infer_base_kind(base_url))
        scopes = []
        for key in ((kind, "*"), (kind, path)):
            limit = self.limits.get(key)
            if limit:
                scopes.append(RateLimitScope(f"{key[0]}:{key[1]}", limit[0], limit[1]))
        return scopes

    def _record_shared_cooldown(
        self,
        base_url: str,
        path: str,
        *,
        retry_after_seconds: float,
        status_code: int,
    ) -> None:
        if self.shared_limiter is None:
            return
        self.shared_limiter.record_cooldown(
            self._rate_limit_scopes(base_url, path),
            retry_after_seconds=retry_after_seconds,
            status_code=status_code,
            reason="http_429",
        )

    def _log(
        self,
        base_url: str,
        path: str,
        status_code: int | None,
        started: float,
        retry_count: int,
        error_type: str,
        ok: bool,
    ) -> None:
        if self.conn is None:
            return
        latency_ms = int((time.monotonic() - started) * 1000)
        try:
            log_api_request(
                self.conn,
                base_url=base_url,
                endpoint=path,
                status_code=status_code,
                latency_ms=latency_ms,
                retry_count=retry_count,
                error_type=error_type,
                ok=ok,
            )
        except sqlite3.Error:
            pass


def _infer_base_kind(base_url: str) -> str:
    if "data-api" in base_url:
        return "data"
    if "gamma-api" in base_url:
        return "gamma"
    if "clob" in base_url:
        return "clob"
    if "lb-api" in base_url:
        return "lb"
    return "default"


def _looks_like_html(text: str) -> bool:
    sample = text[:200].lstrip().lower()
    return sample.startswith("<!doctype html") or sample.startswith("<html")


def _http_error_type(status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if status_code in {403, 503}:
        return "cloudflare_or_forbidden"
    if status_code >= 500:
        return "server_error"
    return "http_error"


def _retryable(error_type: str, status_code: int | None) -> bool:
    if error_type in {"rate_limited", "cloudflare_or_forbidden", "server_error", "timeout", "url_error", "cloudflare_or_html"}:
        return True
    return bool(status_code and status_code >= 500)


def _retry_after_seconds(headers: Any, *, now: float | None = None) -> float | None:
    if headers is None:
        return None
    try:
        value = str(headers.get("Retry-After") or "").strip()
    except (AttributeError, TypeError):
        return None
    if not value:
        return None
    try:
        return min(MAX_RETRY_AFTER_SECONDS, max(0.0, float(value)))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        current = time.time() if now is None else float(now)
        return min(MAX_RETRY_AFTER_SECONDS, max(0.0, retry_at.timestamp() - current))
    except (TypeError, ValueError, OverflowError):
        return None
