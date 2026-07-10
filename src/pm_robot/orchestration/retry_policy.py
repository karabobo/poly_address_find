"""Shared scheduling rules for retryable orchestration failures."""

from __future__ import annotations

import math

from pm_robot.clients.http import HttpClientError


def is_upstream_scheduling_error(error: BaseException) -> bool:
    """Return true when work should be deferred without consuming its failure budget."""
    return isinstance(error, HttpClientError) and error.status_code == 429


def upstream_aware_retry_at(
    error: BaseException,
    *,
    now: int,
    attempts: int,
    base_delay_seconds: int = 900,
    max_delay_seconds: int = 21_600,
    default_rate_limit_seconds: int = 60,
    max_rate_limit_seconds: int = 3_600,
) -> int:
    """Schedule HTTP 429 retries from Retry-After without changing other backoff rules."""
    if is_upstream_scheduling_error(error):
        assert isinstance(error, HttpClientError)
        retry_after = error.retry_after_seconds
        if retry_after is None:
            retry_after = float(default_rate_limit_seconds)
        delay = max(1, min(max_rate_limit_seconds, math.ceil(retry_after)))
        return int(now) + delay

    attempt_count = max(1, int(attempts))
    delay = min(max_delay_seconds, base_delay_seconds * attempt_count)
    return int(now) + delay
