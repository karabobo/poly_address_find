CREATE TABLE IF NOT EXISTS api_rate_limit_state (
    scope TEXT PRIMARY KEY,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    window_seconds REAL NOT NULL CHECK (window_seconds > 0),
    next_permit_at REAL NOT NULL DEFAULT 0,
    cooldown_until REAL NOT NULL DEFAULT 0,
    last_status_code INTEGER,
    last_retry_after_seconds REAL NOT NULL DEFAULT 0,
    total_permits INTEGER NOT NULL DEFAULT 0,
    total_cooldowns INTEGER NOT NULL DEFAULT 0,
    last_cooldown_reason TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_api_rate_limit_state_cooldown
    ON api_rate_limit_state(cooldown_until DESC, updated_at DESC);
