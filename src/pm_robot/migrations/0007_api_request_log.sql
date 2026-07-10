CREATE TABLE IF NOT EXISTS api_request_log (
    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    base_url TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    status_code INTEGER,
    latency_ms INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_type TEXT NOT NULL DEFAULT '',
    ok INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_api_request_log_ts
    ON api_request_log(ts DESC);

CREATE INDEX IF NOT EXISTS idx_api_request_log_base_endpoint
    ON api_request_log(base_url, endpoint, ts DESC);
