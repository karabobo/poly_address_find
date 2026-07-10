CREATE TABLE IF NOT EXISTS gamma_market_cache (
    market_slug TEXT PRIMARY KEY,
    condition_id TEXT,
    event_slug TEXT,
    question TEXT,
    title TEXT,
    category TEXT,
    end_date TEXT,
    closed INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 0,
    archived INTEGER NOT NULL DEFAULT 0,
    clob_token_ids_json TEXT NOT NULL DEFAULT '[]',
    outcomes_json TEXT NOT NULL DEFAULT '[]',
    outcome_prices_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}',
    fetched_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gamma_market_cache_expires
    ON gamma_market_cache(expires_at ASC);

CREATE INDEX IF NOT EXISTS idx_gamma_market_cache_condition
    ON gamma_market_cache(condition_id);
