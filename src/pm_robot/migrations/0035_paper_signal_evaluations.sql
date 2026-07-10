CREATE TABLE IF NOT EXISTS paper_signal_evaluations (
    signal_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    candidate_stage TEXT NOT NULL DEFAULT '',
    validation_cohort TEXT NOT NULL DEFAULT '',
    market_slug TEXT NOT NULL DEFAULT '',
    asset_id TEXT NOT NULL DEFAULT '',
    outcome TEXT NOT NULL DEFAULT '',
    side TEXT NOT NULL DEFAULT '',
    detected_at INTEGER NOT NULL DEFAULT 0,
    signal_age_sec INTEGER NOT NULL DEFAULT 0,
    leader_price REAL,
    requested_stake_usd REAL NOT NULL DEFAULT 0,
    best_bid REAL,
    best_ask REAL,
    executable_price REAL,
    fillable_stake_usd REAL NOT NULL DEFAULT 0,
    quote_snapshot_at INTEGER NOT NULL DEFAULT 0,
    quote_latency_ms INTEGER NOT NULL DEFAULT 0,
    quote_source TEXT NOT NULL DEFAULT '',
    quote_error TEXT NOT NULL DEFAULT '',
    accepted INTEGER NOT NULL DEFAULT 0,
    decision_reason TEXT NOT NULL DEFAULT '',
    stake_usd REAL NOT NULL DEFAULT 0,
    route TEXT NOT NULL DEFAULT '',
    fee_usd REAL NOT NULL DEFAULT 0,
    slippage_bps REAL,
    leader_score REAL NOT NULL DEFAULT 0,
    copy_event_count REAL NOT NULL DEFAULT 0,
    hygiene_status TEXT NOT NULL DEFAULT '',
    evaluated_at INTEGER NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_paper_signal_evaluations_wallet_time
    ON paper_signal_evaluations(wallet, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_paper_signal_evaluations_accepted_time
    ON paper_signal_evaluations(accepted, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_paper_signal_evaluations_reason_time
    ON paper_signal_evaluations(decision_reason, evaluated_at DESC);
