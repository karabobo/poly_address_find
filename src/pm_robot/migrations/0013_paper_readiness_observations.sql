CREATE TABLE IF NOT EXISTS paper_readiness_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    orders INTEGER NOT NULL,
    settled_positions INTEGER NOT NULL,
    mark_coverage REAL NOT NULL,
    settled_roi REAL NOT NULL,
    total_roi REAL NOT NULL,
    production_ready INTEGER NOT NULL,
    blockers_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_readiness_observations_wallet_time
    ON paper_readiness_observations(wallet, observed_at DESC, observation_id DESC);
