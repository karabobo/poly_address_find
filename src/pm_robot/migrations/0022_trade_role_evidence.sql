CREATE TABLE IF NOT EXISTS wallet_trade_role_evidence (
    wallet TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    total_trades INTEGER NOT NULL,
    taker_trades INTEGER NOT NULL,
    maker_trades INTEGER NOT NULL,
    maker_fraction REAL,
    sample_complete INTEGER NOT NULL,
    sample_limit INTEGER NOT NULL,
    evidence_source TEXT NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    fetched_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trade_role_evidence_fetched
    ON wallet_trade_role_evidence(fetched_at, sample_complete);
