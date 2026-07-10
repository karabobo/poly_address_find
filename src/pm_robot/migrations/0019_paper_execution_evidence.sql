ALTER TABLE paper_orders ADD COLUMN leader_price REAL;
ALTER TABLE paper_orders ADD COLUMN executable_price REAL;
ALTER TABLE paper_orders ADD COLUMN best_bid REAL;
ALTER TABLE paper_orders ADD COLUMN best_ask REAL;
ALTER TABLE paper_orders ADD COLUMN fillable_stake_usd REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_orders ADD COLUMN fee_usd REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_orders ADD COLUMN slippage_bps REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_orders ADD COLUMN quote_snapshot_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE paper_orders ADD COLUMN quote_latency_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE paper_orders ADD COLUMN quote_source TEXT NOT NULL DEFAULT '';
ALTER TABLE paper_orders ADD COLUMN quote_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE paper_orders ADD COLUMN validation_cohort TEXT NOT NULL DEFAULT 'legacy_exploratory';

ALTER TABLE paper_fills ADD COLUMN leader_price REAL;
ALTER TABLE paper_fills ADD COLUMN fee_usd REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_fills ADD COLUMN slippage_bps REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_fills ADD COLUMN validation_cohort TEXT NOT NULL DEFAULT 'legacy_exploratory';

CREATE INDEX IF NOT EXISTS idx_paper_orders_cohort_created
    ON paper_orders(validation_cohort, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_paper_fills_cohort_wallet
    ON paper_fills(validation_cohort, wallet, filled_at DESC);
