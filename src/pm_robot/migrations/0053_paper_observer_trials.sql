CREATE TABLE IF NOT EXISTS paper_observer_trials (
    signal_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    candidate_stage TEXT NOT NULL DEFAULT '',
    validation_cohort TEXT NOT NULL DEFAULT '',
    market_slug TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    outcome TEXT NOT NULL DEFAULT '',
    side TEXT NOT NULL DEFAULT 'BUY',
    detected_at INTEGER NOT NULL,
    entry_evaluated_at INTEGER NOT NULL,
    leader_price REAL,
    entry_price REAL NOT NULL CHECK(entry_price > 0 AND entry_price <= 1),
    stake_usd REAL NOT NULL CHECK(stake_usd > 0),
    fee_usd REAL NOT NULL DEFAULT 0 CHECK(fee_usd >= 0),
    cost_usd REAL NOT NULL CHECK(cost_usd > 0),
    shares REAL NOT NULL CHECK(shares > 0),
    slippage_bps REAL,
    signal_age_sec INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open', 'resolved')),
    mark_price REAL CHECK(mark_price IS NULL OR (mark_price >= 0 AND mark_price <= 1)),
    mark_source TEXT NOT NULL DEFAULT '',
    mark_value_usd REAL,
    pnl_usd REAL,
    roi REAL,
    resolved_at INTEGER,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_observer_trials_wallet_status
    ON paper_observer_trials(wallet, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_paper_observer_trials_status_market
    ON paper_observer_trials(status, market_slug);

CREATE INDEX IF NOT EXISTS idx_paper_observer_trials_resolved_at
    ON paper_observer_trials(resolved_at DESC)
    WHERE status = 'resolved';

-- Seed durable research trials from existing actionable observer evidence.
-- These rows are independent from paper_orders and never authorize execution.
INSERT OR IGNORE INTO paper_observer_trials(
    signal_id, wallet, candidate_stage, validation_cohort, market_slug,
    asset_id, outcome, side, detected_at, entry_evaluated_at,
    leader_price, entry_price, stake_usd, fee_usd, cost_usd, shares,
    slippage_bps, signal_age_sec, status, updated_at
)
SELECT
    signal_id,
    wallet,
    candidate_stage,
    validation_cohort,
    market_slug,
    asset_id,
    outcome,
    side,
    detected_at,
    evaluated_at,
    leader_price,
    executable_price,
    stake_usd,
    fee_usd,
    stake_usd + fee_usd,
    stake_usd / executable_price,
    slippage_bps,
    signal_age_sec,
    'open',
    evaluated_at
FROM paper_signal_evaluations
WHERE accepted = 1
  AND actionable = 1
  AND UPPER(side) = 'BUY'
  AND COALESCE(market_slug, '') != ''
  AND COALESCE(asset_id, '') != ''
  AND executable_price > 0
  AND executable_price <= 1
  AND fee_usd >= 0
  AND stake_usd > 0;
