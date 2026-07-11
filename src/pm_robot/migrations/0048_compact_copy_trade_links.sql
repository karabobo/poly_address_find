PRAGMA foreign_keys = OFF;

BEGIN IMMEDIATE;

-- Raw copy links are rebuildable working data. Keep only links that support a
-- qualified pair; pair summaries and leader performance remain authoritative.
UPDATE copy_backtest_trades
SET link_id = NULL
WHERE link_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM copy_pair_stats AS pair
      WHERE pair.leader_wallet = copy_backtest_trades.leader_wallet
        AND pair.follower_wallet = copy_backtest_trades.follower_wallet
        AND pair.qualifies = 1
  );

DROP TABLE IF EXISTS copy_trade_links_compact;

CREATE TABLE copy_trade_links_compact (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    leader_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    follower_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    leader_activity_id INTEGER NOT NULL REFERENCES wallet_activity(activity_id) ON DELETE CASCADE,
    follower_activity_id INTEGER NOT NULL REFERENCES wallet_activity(activity_id) ON DELETE CASCADE,
    condition_id TEXT,
    market_slug TEXT,
    asset_id TEXT,
    outcome TEXT,
    side TEXT,
    leader_ts INTEGER NOT NULL,
    follower_ts INTEGER NOT NULL,
    lag_seconds INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(leader_activity_id, follower_activity_id)
);

INSERT INTO copy_trade_links_compact(
    link_id, leader_wallet, follower_wallet,
    leader_activity_id, follower_activity_id,
    condition_id, market_slug, asset_id, outcome, side,
    leader_ts, follower_ts, lag_seconds, created_at
)
SELECT
    link.link_id, link.leader_wallet, link.follower_wallet,
    link.leader_activity_id, link.follower_activity_id,
    link.condition_id, link.market_slug, link.asset_id, link.outcome, link.side,
    link.leader_ts, link.follower_ts, link.lag_seconds, link.created_at
FROM copy_pair_stats AS pair
CROSS JOIN copy_trade_links AS link INDEXED BY idx_copy_trade_links_leader
WHERE pair.qualifies = 1
  AND link.leader_wallet = pair.leader_wallet
  AND link.follower_wallet = pair.follower_wallet;

DROP TABLE copy_trade_links;
ALTER TABLE copy_trade_links_compact RENAME TO copy_trade_links;

CREATE INDEX idx_copy_trade_links_leader
    ON copy_trade_links(leader_wallet, follower_wallet, follower_ts DESC);

CREATE INDEX idx_copy_trade_links_follower
    ON copy_trade_links(follower_wallet, follower_ts DESC);

COMMIT;

PRAGMA foreign_keys = ON;
