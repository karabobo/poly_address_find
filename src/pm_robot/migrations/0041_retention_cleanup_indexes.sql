CREATE INDEX IF NOT EXISTS idx_copy_backtest_trades_follower
    ON copy_backtest_trades(follower_wallet);

CREATE INDEX IF NOT EXISTS idx_copy_pair_stats_follower
    ON copy_pair_stats(follower_wallet);

CREATE INDEX IF NOT EXISTS idx_paper_orders_wallet
    ON paper_orders(wallet);
