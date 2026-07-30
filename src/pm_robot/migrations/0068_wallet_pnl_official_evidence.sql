-- Keep official lifetime profit evidence beside bounded recent position estimates.
BEGIN IMMEDIATE;

ALTER TABLE wallet_pnl_summaries ADD COLUMN official_all_pnl_usdc REAL;
ALTER TABLE wallet_pnl_summaries ADD COLUMN official_all_volume_usdc REAL;
ALTER TABLE wallet_pnl_summaries ADD COLUMN official_profit_intensity REAL;
ALTER TABLE wallet_pnl_summaries ADD COLUMN evidence_metrics_json TEXT NOT NULL DEFAULT '{}';

COMMIT;
