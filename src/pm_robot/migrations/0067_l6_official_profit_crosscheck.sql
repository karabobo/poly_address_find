-- Preserve official leaderboard cross-checks separately from bounded position PnL.
BEGIN IMMEDIATE;

ALTER TABLE wallet_l6_validations ADD COLUMN official_all_pnl_usdc REAL;
ALTER TABLE wallet_l6_validations ADD COLUMN official_all_volume_usdc REAL;
ALTER TABLE wallet_l6_validations ADD COLUMN official_profit_intensity REAL;
ALTER TABLE wallet_l6_validations ADD COLUMN official_month_pnl_usdc REAL;
ALTER TABLE wallet_l6_validations ADD COLUMN official_week_pnl_usdc REAL;

COMMIT;
