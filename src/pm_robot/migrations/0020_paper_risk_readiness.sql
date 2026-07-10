ALTER TABLE paper_wallet_quality ADD COLUMN max_drawdown_pct REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_wallet_quality ADD COLUMN max_market_exposure_share REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_wallet_quality ADD COLUMN validation_days REAL NOT NULL DEFAULT 0;

ALTER TABLE paper_readiness_observations ADD COLUMN max_drawdown_pct REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_readiness_observations ADD COLUMN max_market_exposure_share REAL NOT NULL DEFAULT 0;
ALTER TABLE paper_readiness_observations ADD COLUMN validation_days REAL NOT NULL DEFAULT 0;
