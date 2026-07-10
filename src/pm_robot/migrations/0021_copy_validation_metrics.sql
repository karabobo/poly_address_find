ALTER TABLE copy_leader_performance ADD COLUMN edge_retention_pct REAL;
ALTER TABLE copy_leader_performance ADD COLUMN walk_forward_consistency_pct REAL;
ALTER TABLE copy_leader_performance ADD COLUMN max_drawdown_pct REAL;
