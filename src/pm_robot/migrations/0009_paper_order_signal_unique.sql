CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_orders_signal_id
    ON paper_orders(signal_id);
