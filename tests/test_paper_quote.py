from pm_robot.execution.paper_quote import simulate_buy_quote


def test_quote_uses_vwap_across_ask_levels():
    quote = simulate_buy_quote(
        {
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [
                {"price": "0.50", "size": "10"},
                {"price": "0.60", "size": "100"},
            ],
        },
        20,
    )

    assert quote.best_bid == 0.49
    assert quote.best_ask == 0.5
    assert round(quote.fillable_stake_usd, 6) == 20
    assert quote.executable_price is not None
    assert quote.executable_price > 0.5
    assert quote.fee_usd == 0.2


def test_quote_reports_insufficient_depth():
    quote = simulate_buy_quote(
        {"bids": [], "asks": [{"price": "0.50", "size": "2"}]},
        20,
    )

    assert quote.fillable_stake_usd == 1
