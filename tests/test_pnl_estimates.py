import pytest

from pm_robot.clients.polymarket_public import DATA_BASE, PublicPolymarketClient
from pm_robot.research.pnl_estimates import estimate_wallet_pnl


class FakeHttp:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get_json(self, base, path, params=None):
        self.calls.append((base, path, params))
        return self.payload


def test_public_client_closed_positions_uses_bounded_data_api_options():
    http = FakeHttp([{"asset": "a"}])
    client = PublicPolymarketClient(http=http)

    rows = client.closed_positions("0xabc", limit=5_000, offset=-10, size_threshold=0.01)

    assert rows == [{"asset": "a"}]
    assert http.calls == [
        (
            DATA_BASE,
            "/closed-positions",
            {
                "user": "0xabc",
                "limit": "50",
                "offset": "0",
                "sizeThreshold": "0.01",
            },
        )
    ]


def test_public_client_position_values_returns_documented_list_shape():
    http = FakeHttp([{"user": "0xabc", "value": 12.5}])
    client = PublicPolymarketClient(http=http)

    assert client.position_values("0xabc") == [{"user": "0xabc", "value": 12.5}]
    assert http.calls == [(DATA_BASE, "/value", {"user": "0xabc"})]

    mapping_http = FakeHttp({"unexpected": "shape"})
    assert PublicPolymarketClient(http=mapping_http).position_values("0xabc") == []


def test_estimate_wallet_pnl_combines_open_and_closed_without_account_roi_claim():
    estimate = estimate_wallet_pnl(
        [
            {"asset": "open-1", "cashPnl": "5.25", "initialValue": "100"},
            {"asset": "open-2", "currentValue": "28", "initialValue": "20"},
            # Documented no-double-counting assumption: realizedPnl on open rows
            # is ignored because closed_positions is the realized source.
            {"asset": "open-3", "realizedPnl": "999", "initialValue": "10"},
        ],
        [
            {"asset": "closed-1", "realizedPnl": "7.75", "costBasis": "50"},
            {"asset": "closed-2", "proceeds": "30", "buyAmount": "25"},
        ],
    )

    assert estimate.open_estimated_pnl_usdc == pytest.approx(13.25)
    assert estimate.closed_realized_pnl_usdc == pytest.approx(12.75)
    assert estimate.total_estimated_pnl_usdc == pytest.approx(26.0)
    assert estimate.capital_basis_usdc == pytest.approx(205.0)
    assert estimate.cost_roi_estimate == pytest.approx(26.0 / 205.0)
    assert estimate.open_positions_count == 3
    assert estimate.closed_positions_count == 2
    assert estimate.open_pnl_count == 2
    assert estimate.closed_pnl_count == 2
    assert estimate.open_basis_count == 3
    assert estimate.closed_basis_count == 2
    assert estimate.malformed_rows_count == 0


def test_estimate_wallet_pnl_is_defensive_about_missing_and_malformed_fields():
    estimate = estimate_wallet_pnl(
        [
            {"asset": "open-1", "cashPnl": "bad", "initialValue": ""},
            {"asset": "open-2", "avgPrice": "0.40", "size": "10", "cash_pnl": "-1.5"},
            ["not", "a", "position"],
        ],
        [
            {"asset": "closed-1", "realizedPnl": None, "costBasis": "0"},
            {"asset": "closed-2", "pnl": "$2.50", "avg_price": "0.25", "size": "20"},
            "bad row",
        ],
    )

    assert estimate.open_estimated_pnl_usdc == pytest.approx(-1.5)
    assert estimate.closed_realized_pnl_usdc == pytest.approx(2.5)
    assert estimate.total_estimated_pnl_usdc == pytest.approx(1.0)
    assert estimate.capital_basis_usdc == pytest.approx(9.0)
    assert estimate.cost_roi_estimate == pytest.approx(1.0 / 9.0)
    assert estimate.open_positions_count == 2
    assert estimate.closed_positions_count == 2
    assert estimate.open_pnl_count == 1
    assert estimate.closed_pnl_count == 1
    assert estimate.open_basis_count == 1
    assert estimate.closed_basis_count == 1
    assert estimate.malformed_rows_count == 2


def test_estimate_wallet_pnl_roi_is_none_without_capital_denominator():
    estimate = estimate_wallet_pnl(
        [{"asset": "open-1", "cashPnl": "3"}],
        [{"asset": "closed-1", "realizedPnl": "-1"}],
    )

    assert estimate.total_estimated_pnl_usdc == pytest.approx(2.0)
    assert estimate.capital_basis_usdc is None
    assert estimate.cost_roi_estimate is None
    assert estimate.open_basis_count == 0
    assert estimate.closed_basis_count == 0
