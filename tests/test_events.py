from datetime import datetime, timezone

import pytest

from producer.events import (
    InvalidTradeError,
    flatten_market_trades,
    normalize_trade,
    validate_trade,
)


def _raw_trade(**overrides):
    trade = {
        "product_id": "BTC-USD",
        "trade_id": "804391204",
        "price": "64321.18",
        "size": "0.0034",
        "side": "BUY",
        "time": "2026-08-01T14:52:31.482Z",
    }
    trade.update(overrides)
    return trade


def test_flatten_market_trades_extracts_all_trades():
    message = {
        "channel": "market_trades",
        "events": [
            {"type": "snapshot", "trades": [_raw_trade(trade_id="1"), _raw_trade(trade_id="2")]},
            {"type": "update", "trades": [_raw_trade(trade_id="3")]},
        ],
    }
    trades = flatten_market_trades(message)
    assert [t["trade_id"] for t in trades] == ["1", "2", "3"]


def test_normalize_trade_builds_expected_event():
    trade = normalize_trade(_raw_trade())
    assert trade.event_id == "BTC-USD:804391204"
    assert trade.schema_version == 1
    assert trade.source == "coinbase"
    assert trade.maker_side == "BUY"


def test_normalize_trade_sets_ingested_at_close_to_now():
    trade = normalize_trade(_raw_trade())
    ingested_at = datetime.fromisoformat(trade.ingested_at.replace("Z", "+00:00"))
    assert (datetime.now(timezone.utc) - ingested_at).total_seconds() < 5


def test_validate_trade_accepts_valid_trade():
    validate_trade(_raw_trade())


@pytest.mark.parametrize(
    "overrides",
    [
        {"price": "0"},
        {"price": "-1"},
        {"price": "not-a-number"},
        {"size": "0"},
        {"size": "-0.001"},
        {"side": "HOLD"},
        {"product_id": ""},
        {"trade_id": ""},
        {"time": "not-a-timestamp"},
    ],
)
def test_validate_trade_rejects_invalid_trades(overrides):
    with pytest.raises(InvalidTradeError):
        validate_trade(_raw_trade(**overrides))
