from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

SCHEMA_VERSION = 1
SOURCE = "coinbase"
VALID_SIDES = {"BUY", "SELL"}


class InvalidTradeError(ValueError):
    pass


@dataclass(frozen=True)
class NormalizedTrade:
    event_id: str
    schema_version: int
    source: str
    product_id: str
    trade_id: str
    price: str
    size: str
    maker_side: str
    trade_time: str
    ingested_at: str

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "schema_version": self.schema_version,
            "source": self.source,
            "product_id": self.product_id,
            "trade_id": self.trade_id,
            "price": self.price,
            "size": self.size,
            "maker_side": self.maker_side,
            "trade_time": self.trade_time,
            "ingested_at": self.ingested_at,
        }


def flatten_market_trades(message: dict) -> list[dict]:
    """Extract individual raw trade dicts out of a market_trades WS message."""
    trades = []
    for event in message.get("events", []):
        trades.extend(event.get("trades", []))
    return trades


def validate_trade(raw_trade: dict) -> None:
    """Raise InvalidTradeError if the raw trade fails validation."""
    if not raw_trade.get("product_id"):
        raise InvalidTradeError("missing product_id")
    if not raw_trade.get("trade_id"):
        raise InvalidTradeError("missing trade_id")

    try:
        price = Decimal(str(raw_trade.get("price")))
    except (InvalidOperation, TypeError):
        raise InvalidTradeError(f"invalid price: {raw_trade.get('price')!r}")
    if price <= 0:
        raise InvalidTradeError(f"price must be > 0, got {price}")

    try:
        size = Decimal(str(raw_trade.get("size")))
    except (InvalidOperation, TypeError):
        raise InvalidTradeError(f"invalid size: {raw_trade.get('size')!r}")
    if size <= 0:
        raise InvalidTradeError(f"size must be > 0, got {size}")

    side = raw_trade.get("side")
    if side not in VALID_SIDES:
        raise InvalidTradeError(f"side must be BUY or SELL, got {side!r}")

    trade_time = raw_trade.get("time")
    if not trade_time or not _is_valid_timestamp(trade_time):
        raise InvalidTradeError(f"invalid trade_time: {trade_time!r}")


def _is_valid_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def normalize_trade(raw_trade: dict) -> NormalizedTrade:
    """Validate a raw Coinbase trade and normalize it into our canonical event shape."""
    validate_trade(raw_trade)

    product_id = raw_trade["product_id"]
    trade_id = raw_trade["trade_id"]
    ingested_at = (
        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )

    return NormalizedTrade(
        event_id=f"{product_id}:{trade_id}",
        schema_version=SCHEMA_VERSION,
        source=SOURCE,
        product_id=product_id,
        trade_id=trade_id,
        price=raw_trade["price"],
        size=raw_trade["size"],
        maker_side=raw_trade["side"],
        trade_time=raw_trade["time"],
        ingested_at=ingested_at,
    )
