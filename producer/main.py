import asyncio
import json
import logging
import ssl

import certifi
import websockets

from producer.events import InvalidTradeError, flatten_market_trades, normalize_trade

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_IDS = ["BTC-USD", "ETH-USD", "SOL-USD"]

SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    async with websockets.connect(WS_URL, ssl=SSL_CONTEXT) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": PRODUCT_IDS,
        }))
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": "heartbeats",
        }))
        logger.info("connected to %s, subscribed to market_trades and heartbeats", WS_URL)

        async for raw_message in ws:
            handle_message(json.loads(raw_message))


def handle_message(message: dict) -> None:
    if message.get("channel") != "market_trades":
        return

    for raw_trade in flatten_market_trades(message):
        try:
            trade = normalize_trade(raw_trade)
        except InvalidTradeError as exc:
            logger.warning("rejected trade %s: %s", raw_trade, exc)
            continue
        logger.info("trade %s", trade.to_dict())


if __name__ == "__main__":
    asyncio.run(main())
