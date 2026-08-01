import asyncio
import json
import ssl

import certifi
import websockets

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_IDS = ["BTC-USD", "ETH-USD", "SOL-USD"]

SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


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

        async for raw_message in ws:
            message = json.loads(raw_message)
            print(message)


if __name__ == "__main__":
    asyncio.run(main())
