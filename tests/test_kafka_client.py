import asyncio

from producer.kafka_client import DLQ_TOPIC, RAW_TOPIC, publish_rejected, publish_trade


class FakeProducer:
    def __init__(self):
        self.calls = []

    async def send_and_wait(self, topic, value, key):
        self.calls.append({"topic": topic, "value": value, "key": key})


def test_publish_trade_sends_to_raw_topic_keyed_by_product_id():
    producer = FakeProducer()
    trade = {"product_id": "BTC-USD", "event_id": "BTC-USD:1"}

    asyncio.run(publish_trade(producer, trade))

    assert producer.calls == [{"topic": RAW_TOPIC, "value": trade, "key": "BTC-USD"}]


def test_publish_rejected_sends_to_dlq_topic_with_reason():
    producer = FakeProducer()
    raw_trade = {"product_id": "ETH-USD", "trade_id": "9"}

    asyncio.run(publish_rejected(producer, raw_trade, "invalid price"))

    assert producer.calls == [
        {
            "topic": DLQ_TOPIC,
            "value": {"raw_trade": raw_trade, "reason": "invalid price"},
            "key": "ETH-USD",
        }
    ]
