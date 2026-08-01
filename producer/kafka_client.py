import json

from aiokafka import AIOKafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "crypto.market-trades.raw.v1"
DLQ_TOPIC = "crypto.market-trades.dlq.v1"


def build_producer() -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        enable_idempotence=True,
        compression_type="snappy",
        key_serializer=lambda key: key.encode("utf-8") if key is not None else None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


async def publish_trade(producer: AIOKafkaProducer, trade_dict: dict) -> None:
    await producer.send_and_wait(RAW_TOPIC, value=trade_dict, key=trade_dict["product_id"])


async def publish_rejected(producer: AIOKafkaProducer, raw_trade: dict, reason: str) -> None:
    payload = {"raw_trade": raw_trade, "reason": reason}
    await producer.send_and_wait(DLQ_TOPIC, value=payload, key=raw_trade.get("product_id"))
